use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::object_store::ObjectStore;
use datafusion::object_store::path::Path;
use object_store_opendal::OpendalStore;
use opendal::services;
use pyo3::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) struct MountTree {
    root: MountNode,
}

impl MountTree {
    pub(crate) fn from_mount_configs(mount_configs: Vec<MountConfig>) -> DFResult<Self> {
        let mut root = MountNode {
            mount: None,
            children: BTreeMap::new(),
        };

        for mount_config in mount_configs {
            // "/" has no segments, so it folds to the root itself
            let node = mount_config
                .path
                .split("/")
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty())
                .fold(&mut root, |node, segment| {
                    node.children.entry(segment).or_insert_with(|| MountNode {
                        mount: None,
                        children: BTreeMap::new(),
                    })
                });
            if node.mount.is_some() {
                return Err(DataFusionError::Configuration(format!(
                    "Duplicate mount path '{}'",
                    mount_config.path
                )));
            }
            node.mount = Some(Mount::new(&mount_config.source)?);
        }

        Ok(Self { root })
    }

    /// Finds the deepest mount containing `path`, returns its store and the path inside it.
    pub(crate) fn resolve(&self, path: &Path) -> DFResult<(Arc<dyn ObjectStore>, Path)> {
        // The deepest mount on the path, and the number of segments in its mount path
        let (mount, depth) = path
            .into_iter()
            // Walk down the tree until a segment has no node
            .scan(&self.root, |node, segment| {
                *node = node.children.get(segment.as_ref())?;
                Some(*node)
            })
            // Count the depth
            .zip(1..)
            // Skip nodes without a mount and get the last one
            .filter_map(|(node, depth)| node.mount.as_ref().map(|mount| (mount, depth)))
            .last()
            // Get "/" if no deeper mount was found
            .or_else(|| self.root.mount.as_ref().map(|mount| (mount, 0)))
            // If "/" has no mount either, return an error
            .ok_or_else(|| {
                DataFusionError::Configuration(format!("No mount found for path '{path}'"))
            })?;
        let rest = Path::from_iter(path.into_iter().skip(depth));
        Ok((mount.get_store(), rest))
    }
}

struct MountNode {
    mount: Option<Mount>,
    children: BTreeMap<String, MountNode>,
}

struct Mount {
    store: Arc<OpendalStore>,
}

impl Mount {
    /// Builds a store rooted at `source`, an absolute local path.
    fn new(source: &str) -> DFResult<Self> {
        if !std::path::Path::new(source).is_absolute() {
            return Err(DataFusionError::Configuration(format!(
                "Mount source '{source}' is not supported yet, only local directories are"
            )));
        }
        let operator = opendal::Operator::new(services::Fs::default().root(source))
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(Self {
            store: Arc::new(OpendalStore::new(operator)),
        })
    }

    fn get_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store) as Arc<dyn ObjectStore>
    }
}

#[derive(FromPyObject)]
pub(crate) struct MountConfig {
    path: String,
    source: String,
}

#[cfg(test)]
impl MountConfig {
    pub(crate) fn new(path: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            source: source.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A mount of the crate directory: opendal's fs service would create a missing one.
    fn mount_config(path: &str) -> MountConfig {
        MountConfig {
            path: path.to_string(),
            source: env!("CARGO_MANIFEST_DIR").to_string(),
        }
    }

    fn child_names(node: &MountNode) -> Vec<&str> {
        node.children.keys().map(String::as_str).collect()
    }

    /// Checks this tree, where `*` marks a node with a mount and `?` is `root_mounted`:
    ///
    /// ```text
    /// / ?
    /// ├── top1
    /// │   └── middle1 *
    /// └── top2 *
    ///     └── middle2
    ///         └── bottom1 *
    /// ```
    fn assert_nested_tree(tree: &MountTree, root_mounted: bool) {
        let root = &tree.root;
        assert_eq!(root.mount.is_some(), root_mounted);
        assert_eq!(child_names(root), ["top1", "top2"]);

        let top1 = &root.children["top1"];
        assert!(top1.mount.is_none());
        assert_eq!(child_names(top1), ["middle1"]);
        let middle1 = &top1.children["middle1"];
        assert!(middle1.mount.is_some());
        assert!(middle1.children.is_empty());

        let top2 = &root.children["top2"];
        assert!(top2.mount.is_some());
        assert_eq!(child_names(top2), ["middle2"]);
        let middle2 = &top2.children["middle2"];
        assert!(middle2.mount.is_none());
        assert_eq!(child_names(middle2), ["bottom1"]);
        let bottom1 = &middle2.children["bottom1"];
        assert!(bottom1.mount.is_some());
        assert!(bottom1.children.is_empty());
    }

    #[test]
    fn nested_mounts_with_root() {
        let tree = MountTree::from_mount_configs(vec![
            mount_config("/"),
            mount_config("/top1/middle1"),
            mount_config("/top2"),
            mount_config("/top2/middle2/bottom1"),
        ])
        .unwrap();
        assert_nested_tree(&tree, true);
    }

    /// No mount on "/", and children come before their parents, so the parents' nodes already exist.
    #[test]
    fn nested_mounts_without_root() {
        let tree = MountTree::from_mount_configs(vec![
            mount_config("/top2/middle2/bottom1"),
            mount_config("/top2"),
            mount_config("/top1/middle1"),
        ])
        .unwrap();
        assert_nested_tree(&tree, false);
    }

    /// The store mounted at the node `segments` below the root.
    fn store_at(tree: &MountTree, segments: &[&str]) -> *const () {
        let node = segments
            .iter()
            .fold(&tree.root, |node, segment| &node.children[*segment]);
        Arc::as_ptr(&node.mount.as_ref().unwrap().store) as *const ()
    }

    /// Resolves into the deepest mount, keeping every segment below it, see `assert_nested_tree`.
    #[test]
    fn resolve_nested() {
        let tree = MountTree::from_mount_configs(vec![
            mount_config("/"),
            mount_config("/top1/middle1"),
            mount_config("/top2"),
            mount_config("/top2/middle2/bottom1"),
        ])
        .unwrap();

        for (path, mount_segments, rest) in [
            ("/", &[][..], ""),
            ("/top1/other", &[][..], "top1/other"),
            ("/top1/middle1", &["top1", "middle1"][..], ""),
            ("/top2/cat/x", &["top2"][..], "cat/x"),
            ("/top2/middle2/x", &["top2"][..], "middle2/x"),
            (
                "/top2/middle2/bottom1/cat/x",
                &["top2", "middle2", "bottom1"][..],
                "cat/x",
            ),
        ] {
            let (store, resolved) = tree.resolve(&path.into()).unwrap();
            assert_eq!(resolved.as_ref(), rest, "{path}");
            assert_eq!(
                Arc::as_ptr(&store) as *const (),
                store_at(&tree, mount_segments),
                "{path}"
            );
        }
    }

    #[test]
    fn resolve_without_mount() {
        let tree = MountTree::from_mount_configs(vec![mount_config("/top")]).unwrap();
        for path in ["/", "/other", "/other/top"] {
            let result = tree.resolve(&path.into());
            assert!(
                matches!(result, Err(DataFusionError::Configuration(_))),
                "{path}"
            );
        }
    }

    /// `Path` escapes "..", so it is a literal segment and can't escape the mount.
    #[test]
    fn resolve_keeps_dot_dot_inside_mount() {
        let tree = MountTree::from_mount_configs(vec![mount_config("/top")]).unwrap();
        let (_, resolved) = tree.resolve(&"/top/../x".into()).unwrap();
        assert_eq!(resolved.as_ref(), "%2E%2E/x");
    }

    #[test]
    fn duplicate_mount_path() {
        for path in ["/", "/top"] {
            let result =
                MountTree::from_mount_configs(vec![mount_config(path), mount_config(path)]);
            assert!(matches!(result, Err(DataFusionError::Configuration(_))));
        }
    }

    #[test]
    fn remote_source_unsupported() {
        let config = MountConfig {
            path: "/hats".to_string(),
            source: "s3://bucket/prefix".to_string(),
        };
        let result = MountTree::from_mount_configs(vec![config]);
        assert!(matches!(result, Err(DataFusionError::Configuration(_))));
    }
}
