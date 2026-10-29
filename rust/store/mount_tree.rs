use crate::store::store_path::StorePath;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::object_store::path::Path;
use datafusion::prelude::SessionContext;
use opendal::services;
use pyo3::prelude::*;
use std::collections::BTreeMap;

/// URL scheme of the mount stores, see `mount_host` for the host.
const MOUNT_SCHEME: &str = "mount";

pub(crate) struct MountTree {
    root: MountNode,
}

impl MountTree {
    pub(crate) fn from_mount_configs(mount_configs: Vec<MountConfig>) -> DFResult<Self> {
        let mut root = MountNode {
            mount: None,
            children: BTreeMap::new(),
        };

        let n_mounts = mount_configs.len();
        for (index, mount_config) in mount_configs.into_iter().enumerate() {
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
            let host = mount_host(index, n_mounts, &mount_config.path);
            node.mount = Some(Mount::new(&host, &mount_config.source)?);
        }

        Ok(Self { root })
    }

    /// Registers the store of every mount in the session under the mount's URL.
    pub(crate) fn register_object_stores(&self, ctx: &SessionContext) {
        for mount in self.root.mounts() {
            let root = mount.store_path(Path::default());
            ctx.register_object_store(root.store_url().as_ref(), root.object_store());
        }
    }

    /// Finds the deepest mount containing `path`, returns its store and the path inside it.
    pub(crate) fn resolve(&self, path: &Path) -> DFResult<StorePath> {
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
        Ok(mount.store_path(rest))
    }
}

/// Host of a mount's store URL, e.g. "03.surveys.ztf_dr24" for the mount #3 at "/surveys/ztf_dr24".
///
/// The zero-padded index makes it unique, the mount path is added for readability.
fn mount_host(index: usize, n_mounts: usize, mount_path: &str) -> String {
    // Padded to the digits of the largest index, so the hosts sort in the configuration order
    let width = n_mounts.saturating_sub(1).checked_ilog10().unwrap_or(0) as usize + 1;
    let readable = mount_host_safe_path(mount_path);
    if readable.is_empty() {
        format!("{index:0width$}")
    } else {
        format!("{index:0width$}.{readable}")
    }
}

/// Lossy conversion of a path to URL host characters: "/a b/c" -> "a_b.c".
fn mount_host_safe_path(path: &str) -> String {
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            segment
                .chars()
                .map(|c| match c {
                    'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' => c,
                    _ => '_',
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join(".")
}

struct MountNode {
    mount: Option<Mount>,
    children: BTreeMap<String, MountNode>,
}

impl MountNode {
    /// Mounts of this node and all its descendants.
    fn mounts(&self) -> Box<dyn Iterator<Item = &Mount> + '_> {
        Box::new(
            self.mount
                .iter()
                .chain(self.children.values().flat_map(MountNode::mounts)),
        )
    }
}

struct Mount {
    /// URL the store is registered under in a session.
    url: ObjectStoreUrl,
    /// The store itself, e.g. a local directory or an S3 bucket.
    opendal_operator: opendal::Operator,
}

impl Mount {
    /// Builds a store rooted at `source`, an absolute local path, with the URL "mount://{host}".
    fn new(host: &str, source: &str) -> DFResult<Self> {
        if !std::path::Path::new(source).is_absolute() {
            return Err(DataFusionError::Configuration(format!(
                "Mount source '{source}' is not supported yet, only local directories are"
            )));
        }
        let opendal_operator = opendal::Operator::new(services::Fs::default().root(source))
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(Self {
            url: ObjectStoreUrl::parse(format!("{MOUNT_SCHEME}://{host}"))?,
            opendal_operator,
        })
    }

    /// The location of `path` inside the mount.
    fn store_path(&self, path: Path) -> StorePath {
        StorePath::new(self.opendal_operator.clone(), self.url.clone(), path)
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

    /// The store URL of the mount at the node `segments` below the root.
    fn store_url_at<'a>(tree: &'a MountTree, segments: &[&str]) -> &'a ObjectStoreUrl {
        let node = segments
            .iter()
            .fold(&tree.root, |node, segment| &node.children[*segment]);
        &node.mount.as_ref().unwrap().url
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

        for (path, mount_segments, rest, url) in [
            ("/", &[][..], "", "mount://0/"),
            ("/top1/other", &[][..], "top1/other", "mount://0/top1/other"),
            (
                "/top1/middle1",
                &["top1", "middle1"][..],
                "",
                "mount://1.top1.middle1/",
            ),
            (
                "/top2/cat/x",
                &["top2"][..],
                "cat/x",
                "mount://2.top2/cat/x",
            ),
            (
                "/top2/middle2/x",
                &["top2"][..],
                "middle2/x",
                "mount://2.top2/middle2/x",
            ),
            (
                "/top2/middle2/bottom1/cat/x",
                &["top2", "middle2", "bottom1"][..],
                "cat/x",
                "mount://3.top2.middle2.bottom1/cat/x",
            ),
        ] {
            let resolved = tree.resolve(&path.into()).unwrap();
            assert_eq!(resolved.path().as_ref(), rest, "{path}");
            assert_eq!(
                resolved.store_url(),
                store_url_at(&tree, mount_segments),
                "{path}"
            );
            assert_eq!(resolved.url().as_str(), url, "{path}");
        }
    }

    #[test]
    fn mount_hosts() {
        for (index, n_mounts, mount_path, host) in [
            (0, 1, "/", "0"),
            (3, 12, "/surveys/ztf_dr24", "03.surveys.ztf_dr24"),
            (4, 7, "/Hats/v2-final", "4.Hats.v2-final"),
            (5, 7, "/with space/ёжик", "5.with_space.____"),
            (6, 7, "/a:b@c", "6.a_b_c"),
            (9, 10, "/x", "9.x"),
            (10, 11, "/x", "10.x"),
            (7, 101, "/x", "007.x"),
        ] {
            assert_eq!(mount_host(index, n_mounts, mount_path), host);
        }
    }

    /// Hosts are kept as written: not lowercased and not parsed as an IP address.
    #[test]
    fn mount_urls_keep_hosts() {
        let tree =
            MountTree::from_mount_configs(vec![mount_config("/Hats"), mount_config("/2/3/4")])
                .unwrap();
        for (path, url) in [
            ("/Hats/x", "mount://0.Hats/x"),
            ("/2/3/4/x", "mount://1.2.3.4/x"),
        ] {
            assert_eq!(tree.resolve(&path.into()).unwrap().url().as_str(), url);
        }
    }

    /// Indexes are padded to the width of the largest one.
    #[test]
    fn mount_index_padding() {
        let configs = (0..11).map(|i| mount_config(&format!("/m{i}"))).collect();
        let tree = MountTree::from_mount_configs(configs).unwrap();
        for (path, store_url) in [("/m0/x", "mount://00.m0/"), ("/m10/x", "mount://10.m10/")] {
            let resolved = tree.resolve(&path.into()).unwrap();
            assert_eq!(resolved.store_url().as_str(), store_url);
        }
    }

    #[test]
    fn register_object_stores() {
        let tree = MountTree::from_mount_configs(vec![
            mount_config("/"),
            mount_config("/top1/middle1"),
            mount_config("/top2"),
        ])
        .unwrap();
        let ctx = SessionContext::new();
        tree.register_object_stores(&ctx);
        for path in ["/x", "/top1/middle1/x", "/top2/x"] {
            let resolved = tree.resolve(&path.into()).unwrap();
            let registered = ctx.runtime_env().object_store(resolved.store_url());
            assert!(registered.is_ok(), "{path}");
        }
        let unregistered = ObjectStoreUrl::parse("mount://9.other").unwrap();
        assert!(ctx.runtime_env().object_store(&unregistered).is_err());
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
        let resolved = tree.resolve(&"/top/../x".into()).unwrap();
        assert_eq!(resolved.path().as_ref(), "%2E%2E/x");
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
