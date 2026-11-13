//! Server configuration, see hats-query.example.toml.

use crate::tap::schema_of;
use config::{Config, ConfigError, File, FileFormat};
use datafusion::object_store;
use serde::{Deserialize, Deserializer, de};
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Component, PathBuf};
use url::Url;

/// The whole configuration.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub log: LogConfig,
    /// The `[[mount]]` entries.
    #[serde(default, rename = "mount")]
    pub mounts: Vec<MountConfig>,
    pub tap: TapConfig,
}

impl Configuration {
    /// Read a TOML config file.
    ///
    /// A relative local mount source is resolved against the file's directory.
    pub fn load(path: impl AsRef<std::path::Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let config = Config::builder()
            .add_source(File::from(path).format(FileFormat::Toml))
            .build()?;
        Self::from_config(config, path.parent().unwrap_or(std::path::Path::new("")))
    }

    /// Deserialize `config` and validate the result, resolving relative local mount sources
    /// against `base_dir`.
    fn from_config(config: Config, base_dir: &std::path::Path) -> Result<Self, ConfigError> {
        let mut config: Self = config.try_deserialize()?;
        for mount in &mut config.mounts {
            mount
                .source
                .resolve(base_dir)
                .map_err(|err| ConfigError::Message(format!("mount /{}: {err}", mount.path)))?;
        }
        config.tap.validate().map_err(ConfigError::Message)?;
        Ok(config)
    }
}

/// The `[server]` section.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// IPv4 or IPv6 address to listen on.
    pub address: IpAddr,
    pub port: u16,
    /// Reload on code changes and return tracebacks in error responses.
    pub developer_mode: bool,
    /// Who runs this deployment.
    pub contact: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 80,
            developer_mode: false,
            contact: None,
        }
    }
}

/// The `[log]` section.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct LogConfig {
    pub level: LogLevel,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Critical,
    Error,
    Warning,
    #[default]
    Info,
    Debug,
    Trace,
}

/// A `[[mount]]`: the directory `source` served under the url prefix `path`.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MountConfig {
    #[serde(deserialize_with = "server_path")]
    pub path: object_store::path::Path,
    pub source: MountSource,
}

impl MountConfig {
    /// The directory `source` served under the server path `path`, e.g. "/hats".
    ///
    /// A relative local `source` is resolved against `base_dir`, see `MountSource`.
    pub fn new(path: &str, source: String, base_dir: &std::path::Path) -> Result<Self, String> {
        let mut source = MountSource::try_from(source)?;
        source.resolve(base_dir)?;
        Ok(Self {
            path: parse_server_path(path)?,
            source,
        })
    }
}

/// Where a mount's data lives.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(try_from = "String")]
pub enum MountSource {
    /// A local directory, given as a plain path.
    ///
    /// `Configuration::load` turns it into an absolute path to an existing directory.
    Local(PathBuf),
    /// A remote directory, given as a `scheme://` URL.
    Remote(Url),
}

impl MountSource {
    /// Turn a local source into an absolute path to an existing directory, resolving a relative
    /// one against `base_dir`, keep a remote one as is.
    fn resolve(&mut self, base_dir: &std::path::Path) -> Result<(), String> {
        if let Self::Local(path) = self {
            let resolved = base_dir
                .join(&*path)
                .canonicalize()
                .map_err(|err| format!("{}: {err}", path.display()))?;
            if !resolved.is_dir() {
                return Err(format!("{} is not a directory", resolved.display()));
            }
            *path = resolved;
        }
        Ok(())
    }
}

impl TryFrom<String> for MountSource {
    type Error = String;

    fn try_from(source: String) -> Result<Self, Self::Error> {
        // A Windows drive letter parses as a one-letter URL scheme
        let is_drive = matches!(
            std::path::Path::new(&source).components().next(),
            Some(Component::Prefix(_))
        );
        match Url::parse(&source) {
            Ok(url) if url.scheme() == "file" => Err(format!(
                "{source}: give a local source as a plain path, not a file: URL"
            )),
            Ok(url) if !is_drive => Ok(Self::Remote(url)),
            _ => Ok(Self::Local(PathBuf::from(source))),
        }
    }
}

/// The `[tap]` section.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TapConfig {
    /// The `[[tap.table]]` entries, at least one.
    #[serde(rename = "table")]
    pub tables: Vec<TapTableConfig>,
    /// The `[[tap.example]]` entries.
    #[serde(default, rename = "example")]
    pub examples: Vec<TapExampleConfig>,
}

impl TapConfig {
    /// Require at least one table, "schema.table" table names, and examples to name known
    /// tables only.
    fn validate(&self) -> Result<(), String> {
        if self.tables.is_empty() {
            return Err("tap: at least one [[tap.table]] is required".to_owned());
        }
        for table in &self.tables {
            table.validate()?;
        }
        let known: HashSet<&str> = self.tables.iter().map(|t| t.name.as_str()).collect();
        for example in &self.examples {
            let unknown: Vec<&str> = example
                .tables
                .iter()
                .map(String::as_str)
                .filter(|table| !known.contains(table))
                .collect();
            if !unknown.is_empty() {
                return Err(format!(
                    "example {:?} names unknown tables: {}",
                    example.name,
                    unknown.join(", ")
                ));
            }
        }
        Ok(())
    }
}

/// A `[[tap.table]]`: the HATS catalog at `path` published as the table `name`.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TapTableConfig {
    pub name: String,
    #[serde(deserialize_with = "server_path")]
    pub path: object_store::path::Path,
}

impl TapTableConfig {
    /// The HATS catalog at the server path `path`, e.g. "/hats/x", published as the table `name`,
    /// e.g. "schema.table".
    pub fn new(name: String, path: &str) -> Result<Self, String> {
        let table = Self {
            name,
            path: parse_server_path(path)?,
        };
        table.validate()?;
        Ok(table)
    }

    /// Require a "schema.table" name.
    fn validate(&self) -> Result<(), String> {
        schema_of(&self.name).map_err(|err| err.to_string())?;
        Ok(())
    }
}

/// A `[[tap.example]]`: a query offered at /tap/examples.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TapExampleConfig {
    pub name: String,
    pub query: String,
    /// Tables the query is about, out of the `[[tap.table]]` names.
    #[serde(default)]
    pub tables: Vec<String>,
}

/// Deserialize a server path, see `parse_server_path`.
fn server_path<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<object_store::path::Path, D::Error> {
    let value = String::deserialize(deserializer)?;
    parse_server_path(&value).map_err(de::Error::custom)
}

/// Parse a server path, resolved through the mount tree, e.g. "/hats/x".
///
/// It must start with '/', which the returned path drops, and must not have empty, `.` or `..`
/// segments.
fn parse_server_path(value: &str) -> Result<object_store::path::Path, String> {
    // Technically, it doesn't matter for object_store::path::Path to start with "/" or not,
    // but we want it to look like a server path.
    if !value.starts_with('/') {
        return Err(format!("{value} must start with '/'"));
    }
    object_store::path::Path::parse(value).map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::util::WORKSPACE_ROOT;

    fn from_toml(text: &str) -> Result<Configuration, ConfigError> {
        let config = Config::builder()
            .add_source(File::from_str(text, FileFormat::Toml))
            .build()?;
        Configuration::from_config(config, std::path::Path::new(WORKSPACE_ROOT))
    }

    const MINIMAL_TAP: &str = r#"
        [[tap.table]]
        name = "s.t"
        path = "/hats/t"
    "#;

    /// A config with a single table `name` at `path`.
    fn table(name: &str, path: &str) -> Result<Configuration, ConfigError> {
        from_toml(&format!(
            r#"
            [[tap.table]]
            name = "{name}"
            path = "{path}"
            "#
        ))
    }

    /// A config with a single table at `path`.
    fn table_at(path: &str) -> Result<Configuration, ConfigError> {
        table("s.t", path)
    }

    #[test]
    fn example_config() {
        let config = Configuration::load(
            std::path::Path::new(WORKSPACE_ROOT).join("hats-query.example.toml"),
        )
        .unwrap();
        let expected_source = std::path::Path::new(WORKSPACE_ROOT)
            .join("test-data/git-submodules/hats/tests/data")
            .canonicalize()
            .unwrap();
        assert_eq!(config.mounts.len(), 1);
        assert_eq!(config.mounts[0].path.as_ref(), "hats");
        assert_eq!(config.mounts[0].source, MountSource::Local(expected_source));
        assert_eq!(config.log.level, LogLevel::Info);
        assert_eq!(config.tap.tables.len(), 2);
        assert_eq!(config.tap.examples.len(), 2);
    }

    #[test]
    fn defaults() {
        let config = from_toml(MINIMAL_TAP).unwrap();
        assert_eq!(config.server, ServerConfig::default());
        assert_eq!(config.log, LogConfig::default());
        assert!(config.mounts.is_empty());
        assert!(config.tap.examples.is_empty());
    }

    #[test]
    fn unknown_key_rejected() {
        let text = format!(
            r#"
            [server]
            bogus = 1
            {MINIMAL_TAP}
            "#
        );
        let err = from_toml(&text).unwrap_err().to_string();
        assert!(err.contains("bogus"), "{err}");
    }

    #[test]
    fn invalid_address_rejected() {
        let text = format!(
            r#"
            [server]
            address = "localhost"
            {MINIMAL_TAP}
            "#
        );
        let err = from_toml(&text).unwrap_err().to_string();
        assert!(err.contains("address"), "{err}");
    }

    #[test]
    fn unknown_log_level_rejected() {
        let text = format!(
            r#"
            [log]
            level = "loud"
            {MINIMAL_TAP}
            "#
        );
        let err = from_toml(&text).unwrap_err().to_string();
        assert!(err.contains("loud"), "{err}");
    }

    #[test]
    fn missing_tap_rejected() {
        let text = r#"
            [server]
            port = 8000
        "#;
        let err = from_toml(text).unwrap_err().to_string();
        assert!(err.contains("tap"), "{err}");
    }

    #[test]
    fn empty_tables_rejected() {
        let text = r#"
            [tap]
            table = []
        "#;
        let err = from_toml(text).unwrap_err().to_string();
        assert!(err.contains("at least one"), "{err}");
    }

    #[test]
    fn unqualified_table_name_rejected() {
        let err = table("t", "/hats/t").unwrap_err().to_string();
        assert!(err.contains("must be qualified with a schema"), "{err}");
    }

    #[test]
    fn example_with_unknown_table_rejected() {
        let text = format!(
            r#"
            {MINIMAL_TAP}
            [[tap.example]]
            name = "e"
            query = "SELECT 1"
            tables = ["s.t", "s.u"]
            "#
        );
        let err = from_toml(&text).unwrap_err().to_string();
        assert!(err.contains("unknown tables: s.u"), "{err}");
    }

    #[test]
    fn server_path_parsed() {
        for (path, expected) in [
            ("/", ""),
            ("/hats", "hats"),
            ("/hats/", "hats"),
            ("/hats/x", "hats/x"),
        ] {
            let config = table_at(path).unwrap();
            assert_eq!(config.tap.tables[0].path.as_ref(), expected, "{path}");
        }
    }

    #[test]
    fn server_path_rejected() {
        for path in ["hats", "/hats/../x", "/hats/./x", "//hats", "/hats//x"] {
            assert!(table_at(path).is_err(), "{path}");
        }
    }

    #[test]
    fn mount_local_source_resolved() {
        let mut source = MountSource::try_from("test-data".to_owned()).unwrap();
        source
            .resolve(std::path::Path::new(WORKSPACE_ROOT))
            .unwrap();
        let expected = std::path::Path::new(WORKSPACE_ROOT)
            .join("test-data")
            .canonicalize()
            .unwrap();
        assert_eq!(source, MountSource::Local(expected));
    }

    #[test]
    fn mount_local_source_missing() {
        let mut source = MountSource::try_from("missing".to_owned()).unwrap();
        assert!(
            source
                .resolve(std::path::Path::new(WORKSPACE_ROOT))
                .is_err()
        );
    }

    #[test]
    fn mount_local_source_not_a_directory() {
        let mut source = MountSource::try_from("Cargo.toml".to_owned()).unwrap();
        assert!(
            source
                .resolve(std::path::Path::new(WORKSPACE_ROOT))
                .is_err()
        );
    }

    #[test]
    fn mount_file_url_rejected() {
        assert!(MountSource::try_from("file:///tmp".to_owned()).is_err());
    }

    #[test]
    fn mount_remote_source_kept() {
        let mut source = MountSource::try_from("s3://bucket/prefix".to_owned()).unwrap();
        source
            .resolve(std::path::Path::new(WORKSPACE_ROOT))
            .unwrap();
        assert_eq!(
            source,
            MountSource::Remote(Url::parse("s3://bucket/prefix").unwrap())
        );
    }
}
