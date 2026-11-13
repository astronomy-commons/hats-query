use datafusion::object_store::path::Path;

/// Where data lives: either a server path inside the mount tree or an external location.
pub(crate) enum Location {
    /// Server path, resolved through the mount tree.
    Mounted(Path),
    // External location, not resolved through the mount tree, e.g. a URL with storage options.
    // External(<something>),
}
