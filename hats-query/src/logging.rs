use std::backtrace::Backtrace;
use tracing::Level;

/// Log to stderr at `level`, panics included, with their backtraces.
pub(crate) fn init(level: Level) {
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        .init();
    std::panic::set_hook(Box::new(|info| {
        tracing::error!("{info}\n{}", Backtrace::force_capture());
    }));
}
