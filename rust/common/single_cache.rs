use datafusion::common::{DataFusionError, Result as DFResult};
use moka::future::Cache;

/// Lazily initialized cached value: concurrent callers share a single initialization,
/// and a failed initialization is not cached, so it is retried on the next call.
///
/// It is a wrapper around `moka::future::Cache` with a single key, which implements just a few
/// methods that are needed for our use case.
pub(crate) struct SingleCache<T>(Cache<(), T>);

impl<T> SingleCache<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub(crate) fn new() -> Self {
        Self(Cache::new(1))
    }

    pub(crate) async fn try_get_with<F, Fut>(&self, f: F) -> DFResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = DFResult<T>>,
    {
        self.0
            .try_get_with((), f())
            .await
            .map_err(DataFusionError::Shared)
    }
}
