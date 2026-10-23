use datafusion::common::{DataFusionError, Result as DFResult};
use moka::future::Cache;

pub(crate) struct SingleCache<T>(Cache<(), T>);

impl<T> SingleCache<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub(crate) fn new() -> Self {
        Self(Cache::new(1))
    }

    pub(crate) async fn set(&self, value: T) {
        self.0.insert((), value).await;
    }

    pub(crate) async fn get_with<F, Fut>(&self, f: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        self.0.get_with((), f()).await
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
