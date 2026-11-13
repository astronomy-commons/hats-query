//! The query engine over the mounted HATS catalogs.

use crate::adql::{AdqlContext as _, register_adql};
use crate::configuration::{MountConfig, TapTableConfig};
use crate::store::{Location, Storage};
use crate::tap::KnownTables;
use bytes::Bytes;
use datafusion::common::Result as DFResult;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::object_store::path::Path;
use datafusion::prelude::SessionContext;
use tokio::sync::OnceCell;

pub struct Engine {
    storage: Storage,
    tables: KnownTables,
    /// DataFusion session with ADQL functions and TAP tables, lazily initialized.
    ctx: OnceCell<SessionContext>,
}

impl Engine {
    /// Builds an engine serving the `tables` stored in the `mounts`.
    pub fn new(mounts: Vec<MountConfig>, tables: Vec<TapTableConfig>) -> DFResult<Self> {
        let storage = Storage::from_mount_configs(mounts)?;
        let tables = KnownTables::from_tap_table_configs(&storage, tables)?;
        Ok(Self {
            storage,
            tables,
            ctx: OnceCell::new(),
        })
    }

    async fn ctx(&self) -> DFResult<&SessionContext> {
        self.ctx
            .get_or_try_init(async || {
                let ctx = SessionContext::new();
                register_adql(&ctx);
                self.storage.register_object_stores(&ctx);
                self.tables.register_tap_schema_tables(&ctx).await?;
                self.tables.register_hats_tables(&ctx).await?;
                Ok(ctx)
            })
            .await
    }

    /// Plans an ADQL query, returns the stream of its results.
    pub async fn query(&self, adql: &str) -> DFResult<SendableRecordBatchStream> {
        self.ctx().await?.adql(adql).await?.execute_stream().await
    }

    /// Reads the whole object at `path`, a server path inside the mounts.
    pub async fn read_bytes(&self, path: &Path) -> DFResult<Bytes> {
        self.storage
            .resolve(&Location::Mounted(path.clone()))?
            .read_bytes()
            .await
    }
}
