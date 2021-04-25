use async_trait::async_trait;
use pliantdb_local::core::{self, kv::Kv, schema::Schema};

use crate::error::ResultExt;

#[async_trait]
impl<'a, 'b, DB> Kv for super::Database<'a, 'b, DB>
where
    DB: Schema,
{
    async fn execute_key_operation(
        &self,
        op: core::kv::KeyOperation,
    ) -> Result<core::kv::Output, core::Error> {
        let db = self
            .server
            .open_database::<DB>(self.name)
            .await
            .map_err_to_core()?;
        db.execute_key_operation(op).await
    }
}
