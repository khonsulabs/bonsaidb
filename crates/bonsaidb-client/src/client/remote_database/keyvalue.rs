use async_trait::async_trait;
use bonsaidb_core::keyvalue::AsyncKeyValue;
use bonsaidb_core::networking::ExecuteKeyOperation;

#[async_trait]
impl AsyncKeyValue for super::RemoteDatabase {
    async fn execute_key_operation(
        &self,
        op: bonsaidb_core::keyvalue::KeyOperation,
    ) -> Result<bonsaidb_core::keyvalue::Output, bonsaidb_core::Error> {
        Ok(self
            .client
            .send_api_request_async(&ExecuteKeyOperation {
                database: self.name.to_string(),

                op,
            })
            .await?)
    }
}
