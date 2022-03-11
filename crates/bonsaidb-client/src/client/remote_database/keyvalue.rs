use async_trait::async_trait;
use bonsaidb_core::{
    keyvalue::AsyncKeyValue,
    networking::{DatabaseRequest, DatabaseResponse, Request, Response},
};

#[async_trait]
impl AsyncKeyValue for super::RemoteDatabase {
    async fn execute_key_operation(
        &self,
        op: bonsaidb_core::keyvalue::KeyOperation,
    ) -> Result<bonsaidb_core::keyvalue::Output, bonsaidb_core::Error> {
        match self
            .client
            .send_request(Request::Database {
                database: self.name.to_string(),
                request: DatabaseRequest::ExecuteKeyOperation(op),
            })
            .await?
        {
            Response::Database(DatabaseResponse::KvOutput(output)) => Ok(output),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                bonsaidb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }
}
