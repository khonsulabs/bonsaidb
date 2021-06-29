use async_trait::async_trait;
use pliantdb_core::{
    custom_api::CustomApi,
    kv::Kv,
    networking::{DatabaseRequest, DatabaseResponse, Request, Response},
    schema::Schema,
};

#[async_trait]
impl<DB, A> Kv for super::RemoteDatabase<DB, A>
where
    DB: Schema,
    A: CustomApi,
{
    async fn execute_key_operation(
        &self,
        op: pliantdb_core::kv::KeyOperation,
    ) -> Result<pliantdb_core::kv::Output, pliantdb_core::Error> {
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
            other => Err(pliantdb_core::Error::Networking(
                pliantdb_core::networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }
}
