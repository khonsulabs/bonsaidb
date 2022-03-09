use async_trait::async_trait;
#[cfg(feature = "password-hashing")]
use bonsaidb_core::connection::Authentication;
use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    connection::{AccessPolicy, AsyncConnection, AsyncStorageConnection, QueryKey, Range, Sort},
    custom_api::{CustomApi, CustomApiResult},
    document::DocumentId,
    keyvalue::{AsyncKeyValue, KeyOperation},
    networking::{
        CreateDatabaseHandler, DatabaseRequest, DatabaseRequestDispatcher, DatabaseResponse,
        DeleteDatabaseHandler, Request, RequestDispatcher, Response, ServerRequest,
        ServerRequestDispatcher, ServerResponse,
    },
    permissions::{Dispatcher, Permissions},
    pubsub::AsyncPubSub,
    schema::{CollectionName, NamedReference, ViewName},
    transaction::Transaction,
};

use crate::{
    backend::{self, BackendError, CustomApiDispatcher},
    AsyncDatabase, AsyncStorage, Error,
};

#[derive(Dispatcher, Debug)]
#[dispatcher(input = Request<<Backend::CustomApi as CustomApi>::Request>, input = ServerRequest, actionable = bonsaidb_core::actionable)]
struct ServerDispatcher<'s, Backend>
where
    Backend: backend::Backend,
{
    storage: &'s AsyncStorage<Backend>,
}

#[async_trait]
impl<'s, Backend: backend::Backend> RequestDispatcher for ServerDispatcher<'s, Backend> {
    type Subaction = <Backend::CustomApi as CustomApi>::Request;
    type Output = Response<CustomApiResult<Backend::CustomApi>>;
    type Error = Error;

    async fn handle_subaction(
        &self,
        permissions: &Permissions,
        subaction: Self::Subaction,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        let dispatcher =
            <Backend::CustomApiDispatcher as CustomApiDispatcher<Backend>>::new(self.storage);
        match dispatcher.dispatch(permissions, subaction).await {
            Ok(response) => Ok(Response::Api(Ok(response))),
            Err(err) => match err {
                BackendError::Backend(backend) => Ok(Response::Api(Err(backend))),
                BackendError::Storage(error) => Err(error),
            },
        }
    }
}

#[async_trait]
impl<'s, Backend: backend::Backend> bonsaidb_core::networking::ServerHandler
    for ServerDispatcher<'s, Backend>
{
    async fn handle(
        &self,
        permissions: &Permissions,
        request: ServerRequest,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        ServerRequestDispatcher::dispatch_to_handlers(self, permissions, request).await
    }
}

#[async_trait]
impl<'s, Backend: backend::Backend> bonsaidb_core::networking::DatabaseHandler
    for ServerDispatcher<'s, Backend>
{
    async fn handle(
        &self,
        permissions: &Permissions,
        database_name: String,
        request: DatabaseRequest,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        let database = self.storage.database_without_schema(&database_name).await?;
        DatabaseDispatcher { database }
            .dispatch(permissions, request)
            .await
    }
}

impl<'s, Backend: backend::Backend> ServerRequestDispatcher for ServerDispatcher<'s, Backend> {
    type Output = Response<CustomApiResult<Backend::CustomApi>>;
    type Error = Error;
}

#[async_trait]
impl<'s, Backend: backend::Backend> CreateDatabaseHandler for ServerDispatcher<'s, Backend> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        database: bonsaidb_core::connection::Database,
        only_if_needed: bool,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        self.storage
            .create_database_with_schema(&database.name, database.schema, only_if_needed)
            .await?;
        Ok(Response::Server(ServerResponse::DatabaseCreated {
            name: database.name.clone(),
        }))
    }
}

#[async_trait]
impl<'s, Backend: backend::Backend> DeleteDatabaseHandler for ServerDispatcher<'s, Backend> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        name: String,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        self.storage.delete_database(&name).await?;
        Ok(Response::Server(ServerResponse::DatabaseDeleted { name }))
    }
}

#[async_trait]
impl<'s, Backend: backend::Backend> bonsaidb_core::networking::ListDatabasesHandler
    for ServerDispatcher<'s, Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        Ok(Response::Server(ServerResponse::Databases(
            self.storage.list_databases().await?,
        )))
    }
}

#[async_trait]
impl<'s, Backend: backend::Backend> bonsaidb_core::networking::ListAvailableSchemasHandler
    for ServerDispatcher<'s, Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        Ok(Response::Server(ServerResponse::AvailableSchemas(
            self.storage.list_available_schemas().await?,
        )))
    }
}

#[async_trait]
impl<'s, Backend: backend::Backend> bonsaidb_core::networking::CreateUserHandler
    for ServerDispatcher<'s, Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        username: String,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        Ok(Response::Server(ServerResponse::UserCreated {
            id: self.storage.create_user(&username).await?,
        }))
    }
}

#[async_trait]
impl<'s, Backend: backend::Backend> bonsaidb_core::networking::DeleteUserHandler
    for ServerDispatcher<'s, Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        user: NamedReference<'static, u64>,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        self.storage.delete_user(user).await?;
        Ok(Response::Ok)
    }
}

#[cfg(feature = "password-hashing")]
#[async_trait]
impl<'s, Backend: backend::Backend> bonsaidb_core::networking::SetUserPasswordHandler
    for ServerDispatcher<'s, Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        username: NamedReference<'static, u64>,
        password: bonsaidb_core::connection::SensitiveString,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        self.storage.set_user_password(username, password).await?;
        Ok(Response::Ok)
    }
}

#[async_trait]
#[cfg(feature = "password-hashing")]
impl<'s, Backend: backend::Backend> bonsaidb_core::networking::AuthenticateHandler
    for ServerDispatcher<'s, Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        username: NamedReference<'static, u64>,
        authentication: Authentication,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        let authenticated = self
            .storage
            .authenticate(username.clone(), authentication)
            .await?;

        Ok(Response::Server(ServerResponse::Authenticated(
            authenticated.session().cloned().unwrap(),
        )))
    }
}

#[async_trait]
impl<'s, Backend: backend::Backend>
    bonsaidb_core::networking::AlterUserPermissionGroupMembershipHandler
    for ServerDispatcher<'s, Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        user: NamedReference<'static, u64>,
        group: NamedReference<'static, u64>,
        should_be_member: bool,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        if should_be_member {
            self.storage
                .add_permission_group_to_user(user, group)
                .await?;
        } else {
            self.storage
                .remove_permission_group_from_user(user, group)
                .await?;
        }

        Ok(Response::Ok)
    }
}

#[async_trait]
impl<'s, Backend: backend::Backend> bonsaidb_core::networking::AlterUserRoleMembershipHandler
    for ServerDispatcher<'s, Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        user: NamedReference<'static, u64>,
        role: NamedReference<'static, u64>,
        should_be_member: bool,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        if should_be_member {
            self.storage.add_role_to_user(user, role).await?;
        } else {
            self.storage.remove_role_from_user(user, role).await?;
        }

        Ok(Response::Ok)
    }
}

#[derive(Dispatcher, Debug)]
#[dispatcher(input = DatabaseRequest, actionable = bonsaidb_core::actionable)]
struct DatabaseDispatcher<Backend>
where
    Backend: backend::Backend,
{
    database: AsyncDatabase<Backend>,
}

impl<Backend: backend::Backend> DatabaseRequestDispatcher for DatabaseDispatcher<Backend> {
    type Output = Response<CustomApiResult<Backend::CustomApi>>;
    type Error = Error;
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::GetHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        id: DocumentId,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        let task_db = self.database.clone();
        let document = tokio::task::spawn_blocking(move || {
            task_db
                .database
                .internal_get_from_collection_id(id, &collection)?
                .ok_or_else(|| {
                    Error::Core(bonsaidb_core::Error::DocumentNotFound(
                        collection,
                        Box::new(id),
                    ))
                })
        })
        .await??;
        Ok(Response::Database(DatabaseResponse::Documents(vec![
            document,
        ])))
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::GetMultipleHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        ids: Vec<DocumentId>,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        let task_db = self.database.clone();
        let documents = tokio::task::spawn_blocking(move || {
            task_db
                .database
                .internal_get_multiple_from_collection_id(&ids, &collection)
        })
        .await??;
        Ok(Response::Database(DatabaseResponse::Documents(documents)))
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::ListHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<usize>,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        let task_db = self.database.clone();
        let documents = tokio::task::spawn_blocking(move || {
            task_db
                .database
                .list_from_collection(ids, order, limit, &collection)
        })
        .await??;
        Ok(Response::Database(DatabaseResponse::Documents(documents)))
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::CountHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        ids: Range<DocumentId>,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        let task_db = self.database.clone();
        let documents = tokio::task::spawn_blocking(move || {
            task_db.database.count_from_collection(ids, &collection)
        })
        .await??;
        Ok(Response::Database(DatabaseResponse::Count(documents)))
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::QueryHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        view: ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
        with_docs: bool,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        let task_db = self.database.clone();
        tokio::task::spawn_blocking(move || {
            if with_docs {
                let mappings = task_db.database.query_by_name_with_docs(
                    &view,
                    key,
                    order,
                    limit,
                    access_policy,
                )?;
                Ok(Response::Database(DatabaseResponse::ViewMappingsWithDocs(
                    mappings,
                )))
            } else {
                let mappings =
                    task_db
                        .database
                        .query_by_name(&view, key, order, limit, access_policy)?;
                Ok(Response::Database(DatabaseResponse::ViewMappings(mappings)))
            }
        })
        .await?
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::ReduceHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        view: ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
        grouped: bool,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        let task_db = self.database.clone();
        tokio::task::spawn_blocking(move || {
            if grouped {
                let values = task_db
                    .database
                    .reduce_grouped_by_name(&view, key, access_policy)?;
                Ok(Response::Database(DatabaseResponse::ViewGroupedReduction(
                    values,
                )))
            } else {
                let value = task_db.database.reduce_by_name(&view, key, access_policy)?;
                Ok(Response::Database(DatabaseResponse::ViewReduction(
                    Bytes::from(value),
                )))
            }
        })
        .await?
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::ApplyTransactionHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        transaction: Transaction,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        let results = self.database.apply_transaction(transaction).await?;
        Ok(Response::Database(DatabaseResponse::TransactionResults(
            results,
        )))
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::DeleteDocsHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        view: ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        let task_db = self.database.clone();
        tokio::task::spawn_blocking(move || {
            let count = task_db
                .database
                .delete_docs_by_name(&view, key, access_policy)?;
            Ok(Response::Database(DatabaseResponse::Count(count)))
        })
        .await?
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::ListExecutedTransactionsHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        Ok(Response::Database(DatabaseResponse::ExecutedTransactions(
            self.database
                .list_executed_transactions(starting_id, result_limit)
                .await?,
        )))
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::LastTransactionIdHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        Ok(Response::Database(DatabaseResponse::LastTransactionId(
            self.database.last_transaction_id().await?,
        )))
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::CreateSubscriberHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        let subscriber = self.database.create_subscriber().await?;
        let subscriber_id = subscriber.id;

        Ok(Response::Database(DatabaseResponse::SubscriberCreated {
            subscriber_id,
        }))
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::PublishHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        topic: String,
        payload: Bytes,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        self.database
            .publish_bytes(&topic, payload.into_vec())
            .await?;
        Ok(Response::Ok)
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::PublishToAllHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        topics: Vec<String>,
        payload: Bytes,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        self.database
            .publish_bytes_to_all(topics, payload.into_vec())
            .await?;
        Ok(Response::Ok)
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::SubscribeToHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        subscriber_id: u64,
        topic: String,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        self.database
            .database
            .subscribe_by_id(subscriber_id, &topic)
            .map(|_| Response::Ok)
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::UnsubscribeFromHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        subscriber_id: u64,
        topic: String,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        self.database
            .database
            .unsubscribe_by_id(subscriber_id, &topic)
            .map(|_| Response::Ok)
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::UnregisterSubscriberHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        subscriber_id: u64,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        self.database
            .database
            .unregister_subscriber_by_id(subscriber_id)
            .map(|_| Response::Ok)
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::ExecuteKeyOperationHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        op: KeyOperation,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        let result = self.database.execute_key_operation(op).await?;
        Ok(Response::Database(DatabaseResponse::KvOutput(result)))
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::CompactCollectionHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        self.database.compact_collection_by_name(collection).await?;

        Ok(Response::Ok)
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::CompactKeyValueStoreHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        self.database.compact_key_value_store().await?;

        Ok(Response::Ok)
    }
}

#[async_trait]
impl<Backend: backend::Backend> bonsaidb_core::networking::CompactHandler
    for DatabaseDispatcher<Backend>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<CustomApiResult<Backend::CustomApi>>, Error> {
        self.database.compact().await?;

        Ok(Response::Ok)
    }
}
