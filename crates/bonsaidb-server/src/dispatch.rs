#[cfg(feature = "password-hashing")]
use bonsaidb_core::connection::Authentication;
use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    async_trait::async_trait,
    connection::{
        AccessPolicy, AsyncConnection, AsyncLowLevelConnection, AsyncStorageConnection, Identity,
        QueryKey, Range, Sort,
    },
    document::DocumentId,
    keyvalue::{AsyncKeyValue, KeyOperation},
    networking::{
        CreateDatabaseHandler, DatabaseRequest, DatabaseRequestDispatcher, DatabaseResponse,
        DeleteDatabaseHandler, Request, RequestDispatcher, Response, ServerRequest,
        ServerRequestDispatcher, ServerResponse,
    },
    permissions::{Dispatcher, Permissions},
    pubsub::AsyncPubSub,
    schema::{CollectionName, Name, NamedReference, ViewName},
    transaction::Transaction,
};
use bonsaidb_local::{AsyncDatabase, Storage};

use crate::{Backend, ConnectedClient, CustomServer, Error, ServerDatabase};

#[derive(Dispatcher, Debug)]
#[dispatcher(input = Request, input = ServerRequest, actionable = bonsaidb_core::actionable)]
pub struct ServerDispatcher<'s, B: Backend> {
    pub server: &'s CustomServer<B>,
    pub client: &'s ConnectedClient<B>,
}

// impl Networking for Storage {
//     fn request(&self, request: Request) -> Result<Response, bonsaidb_core::Error> {
//         StorageDispatcher { storage: self }
//             .dispatch(&self.effective_session.permissions, request)
//             .map_err(bonsaidb_core::Error::from)
//     }
// }

// #[async_trait]
// impl AsyncNetworking for AsyncStorage {
//     async fn request(&self, request: Request) -> Result<Response, bonsaidb_core::Error> {
//         let task_self = self.clone();
//         self.runtime
//             .spawn_blocking(move || task_self.storage.request(request))
//             .await
//             .unwrap()
//     }
// }

impl<'s, B: Backend> RequestDispatcher for ServerDispatcher<'s, B> {
    type Output = Response;
    type Error = Error;

    //async fn handle_subaction(
    //     &self,
    //     permissions: &Permissions,
    //     subaction: Self::Subaction,
    // ) -> Result<Response, Error> {
    //     let dispatcher =
    //         <Backend::CustomApiDispatcher as CustomApiDispatcher>::new(self.storage);
    //     match dispatcher.dispatch(permissions, subaction) {
    //         Ok(response) => Ok(Response::Api(Ok(response))),
    //         Err(err) => match err {
    //             BackendError::Backend(backend) => Ok(Response::Api(Err(backend))),
    //             BackendError::Storage(error) => Err(error),
    //         },
    //     }
    // }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ServerHandler for ServerDispatcher<'s, B> {
    async fn handle(
        &self,
        permissions: &Permissions,
        request: ServerRequest,
    ) -> Result<Response, Error> {
        ServerRequestDispatcher::dispatch_to_handlers(self, permissions, request).await
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::DatabaseHandler for ServerDispatcher<'s, B> {
    async fn handle(
        &self,
        permissions: &Permissions,
        database_name: String,
        request: DatabaseRequest,
    ) -> Result<Response, Error> {
        let storage = Storage::from(self.server.storage.clone());
        let db = storage.database_without_schema(&database_name)?;
        DatabaseDispatcher {
            client: self.client,
            database: ServerDatabase {
                server: self.server.clone(),
                db: AsyncDatabase::from(db),
            },
        }
        .dispatch(permissions, request)
        .await
    }
}

#[async_trait]
impl<'s, B: Backend> ServerRequestDispatcher for ServerDispatcher<'s, B> {
    type Output = Response;
    type Error = Error;
}

#[async_trait]
impl<'s, B: Backend> CreateDatabaseHandler for ServerDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        database: bonsaidb_core::connection::Database,
        only_if_needed: bool,
    ) -> Result<Response, Error> {
        self.server
            .create_database_with_schema(&database.name, database.schema, only_if_needed)
            .await?;
        Ok(Response::Server(ServerResponse::DatabaseCreated {
            name: database.name,
        }))
    }
}

#[async_trait]
impl<'s, B: Backend> DeleteDatabaseHandler for ServerDispatcher<'s, B> {
    async fn handle(&self, _permissions: &Permissions, name: String) -> Result<Response, Error> {
        self.server.storage.delete_database(&name).await?;
        Ok(Response::Server(ServerResponse::DatabaseDeleted { name }))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ListDatabasesHandler for ServerDispatcher<'s, B> {
    async fn handle(&self, _permissions: &Permissions) -> Result<Response, Error> {
        Ok(Response::Server(ServerResponse::Databases(
            self.server.list_databases().await?,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ListAvailableSchemasHandler
    for ServerDispatcher<'s, B>
{
    async fn handle(&self, _permissions: &Permissions) -> Result<Response, Error> {
        Ok(Response::Server(ServerResponse::AvailableSchemas(
            self.server.list_available_schemas().await?,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::CreateUserHandler for ServerDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        username: String,
    ) -> Result<Response, Error> {
        self.server.session();
        Ok(Response::Server(ServerResponse::UserCreated {
            id: self.server.create_user(&username).await?,
        }))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::DeleteUserHandler for ServerDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        user: NamedReference<'static, u64>,
    ) -> Result<Response, Error> {
        self.server.delete_user(user).await?;
        Ok(Response::Ok)
    }
}

#[cfg(feature = "password-hashing")]
#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::SetUserPasswordHandler for ServerDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        username: NamedReference<'static, u64>,
        password: bonsaidb_core::connection::SensitiveString,
    ) -> Result<Response, Error> {
        self.server.set_user_password(username, password).await?;
        Ok(Response::Ok)
    }
}

#[cfg(feature = "password-hashing")]
#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::AuthenticateHandler for ServerDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        username: NamedReference<'static, u64>,
        authentication: Authentication,
    ) -> Result<Response, Error> {
        let authenticated = self
            .server
            .authenticate(username.clone(), authentication)
            .await?;
        let session = authenticated.session().cloned().unwrap();

        self.client.logged_in_as(session.clone());

        Ok(Response::Server(ServerResponse::Authenticated(session)))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::AssumeIdentityHandler for ServerDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        identity: Identity,
    ) -> Result<Response, Error> {
        let authenticated = self.server.assume_identity(identity).await?;
        let session = authenticated.session().cloned().unwrap();

        self.client.logged_in_as(session.clone());

        Ok(Response::Server(ServerResponse::Authenticated(session)))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::AlterUserPermissionGroupMembershipHandler
    for ServerDispatcher<'s, B>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        user: NamedReference<'static, u64>,
        group: NamedReference<'static, u64>,
        should_be_member: bool,
    ) -> Result<Response, Error> {
        if should_be_member {
            self.server
                .add_permission_group_to_user(user, group)
                .await?;
        } else {
            self.server
                .remove_permission_group_from_user(user, group)
                .await?;
        }

        Ok(Response::Ok)
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::AlterUserRoleMembershipHandler
    for ServerDispatcher<'s, B>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        user: NamedReference<'static, u64>,
        role: NamedReference<'static, u64>,
        should_be_member: bool,
    ) -> Result<Response, Error> {
        if should_be_member {
            self.server.add_role_to_user(user, role).await?;
        } else {
            self.server.remove_role_from_user(user, role).await?;
        }

        Ok(Response::Ok)
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ApiHandler for ServerDispatcher<'s, B> {
    async fn handle(
        &self,
        permissions: &Permissions,
        name: Name,
        request: Bytes,
    ) -> Result<Response, Error> {
        if let Some(dispatcher) = self.server.custom_api_dispatcher(&name) {
            dispatcher
                .dispatch(self.server, self.client, permissions, &request)
                .await
                .map(|response| Response::Api { name, response })
        } else {
            Err(Error::from(bonsaidb_core::Error::CustomApiNotFound(name)))
        }
    }
}

#[derive(Dispatcher, Debug)]
#[dispatcher(input = DatabaseRequest, actionable = bonsaidb_core::actionable)]
struct DatabaseDispatcher<'s, B: Backend> {
    database: ServerDatabase<B>,
    client: &'s ConnectedClient<B>,
}

#[async_trait]
impl<'s, B: Backend> DatabaseRequestDispatcher for DatabaseDispatcher<'s, B> {
    type Output = Response;
    type Error = Error;
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::GetHandler for DatabaseDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        id: DocumentId,
    ) -> Result<Response, Error> {
        let document = self
            .database
            .get_from_collection(id, &collection)
            .await?
            .ok_or_else(|| {
                Error::Core(bonsaidb_core::Error::DocumentNotFound(
                    collection,
                    Box::new(id),
                ))
            })?;
        Ok(Response::Database(DatabaseResponse::Documents(vec![
            document,
        ])))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::GetMultipleHandler for DatabaseDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        ids: Vec<DocumentId>,
    ) -> Result<Response, Error> {
        let documents = self
            .database
            .get_multiple_from_collection(&ids, &collection)
            .await?;
        Ok(Response::Database(DatabaseResponse::Documents(documents)))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ListHandler for DatabaseDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
    ) -> Result<Response, Error> {
        let documents = self
            .database
            .list_from_collection(ids, order, limit, &collection)
            .await?;
        Ok(Response::Database(DatabaseResponse::Documents(documents)))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::CountHandler for DatabaseDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        ids: Range<DocumentId>,
    ) -> Result<Response, Error> {
        let documents = self
            .database
            .count_from_collection(ids, &collection)
            .await?;
        Ok(Response::Database(DatabaseResponse::Count(documents)))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::QueryHandler for DatabaseDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        view: ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
        with_docs: bool,
    ) -> Result<Response, Error> {
        if with_docs {
            let mappings = self
                .database
                .query_by_name_with_docs(&view, key, order, limit, access_policy)
                .await?;
            Ok(Response::Database(DatabaseResponse::ViewMappingsWithDocs(
                mappings,
            )))
        } else {
            let mappings = self
                .database
                .query_by_name(&view, key, order, limit, access_policy)
                .await?;
            Ok(Response::Database(DatabaseResponse::ViewMappings(mappings)))
        }
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ReduceHandler for DatabaseDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        view: ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
        grouped: bool,
    ) -> Result<Response, Error> {
        if grouped {
            let values = self
                .database
                .reduce_grouped_by_name(&view, key, access_policy)
                .await?;
            Ok(Response::Database(DatabaseResponse::ViewGroupedReduction(
                values,
            )))
        } else {
            let value = self
                .database
                .reduce_by_name(&view, key, access_policy)
                .await?;
            Ok(Response::Database(DatabaseResponse::ViewReduction(
                Bytes::from(value),
            )))
        }
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ApplyTransactionHandler
    for DatabaseDispatcher<'s, B>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        transaction: Transaction,
    ) -> Result<Response, Error> {
        let results = self.database.apply_transaction(transaction).await?;
        Ok(Response::Database(DatabaseResponse::TransactionResults(
            results,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::DeleteDocsHandler for DatabaseDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        view: ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Response, Error> {
        let count = self
            .database
            .delete_docs_by_name(&view, key, access_policy)
            .await?;
        Ok(Response::Database(DatabaseResponse::Count(count)))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ListExecutedTransactionsHandler
    for DatabaseDispatcher<'s, B>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        starting_id: Option<u64>,
        result_limit: Option<u32>,
    ) -> Result<Response, Error> {
        Ok(Response::Database(DatabaseResponse::ExecutedTransactions(
            self.database
                .list_executed_transactions(starting_id, result_limit)
                .await?,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::LastTransactionIdHandler
    for DatabaseDispatcher<'s, B>
{
    async fn handle(&self, _permissions: &Permissions) -> Result<Response, Error> {
        Ok(Response::Database(DatabaseResponse::LastTransactionId(
            self.database.last_transaction_id().await?,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::CreateSubscriberHandler
    for DatabaseDispatcher<'s, B>
{
    async fn handle(&self, _permissions: &Permissions) -> Result<Response, Error> {
        let subscriber = self.database.create_subscriber().await?;
        let subscriber_id = subscriber.id();

        self.client.register_subscriber(
            subscriber,
            self.database
                .server
                .session()
                .and_then(|session| session.id),
        );

        Ok(Response::Database(DatabaseResponse::SubscriberCreated {
            subscriber_id,
        }))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::PublishHandler for DatabaseDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        topic: String,
        payload: Bytes,
    ) -> Result<Response, Error> {
        self.database
            .publish_bytes(&topic, payload.into_vec())
            .await?;
        Ok(Response::Ok)
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::PublishToAllHandler for DatabaseDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        topics: Vec<String>,
        payload: Bytes,
    ) -> Result<Response, Error> {
        self.database
            .publish_bytes_to_all(topics, payload.into_vec())
            .await?;
        Ok(Response::Ok)
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::SubscribeToHandler for DatabaseDispatcher<'s, B> {
    async fn handle(
        &self,
        _permissions: &Permissions,
        subscriber_id: u64,
        topic: String,
    ) -> Result<Response, Error> {
        self.client
            .subscribe_by_id(
                subscriber_id,
                topic,
                self.database
                    .server
                    .session()
                    .and_then(|session| session.id),
            )
            .map(|_| Response::Ok)
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::UnsubscribeFromHandler
    for DatabaseDispatcher<'s, B>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        subscriber_id: u64,
        topic: String,
    ) -> Result<Response, Error> {
        self.client
            .unsubscribe_by_id(
                subscriber_id,
                &topic,
                self.database
                    .server
                    .session()
                    .and_then(|session| session.id),
            )
            .map(|_| Response::Ok)
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::UnregisterSubscriberHandler
    for DatabaseDispatcher<'s, B>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        subscriber_id: u64,
    ) -> Result<Response, Error> {
        self.client
            .unregister_subscriber_by_id(
                subscriber_id,
                self.database
                    .server
                    .session()
                    .and_then(|session| session.id),
            )
            .map(|_| Response::Ok)
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ExecuteKeyOperationHandler
    for DatabaseDispatcher<'s, B>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        op: KeyOperation,
    ) -> Result<Response, Error> {
        let result = self.database.execute_key_operation(op).await?;
        Ok(Response::Database(DatabaseResponse::KvOutput(result)))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::CompactCollectionHandler
    for DatabaseDispatcher<'s, B>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
    ) -> Result<Response, Error> {
        self.database.compact_collection_by_name(collection).await?;

        Ok(Response::Ok)
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::CompactKeyValueStoreHandler
    for DatabaseDispatcher<'s, B>
{
    async fn handle(&self, _permissions: &Permissions) -> Result<Response, Error> {
        self.database.compact_key_value_store().await?;

        Ok(Response::Ok)
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::CompactHandler for DatabaseDispatcher<'s, B> {
    async fn handle(&self, _permissions: &Permissions) -> Result<Response, Error> {
        self.database.compact().await?;

        Ok(Response::Ok)
    }
}
