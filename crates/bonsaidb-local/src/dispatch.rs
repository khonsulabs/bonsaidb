#[cfg(feature = "password-hashing")]
use bonsaidb_core::connection::Authentication;
use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    connection::{AccessPolicy, Connection, QueryKey, Range, Sort, StorageConnection},
    custom_api::{CustomApi, CustomApiResult},
    document::DocumentId,
    keyvalue::{KeyOperation, KeyValue},
    networking::{
        CreateDatabaseHandler, DatabaseRequest, DatabaseRequestDispatcher, DatabaseResponse,
        DeleteDatabaseHandler, Request, RequestDispatcher, Response, ServerRequest,
        ServerRequestDispatcher, ServerResponse,
    },
    permissions::{Dispatcher, Permissions},
    pubsub::PubSub,
    schema::{CollectionName, Name, NamedReference, ViewName},
    transaction::Transaction,
};

use crate::{
    custom_api::{self, CustomApiDispatcher, DispatchError},
    Database, Error, Storage,
};

#[derive(Dispatcher, Debug)]
#[dispatcher(input = Request, input = ServerRequest, actionable = bonsaidb_core::actionable)]
struct StorageDispatcher<'s> {
    storage: &'s Storage,
}

impl<'s> RequestDispatcher for StorageDispatcher<'s> {
    type Output = Response;
    type Error = Error;

    // fn handle_subaction(
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

impl<'s> bonsaidb_core::networking::ServerHandler for StorageDispatcher<'s> {
    fn handle(&self, permissions: &Permissions, request: ServerRequest) -> Result<Response, Error> {
        ServerRequestDispatcher::dispatch_to_handlers(self, permissions, request)
    }
}

impl<'s> bonsaidb_core::networking::DatabaseHandler for StorageDispatcher<'s> {
    fn handle(
        &self,
        permissions: &Permissions,
        database_name: String,
        request: DatabaseRequest,
    ) -> Result<Response, Error> {
        let database = self.storage.database_without_schema(&database_name)?;
        DatabaseDispatcher { database }.dispatch(permissions, request)
    }
}

impl<'s> ServerRequestDispatcher for StorageDispatcher<'s> {
    type Output = Response;
    type Error = Error;
}

impl<'s> CreateDatabaseHandler for StorageDispatcher<'s> {
    fn handle(
        &self,
        _permissions: &Permissions,
        database: bonsaidb_core::connection::Database,
        only_if_needed: bool,
    ) -> Result<Response, Error> {
        self.storage.create_database_with_schema(
            &database.name,
            database.schema,
            only_if_needed,
        )?;
        Ok(Response::Server(ServerResponse::DatabaseCreated {
            name: database.name,
        }))
    }
}

impl<'s> DeleteDatabaseHandler for StorageDispatcher<'s> {
    fn handle(&self, _permissions: &Permissions, name: String) -> Result<Response, Error> {
        self.storage.delete_database(&name)?;
        Ok(Response::Server(ServerResponse::DatabaseDeleted { name }))
    }
}

impl<'s> bonsaidb_core::networking::ListDatabasesHandler for StorageDispatcher<'s> {
    fn handle(&self, _permissions: &Permissions) -> Result<Response, Error> {
        Ok(Response::Server(ServerResponse::Databases(
            self.storage.list_databases()?,
        )))
    }
}

impl<'s> bonsaidb_core::networking::ListAvailableSchemasHandler for StorageDispatcher<'s> {
    fn handle(&self, _permissions: &Permissions) -> Result<Response, Error> {
        Ok(Response::Server(ServerResponse::AvailableSchemas(
            self.storage.list_available_schemas()?,
        )))
    }
}

impl<'s> bonsaidb_core::networking::CreateUserHandler for StorageDispatcher<'s> {
    fn handle(&self, _permissions: &Permissions, username: String) -> Result<Response, Error> {
        Ok(Response::Server(ServerResponse::UserCreated {
            id: self.storage.create_user(&username)?,
        }))
    }
}

impl<'s> bonsaidb_core::networking::DeleteUserHandler for StorageDispatcher<'s> {
    fn handle(
        &self,
        _permissions: &Permissions,
        user: NamedReference<'static, u64>,
    ) -> Result<Response, Error> {
        self.storage.delete_user(user)?;
        Ok(Response::Ok)
    }
}

#[cfg(feature = "password-hashing")]
impl<'s> bonsaidb_core::networking::SetUserPasswordHandler for StorageDispatcher<'s> {
    fn handle(
        &self,
        _permissions: &Permissions,
        username: NamedReference<'static, u64>,
        password: bonsaidb_core::connection::SensitiveString,
    ) -> Result<Response, Error> {
        self.storage.set_user_password(username, password)?;
        Ok(Response::Ok)
    }
}

#[cfg(feature = "password-hashing")]
impl<'s> bonsaidb_core::networking::AuthenticateHandler for StorageDispatcher<'s> {
    fn handle(
        &self,
        _permissions: &Permissions,
        username: NamedReference<'static, u64>,
        authentication: Authentication,
    ) -> Result<Response, Error> {
        let authenticated = self
            .storage
            .authenticate(username.clone(), authentication)?;

        Ok(Response::Server(ServerResponse::Authenticated(
            authenticated.session().cloned().unwrap(),
        )))
    }
}

impl<'s> bonsaidb_core::networking::AlterUserPermissionGroupMembershipHandler
    for StorageDispatcher<'s>
{
    fn handle(
        &self,
        _permissions: &Permissions,
        user: NamedReference<'static, u64>,
        group: NamedReference<'static, u64>,
        should_be_member: bool,
    ) -> Result<Response, Error> {
        if should_be_member {
            self.storage.add_permission_group_to_user(user, group)?;
        } else {
            self.storage
                .remove_permission_group_from_user(user, group)?;
        }

        Ok(Response::Ok)
    }
}

impl<'s> bonsaidb_core::networking::AlterUserRoleMembershipHandler for StorageDispatcher<'s> {
    fn handle(
        &self,
        _permissions: &Permissions,
        user: NamedReference<'static, u64>,
        role: NamedReference<'static, u64>,
        should_be_member: bool,
    ) -> Result<Response, Error> {
        if should_be_member {
            self.storage.add_role_to_user(user, role)?;
        } else {
            self.storage.remove_role_from_user(user, role)?;
        }

        Ok(Response::Ok)
    }
}

impl<'s> bonsaidb_core::networking::ApiHandler for StorageDispatcher<'s> {
    fn handle(
        &self,
        permissions: &Permissions,
        name: Name,
        request: Bytes,
    ) -> Result<Response, Error> {
        if let Some(dispatcher) = self.storage.instance.custom_api_dispatcher(&name) {
            dispatcher
                .dispatch(permissions, &request)
                .map(|response| Response::Api { name, response })
        } else {
            Err(Error::from(bonsaidb_core::Error::CustomApiNotFound(name)))
        }
    }
}

#[derive(Dispatcher, Debug)]
#[dispatcher(input = DatabaseRequest, actionable = bonsaidb_core::actionable)]
struct DatabaseDispatcher {
    database: Database,
}

impl DatabaseRequestDispatcher for DatabaseDispatcher {
    type Output = Response;
    type Error = Error;
}

impl bonsaidb_core::networking::GetHandler for DatabaseDispatcher {
    fn handle(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        id: DocumentId,
    ) -> Result<Response, Error> {
        let document = self
            .database
            .internal_get_from_collection_id(id, &collection)?
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

impl bonsaidb_core::networking::GetMultipleHandler for DatabaseDispatcher {
    fn handle(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        ids: Vec<DocumentId>,
    ) -> Result<Response, Error> {
        let documents = self
            .database
            .internal_get_multiple_from_collection_id(&ids, &collection)?;
        Ok(Response::Database(DatabaseResponse::Documents(documents)))
    }
}

impl bonsaidb_core::networking::ListHandler for DatabaseDispatcher {
    fn handle(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<usize>,
    ) -> Result<Response, Error> {
        let documents = self
            .database
            .list_from_collection(ids, order, limit, &collection)?;
        Ok(Response::Database(DatabaseResponse::Documents(documents)))
    }
}

impl bonsaidb_core::networking::CountHandler for DatabaseDispatcher {
    fn handle(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        ids: Range<DocumentId>,
    ) -> Result<Response, Error> {
        let documents = self.database.count_from_collection(ids, &collection)?;
        Ok(Response::Database(DatabaseResponse::Count(documents)))
    }
}

impl bonsaidb_core::networking::QueryHandler for DatabaseDispatcher {
    fn handle(
        &self,
        _permissions: &Permissions,
        view: ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
        with_docs: bool,
    ) -> Result<Response, Error> {
        if with_docs {
            let mappings =
                self.database
                    .query_by_name_with_docs(&view, key, order, limit, access_policy)?;
            Ok(Response::Database(DatabaseResponse::ViewMappingsWithDocs(
                mappings,
            )))
        } else {
            let mappings = self
                .database
                .query_by_name(&view, key, order, limit, access_policy)?;
            Ok(Response::Database(DatabaseResponse::ViewMappings(mappings)))
        }
    }
}

impl bonsaidb_core::networking::ReduceHandler for DatabaseDispatcher {
    fn handle(
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
                .reduce_grouped_by_name(&view, key, access_policy)?;
            Ok(Response::Database(DatabaseResponse::ViewGroupedReduction(
                values,
            )))
        } else {
            let value = self.database.reduce_by_name(&view, key, access_policy)?;
            Ok(Response::Database(DatabaseResponse::ViewReduction(
                Bytes::from(value),
            )))
        }
    }
}

impl bonsaidb_core::networking::ApplyTransactionHandler for DatabaseDispatcher {
    fn handle(
        &self,
        _permissions: &Permissions,
        transaction: Transaction,
    ) -> Result<Response, Error> {
        let results = self.database.apply_transaction(transaction)?;
        Ok(Response::Database(DatabaseResponse::TransactionResults(
            results,
        )))
    }
}

impl bonsaidb_core::networking::DeleteDocsHandler for DatabaseDispatcher {
    fn handle(
        &self,
        _permissions: &Permissions,
        view: ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Response, Error> {
        let count = self
            .database
            .delete_docs_by_name(&view, key, access_policy)?;
        Ok(Response::Database(DatabaseResponse::Count(count)))
    }
}

impl bonsaidb_core::networking::ListExecutedTransactionsHandler for DatabaseDispatcher {
    fn handle(
        &self,
        _permissions: &Permissions,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Response, Error> {
        Ok(Response::Database(DatabaseResponse::ExecutedTransactions(
            self.database
                .list_executed_transactions(starting_id, result_limit)?,
        )))
    }
}

impl bonsaidb_core::networking::LastTransactionIdHandler for DatabaseDispatcher {
    fn handle(&self, _permissions: &Permissions) -> Result<Response, Error> {
        Ok(Response::Database(DatabaseResponse::LastTransactionId(
            self.database.last_transaction_id()?,
        )))
    }
}

impl bonsaidb_core::networking::CreateSubscriberHandler for DatabaseDispatcher {
    fn handle(&self, _permissions: &Permissions) -> Result<Response, Error> {
        let subscriber = self.database.create_subscriber()?;
        let subscriber_id = subscriber.id;

        Ok(Response::Database(DatabaseResponse::SubscriberCreated {
            subscriber_id,
        }))
    }
}

impl bonsaidb_core::networking::PublishHandler for DatabaseDispatcher {
    fn handle(
        &self,
        _permissions: &Permissions,
        topic: String,
        payload: Bytes,
    ) -> Result<Response, Error> {
        self.database.publish_bytes(&topic, payload.into_vec())?;
        Ok(Response::Ok)
    }
}

impl bonsaidb_core::networking::PublishToAllHandler for DatabaseDispatcher {
    fn handle(
        &self,
        _permissions: &Permissions,
        topics: Vec<String>,
        payload: Bytes,
    ) -> Result<Response, Error> {
        self.database
            .publish_bytes_to_all(topics, payload.into_vec())?;
        Ok(Response::Ok)
    }
}

impl bonsaidb_core::networking::SubscribeToHandler for DatabaseDispatcher {
    fn handle(
        &self,
        _permissions: &Permissions,
        subscriber_id: u64,
        topic: String,
    ) -> Result<Response, Error> {
        self.database
            .subscribe_by_id(subscriber_id, &topic)
            .map(|_| Response::Ok)
    }
}

impl bonsaidb_core::networking::UnsubscribeFromHandler for DatabaseDispatcher {
    fn handle(
        &self,
        _permissions: &Permissions,
        subscriber_id: u64,
        topic: String,
    ) -> Result<Response, Error> {
        self.database
            .unsubscribe_by_id(subscriber_id, &topic)
            .map(|_| Response::Ok)
    }
}

impl bonsaidb_core::networking::UnregisterSubscriberHandler for DatabaseDispatcher {
    fn handle(&self, _permissions: &Permissions, subscriber_id: u64) -> Result<Response, Error> {
        self.database
            .unregister_subscriber_by_id(subscriber_id)
            .map(|_| Response::Ok)
    }
}

impl bonsaidb_core::networking::ExecuteKeyOperationHandler for DatabaseDispatcher {
    fn handle(&self, _permissions: &Permissions, op: KeyOperation) -> Result<Response, Error> {
        let result = self.database.execute_key_operation(op)?;
        Ok(Response::Database(DatabaseResponse::KvOutput(result)))
    }
}

impl bonsaidb_core::networking::CompactCollectionHandler for DatabaseDispatcher {
    fn handle(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
    ) -> Result<Response, Error> {
        self.database.compact_collection_by_name(collection)?;

        Ok(Response::Ok)
    }
}

impl bonsaidb_core::networking::CompactKeyValueStoreHandler for DatabaseDispatcher {
    fn handle(&self, _permissions: &Permissions) -> Result<Response, Error> {
        self.database.compact_key_value_store()?;

        Ok(Response::Ok)
    }
}

impl bonsaidb_core::networking::CompactHandler for DatabaseDispatcher {
    fn handle(&self, _permissions: &Permissions) -> Result<Response, Error> {
        self.database.compact()?;

        Ok(Response::Ok)
    }
}
