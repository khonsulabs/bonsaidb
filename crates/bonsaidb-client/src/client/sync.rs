use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bonsaidb_core::admin::{Admin, ADMIN_DATABASE_NAME};
use bonsaidb_core::api;
use bonsaidb_core::arc_bytes::serde::Bytes;
use bonsaidb_core::connection::{
    AccessPolicy, Connection, Database, HasSchema, HasSession, IdentityReference,
    LowLevelConnection, Range, SerializedQueryKey, Sort, StorageConnection,
};
use bonsaidb_core::document::{DocumentId, Header, OwnedDocument};
use bonsaidb_core::keyvalue::KeyValue;
use bonsaidb_core::networking::{
    AlterUserPermissionGroupMembership, AlterUserRoleMembership, ApplyTransaction, AssumeIdentity,
    Compact, CompactCollection, CompactKeyValueStore, Count, CreateDatabase, CreateSubscriber,
    CreateUser, DeleteDatabase, DeleteDocs, DeleteUser, ExecuteKeyOperation, Get, GetMultiple,
    LastTransactionId, List, ListAvailableSchemas, ListDatabases, ListExecutedTransactions,
    ListHeaders, Publish, PublishToAll, Query, QueryWithDocs, Reduce, ReduceGrouped, SubscribeTo,
    UnsubscribeFrom, CURRENT_PROTOCOL_VERSION,
};
use bonsaidb_core::pubsub::{AsyncSubscriber, PubSub, Receiver, Subscriber};
use bonsaidb_core::schema::view::map;
use bonsaidb_core::schema::{CollectionName, ViewName};
use futures::Future;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use url::Url;

use crate::builder::Blocking;
use crate::client::ClientSession;
use crate::{ApiError, AsyncClient, AsyncRemoteDatabase, AsyncRemoteSubscriber, Builder, Error};

/// A BonsaiDb client that blocks the current thread when performing requests.
#[derive(Debug, Clone)]
pub struct BlockingClient(pub(crate) AsyncClient);

impl BlockingClient {
    /// Returns a builder for a new client connecting to `url`.
    pub fn build(url: Url) -> Builder<Blocking> {
        Builder::new(url)
    }

    /// Initialize a client connecting to `url`. This client can be shared by
    /// cloning it. All requests are done asynchronously over the same
    /// connection.
    ///
    /// If the client has an error connecting, the first request made will
    /// present that error. If the client disconnects while processing requests,
    /// all requests being processed will exit and return
    /// [`Error::Disconnected`]. The client will automatically try reconnecting.
    ///
    /// The goal of this design of this reconnection strategy is to make it
    /// easier to build resilliant apps. By allowing existing Client instances
    /// to recover and reconnect, each component of the apps built can adopt a
    /// "retry-to-recover" design, or "abort-and-fail" depending on how critical
    /// the database is to operation.
    pub fn new(url: Url) -> Result<Self, Error> {
        AsyncClient::new_from_parts(
            url,
            CURRENT_PROTOCOL_VERSION,
            HashMap::default(),
            #[cfg(not(target_arch = "wasm32"))]
            None,
            #[cfg(not(target_arch = "wasm32"))]
            Handle::try_current().ok(),
        )
        .map(Self)
    }

    /// Sends an api `request`.
    pub fn send_api_request<Api: api::Api>(
        &self,
        request: &Api,
    ) -> Result<Api::Response, ApiError<Api::Error>> {
        self.0.send_blocking_api_request(request)
    }

    /// Sends an api `request` without waiting for a result. The response from
    /// the server will be ignored.
    pub fn invoke_api_request<Api: api::Api>(&self, request: &Api) -> Result<(), Error> {
        let request = Bytes::from(pot::to_vec(request).map_err(Error::from)?);
        self.0
            .send_request_without_confirmation(Api::name(), request)
            .map(|_| ())
    }

    /// Returns a reference to an async-compatible version of this client.
    #[must_use]
    pub fn as_async(&self) -> &AsyncClient {
        &self.0
    }
}

impl From<AsyncClient> for BlockingClient {
    fn from(client: AsyncClient) -> Self {
        Self(client)
    }
}

impl From<BlockingClient> for AsyncClient {
    fn from(client: BlockingClient) -> Self {
        client.0
    }
}

impl StorageConnection for BlockingClient {
    type Authenticated = Self;
    type Database = BlockingRemoteDatabase;

    fn admin(&self) -> Self::Database {
        BlockingRemoteDatabase(
            self.0
                .remote_database::<Admin>(ADMIN_DATABASE_NAME)
                .unwrap(),
        )
    }

    fn database<DB: bonsaidb_core::schema::Schema>(
        &self,
        name: &str,
    ) -> Result<Self::Database, bonsaidb_core::Error> {
        self.0
            .remote_database::<DB>(name)
            .map(BlockingRemoteDatabase)
    }

    fn create_database_with_schema(
        &self,
        name: &str,
        schema: bonsaidb_core::schema::SchemaName,
        only_if_needed: bool,
    ) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request(&CreateDatabase {
            database: Database {
                name: name.to_string(),
                schema,
            },
            only_if_needed,
        })?;
        Ok(())
    }

    fn delete_database(&self, name: &str) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request(&DeleteDatabase {
            name: name.to_string(),
        })?;
        Ok(())
    }

    fn list_databases(
        &self,
    ) -> Result<Vec<bonsaidb_core::connection::Database>, bonsaidb_core::Error> {
        Ok(self.send_api_request(&ListDatabases)?)
    }

    fn list_available_schemas(
        &self,
    ) -> Result<Vec<bonsaidb_core::schema::SchemaName>, bonsaidb_core::Error> {
        Ok(self.send_api_request(&ListAvailableSchemas)?)
    }

    fn create_user(&self, username: &str) -> Result<u64, bonsaidb_core::Error> {
        Ok(self.send_api_request(&CreateUser {
            username: username.to_string(),
        })?)
    }

    fn delete_user<'user, U: bonsaidb_core::schema::Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
    ) -> Result<(), bonsaidb_core::Error> {
        Ok(self.send_api_request(&DeleteUser {
            user: user.name()?.into_owned(),
        })?)
    }

    #[cfg(feature = "password-hashing")]
    fn set_user_password<'user, U: bonsaidb_core::schema::Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        password: bonsaidb_core::connection::SensitiveString,
    ) -> Result<(), bonsaidb_core::Error> {
        use bonsaidb_core::networking::SetUserPassword;

        Ok(self.send_api_request(&SetUserPassword {
            user: user.name()?.into_owned(),
            password,
        })?)
    }

    #[cfg(any(feature = "token-authentication", feature = "password-hashing"))]
    fn authenticate(
        &self,
        authentication: bonsaidb_core::connection::Authentication,
    ) -> Result<Self::Authenticated, bonsaidb_core::Error> {
        let session =
            self.send_api_request(&bonsaidb_core::networking::Authenticate { authentication })?;
        Ok(Self(AsyncClient {
            data: self.0.data.clone(),
            session: ClientSession {
                session: Arc::new(session),
                connection_id: self.0.data.connection_counter.load(Ordering::SeqCst),
            },
        }))
    }

    fn assume_identity(
        &self,
        identity: IdentityReference<'_>,
    ) -> Result<Self::Authenticated, bonsaidb_core::Error> {
        let session = self.send_api_request(&AssumeIdentity(identity.into_owned()))?;
        Ok(Self(AsyncClient {
            data: self.0.data.clone(),
            session: ClientSession {
                session: Arc::new(session),
                connection_id: self.0.data.connection_counter.load(Ordering::SeqCst),
            },
        }))
    }

    fn add_permission_group_to_user<
        'user,
        'group,
        U: bonsaidb_core::schema::Nameable<'user, u64> + Send + Sync,
        G: bonsaidb_core::schema::Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request(&AlterUserPermissionGroupMembership {
            user: user.name()?.into_owned(),
            group: permission_group.name()?.into_owned(),
            should_be_member: true,
        })?;
        Ok(())
    }

    fn remove_permission_group_from_user<
        'user,
        'group,
        U: bonsaidb_core::schema::Nameable<'user, u64> + Send + Sync,
        G: bonsaidb_core::schema::Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request(&AlterUserPermissionGroupMembership {
            user: user.name()?.into_owned(),
            group: permission_group.name()?.into_owned(),
            should_be_member: false,
        })?;
        Ok(())
    }

    fn add_role_to_user<
        'user,
        'role,
        U: bonsaidb_core::schema::Nameable<'user, u64> + Send + Sync,
        R: bonsaidb_core::schema::Nameable<'role, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: R,
    ) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request(&AlterUserRoleMembership {
            user: user.name()?.into_owned(),
            role: role.name()?.into_owned(),
            should_be_member: true,
        })?;
        Ok(())
    }

    fn remove_role_from_user<
        'user,
        'role,
        U: bonsaidb_core::schema::Nameable<'user, u64> + Send + Sync,
        R: bonsaidb_core::schema::Nameable<'role, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: R,
    ) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request(&AlterUserRoleMembership {
            user: user.name()?.into_owned(),
            role: role.name()?.into_owned(),
            should_be_member: false,
        })?;
        Ok(())
    }
}

impl HasSession for BlockingClient {
    fn session(&self) -> Option<&bonsaidb_core::connection::Session> {
        self.0.session()
    }
}

/// A remote database that blocks the current thread when performing its
/// requests.
#[derive(Debug, Clone)]
pub struct BlockingRemoteDatabase(AsyncRemoteDatabase);

impl Connection for BlockingRemoteDatabase {
    type Storage = BlockingClient;

    fn storage(&self) -> Self::Storage {
        BlockingClient(self.0.client.clone())
    }

    fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<u32>,
    ) -> Result<Vec<bonsaidb_core::transaction::Executed>, bonsaidb_core::Error> {
        Ok(self
            .0
            .client
            .send_blocking_api_request(&ListExecutedTransactions {
                database: self.0.name.to_string(),
                starting_id,
                result_limit,
            })?)
    }

    fn last_transaction_id(&self) -> Result<Option<u64>, bonsaidb_core::Error> {
        Ok(self
            .0
            .client
            .send_blocking_api_request(&LastTransactionId {
                database: self.0.name.to_string(),
            })?)
    }

    fn compact(&self) -> Result<(), bonsaidb_core::Error> {
        self.0.send_blocking_api_request(&Compact {
            database: self.0.name.to_string(),
        })?;
        Ok(())
    }

    fn compact_key_value_store(&self) -> Result<(), bonsaidb_core::Error> {
        self.0.send_blocking_api_request(&CompactKeyValueStore {
            database: self.0.name.to_string(),
        })?;
        Ok(())
    }
}

impl LowLevelConnection for BlockingRemoteDatabase {
    fn apply_transaction(
        &self,
        transaction: bonsaidb_core::transaction::Transaction,
    ) -> Result<Vec<bonsaidb_core::transaction::OperationResult>, bonsaidb_core::Error> {
        Ok(self.0.client.send_blocking_api_request(&ApplyTransaction {
            database: self.0.name.to_string(),
            transaction,
        })?)
    }

    fn get_from_collection(
        &self,
        id: bonsaidb_core::document::DocumentId,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error> {
        Ok(self.0.client.send_blocking_api_request(&Get {
            database: self.0.name.to_string(),
            collection: collection.clone(),
            id,
        })?)
    }

    fn get_multiple_from_collection(
        &self,
        ids: &[bonsaidb_core::document::DocumentId],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        Ok(self.0.client.send_blocking_api_request(&GetMultiple {
            database: self.0.name.to_string(),
            collection: collection.clone(),
            ids: ids.to_vec(),
        })?)
    }

    fn list_from_collection(
        &self,
        ids: Range<bonsaidb_core::document::DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        Ok(self.0.client.send_blocking_api_request(&List {
            database: self.0.name.to_string(),
            collection: collection.clone(),
            ids,
            order,
            limit,
        })?)
    }

    fn list_headers_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<Header>, bonsaidb_core::Error> {
        Ok(self.0.client.send_blocking_api_request(&ListHeaders(List {
            database: self.0.name.to_string(),
            collection: collection.clone(),
            ids,
            order,
            limit,
        }))?)
    }

    fn count_from_collection(
        &self,
        ids: Range<bonsaidb_core::document::DocumentId>,
        collection: &CollectionName,
    ) -> Result<u64, bonsaidb_core::Error> {
        Ok(self.0.client.send_blocking_api_request(&Count {
            database: self.0.name.to_string(),
            collection: collection.clone(),
            ids,
        })?)
    }

    fn compact_collection_by_name(
        &self,
        collection: CollectionName,
    ) -> Result<(), bonsaidb_core::Error> {
        self.0.send_blocking_api_request(&CompactCollection {
            database: self.0.name.to_string(),
            name: collection,
        })?;
        Ok(())
    }

    fn query_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<map::Serialized>, bonsaidb_core::Error> {
        Ok(self.0.client.send_blocking_api_request(&Query {
            database: self.0.name.to_string(),
            view: view.clone(),
            key,
            order,
            limit,
            access_policy,
        })?)
    }

    fn query_by_name_with_docs(
        &self,
        view: &bonsaidb_core::schema::ViewName,
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<bonsaidb_core::schema::view::map::MappedSerializedDocuments, bonsaidb_core::Error>
    {
        Ok(self
            .0
            .client
            .send_blocking_api_request(&QueryWithDocs(Query {
                database: self.0.name.to_string(),
                view: view.clone(),
                key,
                order,
                limit,
                access_policy,
            }))?)
    }

    fn reduce_by_name(
        &self,
        view: &bonsaidb_core::schema::ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, bonsaidb_core::Error> {
        Ok(self
            .0
            .client
            .send_blocking_api_request(&Reduce {
                database: self.0.name.to_string(),
                view: view.clone(),
                key,
                access_policy,
            })?
            .into_vec())
    }

    fn reduce_grouped_by_name(
        &self,
        view: &bonsaidb_core::schema::ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<bonsaidb_core::schema::view::map::MappedSerializedValue>, bonsaidb_core::Error>
    {
        Ok(self
            .0
            .client
            .send_blocking_api_request(&ReduceGrouped(Reduce {
                database: self.0.name.to_string(),
                view: view.clone(),
                key,
                access_policy,
            }))?)
    }

    fn delete_docs_by_name(
        &self,
        view: &bonsaidb_core::schema::ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error> {
        Ok(self.0.client.send_blocking_api_request(&DeleteDocs {
            database: self.0.name.to_string(),
            view: view.clone(),
            key,
            access_policy,
        })?)
    }
}

impl HasSession for BlockingRemoteDatabase {
    fn session(&self) -> Option<&bonsaidb_core::connection::Session> {
        self.0.session()
    }
}

impl HasSchema for BlockingRemoteDatabase {
    fn schematic(&self) -> &bonsaidb_core::schema::Schematic {
        self.0.schematic()
    }
}

impl PubSub for BlockingRemoteDatabase {
    type Subscriber = BlockingRemoteSubscriber;

    fn create_subscriber(&self) -> Result<Self::Subscriber, bonsaidb_core::Error> {
        let subscriber_id = self.0.client.send_blocking_api_request(&CreateSubscriber {
            database: self.0.name.to_string(),
        })?;

        let (sender, receiver) = flume::unbounded();
        self.0.client.register_subscriber(subscriber_id, sender);
        Ok(BlockingRemoteSubscriber(AsyncRemoteSubscriber {
            client: self.0.client.clone(),
            database: self.0.name.clone(),
            id: subscriber_id,
            receiver: Receiver::new(receiver),
            tokio: None,
        }))
    }

    fn publish_bytes(&self, topic: Vec<u8>, payload: Vec<u8>) -> Result<(), bonsaidb_core::Error> {
        self.0.client.send_blocking_api_request(&Publish {
            database: self.0.name.to_string(),
            topic: Bytes::from(topic),
            payload: Bytes::from(payload),
        })?;
        Ok(())
    }

    fn publish_bytes_to_all(
        &self,
        topics: impl IntoIterator<Item = Vec<u8>> + Send,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        let topics = topics.into_iter().map(Bytes::from).collect();
        self.0.client.send_blocking_api_request(&PublishToAll {
            database: self.0.name.to_string(),
            topics,
            payload: Bytes::from(payload),
        })?;
        Ok(())
    }
}

/// A remote PubSub [`Subscriber`] that blocks the current thread when
/// performing requests.
#[derive(Debug)]
pub struct BlockingRemoteSubscriber(AsyncRemoteSubscriber);

impl Subscriber for BlockingRemoteSubscriber {
    fn subscribe_to_bytes(&self, topic: Vec<u8>) -> Result<(), bonsaidb_core::Error> {
        self.0.client.send_blocking_api_request(&SubscribeTo {
            database: self.0.database.to_string(),
            subscriber_id: self.0.id,
            topic: Bytes::from(topic),
        })?;
        Ok(())
    }

    fn unsubscribe_from_bytes(&self, topic: &[u8]) -> Result<(), bonsaidb_core::Error> {
        self.0.client.send_blocking_api_request(&UnsubscribeFrom {
            database: self.0.database.to_string(),
            subscriber_id: self.0.id,
            topic: Bytes::from(topic),
        })?;
        Ok(())
    }

    fn receiver(&self) -> &Receiver {
        AsyncSubscriber::receiver(&self.0)
    }
}

impl KeyValue for BlockingRemoteDatabase {
    fn execute_key_operation(
        &self,
        op: bonsaidb_core::keyvalue::KeyOperation,
    ) -> Result<bonsaidb_core::keyvalue::Output, bonsaidb_core::Error> {
        Ok(self
            .0
            .client
            .send_blocking_api_request(&ExecuteKeyOperation {
                database: self.0.name.to_string(),

                op,
            })?)
    }
}

pub enum Tokio {
    Runtime(Runtime),
    Handle(Handle),
}

impl Tokio {
    pub fn spawn<F: Future<Output = Result<(), crate::Error>> + Send + 'static>(
        self,
        task: F,
    ) -> JoinHandle<Result<(), crate::Error>> {
        match self {
            Self::Runtime(tokio) => {
                // When we have an owned runtime, we must have a thread driving
                // the runtime. To keep the interface to `Client` simple, we are
                // going to spawn the task and let the main block_on task simply
                // wait for the completion event. If the JoinHandle is
                // cancelled, the sender will be dropped and everything will
                // clean up.
                let (completion_sender, completion_receiver) = oneshot::channel();
                let task = async move {
                    task.await?;
                    let _ = completion_sender.send(());
                    Ok(())
                };
                let task = tokio.spawn(task);

                std::thread::spawn(move || {
                    tokio.block_on(async move {
                        let _ = completion_receiver.await;
                    });
                });
                task
            }
            Self::Handle(tokio) => tokio.spawn(task),
        }
    }
}

pub fn spawn_client<F: Future<Output = Result<(), crate::Error>> + Send + 'static>(
    task: F,
    handle: Option<Handle>,
) -> JoinHandle<Result<(), crate::Error>> {
    // We need to spawn a runtime or
    let tokio = if let Some(handle) = handle {
        Tokio::Handle(handle)
    } else {
        Tokio::Runtime(Runtime::new().unwrap())
    };
    tokio.spawn(task)
}
