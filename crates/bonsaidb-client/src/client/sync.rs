use std::sync::atomic::Ordering;
use std::sync::Arc;

use bonsaidb_core::admin::{Admin, ADMIN_DATABASE_NAME};
use bonsaidb_core::arc_bytes::serde::Bytes;
use bonsaidb_core::connection::{
    AccessPolicy, Connection, Database, IdentityReference, LowLevelConnection, Range,
    SerializedQueryKey, Sort, StorageConnection,
};
use bonsaidb_core::document::{DocumentId, Header, OwnedDocument};
use bonsaidb_core::keyvalue::KeyValue;
use bonsaidb_core::networking::{
    AlterUserPermissionGroupMembership, AlterUserRoleMembership, ApplyTransaction, AssumeIdentity,
    Compact, CompactCollection, CompactKeyValueStore, Count, CreateDatabase, CreateSubscriber,
    CreateUser, DeleteDatabase, DeleteDocs, DeleteUser, ExecuteKeyOperation, Get, GetMultiple,
    LastTransactionId, List, ListAvailableSchemas, ListDatabases, ListExecutedTransactions,
    ListHeaders, Publish, PublishToAll, Query, QueryWithDocs, Reduce, ReduceGrouped, SubscribeTo,
    UnsubscribeFrom,
};
use bonsaidb_core::pubsub::{AsyncSubscriber, PubSub, Receiver, Subscriber};
use bonsaidb_core::schema::view::map;
use bonsaidb_core::schema::{CollectionName, ViewName};
use futures::Future;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::client::ClientSession;
use crate::{Client, RemoteDatabase, RemoteSubscriber};

impl StorageConnection for Client {
    type Authenticated = Self;
    type Database = RemoteDatabase;

    fn admin(&self) -> Self::Database {
        self.remote_database::<Admin>(ADMIN_DATABASE_NAME).unwrap()
    }

    fn database<DB: bonsaidb_core::schema::Schema>(
        &self,
        name: &str,
    ) -> Result<Self::Database, bonsaidb_core::Error> {
        self.remote_database::<DB>(name)
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
        Ok(Self {
            data: self.data.clone(),
            session: ClientSession {
                session: Arc::new(session),
                connection_id: self.data.connection_counter.load(Ordering::SeqCst),
            },
        })
    }

    fn assume_identity(
        &self,
        identity: IdentityReference<'_>,
    ) -> Result<Self::Authenticated, bonsaidb_core::Error> {
        let session = self.send_api_request(&AssumeIdentity(identity.into_owned()))?;
        Ok(Self {
            data: self.data.clone(),
            session: ClientSession {
                session: Arc::new(session),
                connection_id: self.data.connection_counter.load(Ordering::SeqCst),
            },
        })
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

impl Connection for RemoteDatabase {
    type Storage = Client;

    fn storage(&self) -> Self::Storage {
        self.client.clone()
    }

    fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<u32>,
    ) -> Result<Vec<bonsaidb_core::transaction::Executed>, bonsaidb_core::Error> {
        Ok(self.client.send_api_request(&ListExecutedTransactions {
            database: self.name.to_string(),
            starting_id,
            result_limit,
        })?)
    }

    fn last_transaction_id(&self) -> Result<Option<u64>, bonsaidb_core::Error> {
        Ok(self.client.send_api_request(&LastTransactionId {
            database: self.name.to_string(),
        })?)
    }

    fn compact(&self) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request(&Compact {
            database: self.name.to_string(),
        })?;
        Ok(())
    }

    fn compact_key_value_store(&self) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request(&CompactKeyValueStore {
            database: self.name.to_string(),
        })?;
        Ok(())
    }
}

impl LowLevelConnection for RemoteDatabase {
    fn apply_transaction(
        &self,
        transaction: bonsaidb_core::transaction::Transaction,
    ) -> Result<Vec<bonsaidb_core::transaction::OperationResult>, bonsaidb_core::Error> {
        Ok(self.client.send_api_request(&ApplyTransaction {
            database: self.name.to_string(),
            transaction,
        })?)
    }

    fn get_from_collection(
        &self,
        id: bonsaidb_core::document::DocumentId,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error> {
        Ok(self.client.send_api_request(&Get {
            database: self.name.to_string(),
            collection: collection.clone(),
            id,
        })?)
    }

    fn get_multiple_from_collection(
        &self,
        ids: &[bonsaidb_core::document::DocumentId],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        Ok(self.client.send_api_request(&GetMultiple {
            database: self.name.to_string(),
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
        Ok(self.client.send_api_request(&List {
            database: self.name.to_string(),
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
        Ok(self.client.send_api_request(&ListHeaders(List {
            database: self.name.to_string(),
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
        Ok(self.client.send_api_request(&Count {
            database: self.name.to_string(),
            collection: collection.clone(),
            ids,
        })?)
    }

    fn compact_collection_by_name(
        &self,
        collection: CollectionName,
    ) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request(&CompactCollection {
            database: self.name.to_string(),
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
        Ok(self.client.send_api_request(&Query {
            database: self.name.to_string(),
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
        Ok(self.client.send_api_request(&QueryWithDocs(Query {
            database: self.name.to_string(),
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
            .client
            .send_api_request(&Reduce {
                database: self.name.to_string(),
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
        Ok(self.client.send_api_request(&ReduceGrouped(Reduce {
            database: self.name.to_string(),
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
        Ok(self.client.send_api_request(&DeleteDocs {
            database: self.name.to_string(),
            view: view.clone(),
            key,
            access_policy,
        })?)
    }
}

impl PubSub for RemoteDatabase {
    type Subscriber = RemoteSubscriber;

    fn create_subscriber(&self) -> Result<Self::Subscriber, bonsaidb_core::Error> {
        let subscriber_id = self.client.send_api_request(&CreateSubscriber {
            database: self.name.to_string(),
        })?;

        let (sender, receiver) = flume::unbounded();
        self.client.register_subscriber(subscriber_id, sender);
        Ok(RemoteSubscriber {
            client: self.client.clone(),
            database: self.name.clone(),
            id: subscriber_id,
            receiver: Receiver::new(receiver),
            tokio: None,
        })
    }

    fn publish_bytes(&self, topic: Vec<u8>, payload: Vec<u8>) -> Result<(), bonsaidb_core::Error> {
        self.client.send_api_request(&Publish {
            database: self.name.to_string(),
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
        self.client.send_api_request(&PublishToAll {
            database: self.name.to_string(),
            topics,
            payload: Bytes::from(payload),
        })?;
        Ok(())
    }
}

impl Subscriber for RemoteSubscriber {
    fn subscribe_to_bytes(&self, topic: Vec<u8>) -> Result<(), bonsaidb_core::Error> {
        self.client.send_api_request(&SubscribeTo {
            database: self.database.to_string(),
            subscriber_id: self.id,
            topic: Bytes::from(topic),
        })?;
        Ok(())
    }

    fn unsubscribe_from_bytes(&self, topic: &[u8]) -> Result<(), bonsaidb_core::Error> {
        self.client.send_api_request(&UnsubscribeFrom {
            database: self.database.to_string(),
            subscriber_id: self.id,
            topic: Bytes::from(topic),
        })?;
        Ok(())
    }

    fn receiver(&self) -> &Receiver {
        AsyncSubscriber::receiver(self)
    }
}

impl KeyValue for RemoteDatabase {
    fn execute_key_operation(
        &self,
        op: bonsaidb_core::keyvalue::KeyOperation,
    ) -> Result<bonsaidb_core::keyvalue::Output, bonsaidb_core::Error> {
        Ok(self.client.send_api_request(&ExecuteKeyOperation {
            database: self.name.to_string(),

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
