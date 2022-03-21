use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    async_trait::async_trait,
    connection::{AsyncConnection, AsyncLowLevelConnection, AsyncStorageConnection},
    keyvalue::AsyncKeyValue,
    networking::{
        AlterUserPermissionGroupMembership, AlterUserRoleMembership, ApplyTransaction,
        AssumeIdentity, Authenticate, Compact, CompactCollection, CompactKeyValueStore, Count,
        CreateDatabase, CreateSubscriber, CreateUser, DeleteDatabase, DeleteDocs, DeleteUser,
        ExecuteKeyOperation, Get, GetMultiple, LastTransactionId, List, ListAvailableSchemas,
        ListDatabases, ListExecutedTransactions, Publish, PublishToAll, Query, QueryWithDocs,
        Reduce, ReduceGrouped, SetUserPassword, SubscribeTo, UnregisterSubscriber, UnsubscribeFrom,
    },
    pubsub::AsyncPubSub,
    schema::ApiName,
};

use crate::{
    api::{CustomApiHandler, DispatchError, DispatcherResult},
    Backend, ConnectedClient, CustomServer, Error, ServerConfiguration,
};

pub fn register_api_handlers<B: Backend>(
    config: ServerConfiguration<B>,
) -> Result<ServerConfiguration<B>, Error> {
    config
        .with_api::<ServerDispatcher, AlterUserPermissionGroupMembership>()?
        .with_api::<ServerDispatcher, AlterUserRoleMembership>()?
        .with_api::<ServerDispatcher, ApplyTransaction>()?
        .with_api::<ServerDispatcher, AssumeIdentity>()?
        .with_api::<ServerDispatcher, Authenticate>()?
        .with_api::<ServerDispatcher, Compact>()?
        .with_api::<ServerDispatcher, CompactCollection>()?
        .with_api::<ServerDispatcher, CompactKeyValueStore>()?
        .with_api::<ServerDispatcher, Count>()?
        .with_api::<ServerDispatcher, CreateDatabase>()?
        .with_api::<ServerDispatcher, CreateSubscriber>()?
        .with_api::<ServerDispatcher, CreateUser>()?
        .with_api::<ServerDispatcher, DeleteDatabase>()?
        .with_api::<ServerDispatcher, DeleteDocs>()?
        .with_api::<ServerDispatcher, DeleteUser>()?
        .with_api::<ServerDispatcher, ExecuteKeyOperation>()?
        .with_api::<ServerDispatcher, Get>()?
        .with_api::<ServerDispatcher, GetMultiple>()?
        .with_api::<ServerDispatcher, LastTransactionId>()?
        .with_api::<ServerDispatcher, List>()?
        .with_api::<ServerDispatcher, ListAvailableSchemas>()?
        .with_api::<ServerDispatcher, ListDatabases>()?
        .with_api::<ServerDispatcher, ListExecutedTransactions>()?
        .with_api::<ServerDispatcher, Publish>()?
        .with_api::<ServerDispatcher, PublishToAll>()?
        .with_api::<ServerDispatcher, Query>()?
        .with_api::<ServerDispatcher, QueryWithDocs>()?
        .with_api::<ServerDispatcher, Reduce>()?
        .with_api::<ServerDispatcher, ReduceGrouped>()?
        .with_api::<ServerDispatcher, SetUserPassword>()?
        .with_api::<ServerDispatcher, SubscribeTo>()?
        .with_api::<ServerDispatcher, UnregisterSubscriber>()?
        .with_api::<ServerDispatcher, UnsubscribeFrom>()
}

#[derive(Debug)]
pub struct ServerDispatcher;
impl ServerDispatcher {
    pub async fn dispatch_api_request<B: Backend>(
        server: &CustomServer<B>,
        client: &ConnectedClient<B>,
        name: &ApiName,
        request: Bytes,
    ) -> Result<Bytes, Error> {
        if let Some(dispatcher) = server.custom_api_dispatcher(name) {
            dispatcher.handle(server, client, &request).await
        } else {
            Err(Error::from(bonsaidb_core::Error::CustomApiNotFound(
                name.clone(),
            )))
        }
    }
}

// #[async_trait]
// impl<B: Backend> CustomApiHandler<B, Database> for ServerDispatcher {
//     async fn handle(
//         &self,
//         permissions: &Permissions,
//         database_name: String,
//         request: DatabaseRequest,
//     ) -> Result<Response, Error> {
//         let storage = Storage::from(self.server.storage.clone());
//         let db = storage.database_without_schema(&database_name)?;
//         DatabaseDispatcher {
//             client: self.client,
//             database: ServerDatabase {
//                 server: self.server.clone(),
//                 db: AsyncDatabase::from(db),
//             },
//         }
//         .dispatch(permissions, request)
//         .await
//     }
// }

#[async_trait]
impl<B: Backend> CustomApiHandler<B, CreateDatabase> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        request: CreateDatabase,
    ) -> DispatcherResult<CreateDatabase> {
        server
            .create_database_with_schema(
                &request.database.name,
                request.database.schema,
                request.only_if_needed,
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, DeleteDatabase> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: DeleteDatabase,
    ) -> DispatcherResult<DeleteDatabase> {
        server.storage.delete_database(&command.name).await?;
        Ok(())
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, ListDatabases> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        _command: ListDatabases,
    ) -> DispatcherResult<ListDatabases> {
        server.list_databases().await.map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, ListAvailableSchemas> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        _command: ListAvailableSchemas,
    ) -> DispatcherResult<ListAvailableSchemas> {
        server
            .list_available_schemas()
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, CreateUser> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: CreateUser,
    ) -> DispatcherResult<CreateUser> {
        server
            .create_user(&command.username)
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, DeleteUser> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: DeleteUser,
    ) -> DispatcherResult<DeleteUser> {
        server
            .delete_user(command.user)
            .await
            .map_err(DispatchError::from)
    }
}

#[cfg(feature = "password-hashing")]
#[async_trait]
impl<B: Backend> CustomApiHandler<B, SetUserPassword> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: SetUserPassword,
    ) -> DispatcherResult<SetUserPassword> {
        server
            .set_user_password(command.user, command.password)
            .await
            .map_err(DispatchError::from)
    }
}

#[cfg(feature = "password-hashing")]
#[async_trait]
impl<B: Backend> CustomApiHandler<B, Authenticate> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        client: &ConnectedClient<B>,
        command: Authenticate,
    ) -> DispatcherResult<Authenticate> {
        let authenticated = server
            .authenticate(command.user, command.authentication)
            .await?;
        let session = authenticated.session().cloned().unwrap();

        client.logged_in_as(session.clone());

        Ok(session)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, AssumeIdentity> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        client: &ConnectedClient<B>,
        command: AssumeIdentity,
    ) -> DispatcherResult<AssumeIdentity> {
        let authenticated = server.assume_identity(command.0).await?;
        let session = authenticated.session().cloned().unwrap();

        client.logged_in_as(session.clone());

        Ok(session)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, AlterUserPermissionGroupMembership> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: AlterUserPermissionGroupMembership,
    ) -> DispatcherResult<AlterUserPermissionGroupMembership> {
        if command.should_be_member {
            server
                .add_permission_group_to_user(command.user, command.group)
                .await
                .map_err(DispatchError::from)
        } else {
            server
                .remove_permission_group_from_user(command.user, command.group)
                .await
                .map_err(DispatchError::from)
        }
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, AlterUserRoleMembership> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: AlterUserRoleMembership,
    ) -> DispatcherResult<AlterUserRoleMembership> {
        if command.should_be_member {
            server
                .add_role_to_user(command.user, command.role)
                .await
                .map_err(DispatchError::from)
        } else {
            server
                .remove_role_from_user(command.user, command.role)
                .await
                .map_err(DispatchError::from)
        }
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, Get> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: Get,
    ) -> DispatcherResult<Get> {
        let database = server.database_without_schema(&command.database).await?;
        database
            .get_from_collection(command.id, &command.collection)
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, GetMultiple> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: GetMultiple,
    ) -> DispatcherResult<GetMultiple> {
        let database = server.database_without_schema(&command.database).await?;
        database
            .get_multiple_from_collection(&command.ids, &command.collection)
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, List> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: List,
    ) -> DispatcherResult<List> {
        let database = server.database_without_schema(&command.database).await?;
        database
            .list_from_collection(
                command.ids,
                command.order,
                command.limit,
                &command.collection,
            )
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, Count> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: Count,
    ) -> DispatcherResult<Count> {
        let database = server.database_without_schema(&command.database).await?;
        database
            .count_from_collection(command.ids, &command.collection)
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, Query> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: Query,
    ) -> DispatcherResult<Query> {
        let database = server.database_without_schema(&command.database).await?;
        database
            .query_by_name(
                &command.view,
                command.key,
                command.order,
                command.limit,
                command.access_policy,
            )
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, QueryWithDocs> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: QueryWithDocs,
    ) -> DispatcherResult<QueryWithDocs> {
        let database = server.database_without_schema(&command.0.database).await?;
        database
            .query_by_name_with_docs(
                &command.0.view,
                command.0.key,
                command.0.order,
                command.0.limit,
                command.0.access_policy,
            )
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, Reduce> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: Reduce,
    ) -> DispatcherResult<Reduce> {
        let database = server.database_without_schema(&command.database).await?;
        database
            .reduce_by_name(&command.view, command.key, command.access_policy)
            .await
            .map(Bytes::from)
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, ReduceGrouped> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: ReduceGrouped,
    ) -> DispatcherResult<ReduceGrouped> {
        let database = server.database_without_schema(&command.0.database).await?;
        database
            .reduce_grouped_by_name(&command.0.view, command.0.key, command.0.access_policy)
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, ApplyTransaction> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: ApplyTransaction,
    ) -> DispatcherResult<ApplyTransaction> {
        let database = server.database_without_schema(&command.database).await?;
        database
            .apply_transaction(command.transaction)
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, DeleteDocs> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: DeleteDocs,
    ) -> DispatcherResult<DeleteDocs> {
        let database = server.database_without_schema(&command.database).await?;
        database
            .delete_docs_by_name(&command.view, command.key, command.access_policy)
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, ListExecutedTransactions> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: ListExecutedTransactions,
    ) -> DispatcherResult<ListExecutedTransactions> {
        let database = server.database_without_schema(&command.database).await?;
        database
            .list_executed_transactions(command.starting_id, command.result_limit)
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, LastTransactionId> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: LastTransactionId,
    ) -> DispatcherResult<LastTransactionId> {
        let database = server.database_without_schema(&command.database).await?;
        database
            .last_transaction_id()
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, CreateSubscriber> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        client: &ConnectedClient<B>,
        command: CreateSubscriber,
    ) -> DispatcherResult<CreateSubscriber> {
        let database = server.database_without_schema(&command.database).await?;
        let subscriber = database.create_subscriber().await?;
        let subscriber_id = subscriber.id();

        client.register_subscriber(subscriber, server.session().and_then(|session| session.id));

        Ok(subscriber_id)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, Publish> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: Publish,
    ) -> DispatcherResult<Publish> {
        let database = server.database_without_schema(&command.database).await?;
        database
            .publish_bytes(&command.topic, command.payload.into_vec())
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, PublishToAll> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: PublishToAll,
    ) -> DispatcherResult<PublishToAll> {
        let database = server.database_without_schema(&command.database).await?;
        database
            .publish_bytes_to_all(command.topics, command.payload.into_vec())
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, SubscribeTo> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        client: &ConnectedClient<B>,
        command: SubscribeTo,
    ) -> DispatcherResult<SubscribeTo> {
        client
            .subscribe_by_id(
                command.subscriber_id,
                command.topic,
                server.session().and_then(|session| session.id),
            )
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, UnsubscribeFrom> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        client: &ConnectedClient<B>,
        command: UnsubscribeFrom,
    ) -> DispatcherResult<UnsubscribeFrom> {
        client
            .unsubscribe_by_id(
                command.subscriber_id,
                &command.topic,
                server.session().and_then(|session| session.id),
            )
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, UnregisterSubscriber> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        client: &ConnectedClient<B>,
        command: UnregisterSubscriber,
    ) -> DispatcherResult<UnregisterSubscriber> {
        client
            .unregister_subscriber_by_id(
                command.subscriber_id,
                server.session().and_then(|session| session.id),
            )
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, ExecuteKeyOperation> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: ExecuteKeyOperation,
    ) -> DispatcherResult<ExecuteKeyOperation> {
        let database = server.database_without_schema(&command.database).await?;
        database
            .execute_key_operation(command.op)
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, CompactCollection> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: CompactCollection,
    ) -> DispatcherResult<CompactCollection> {
        let database = server.database_without_schema(&command.database).await?;
        database
            .compact_collection_by_name(command.name)
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, CompactKeyValueStore> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: CompactKeyValueStore,
    ) -> DispatcherResult<CompactKeyValueStore> {
        let database = server.database_without_schema(&command.database).await?;
        database
            .compact_key_value_store()
            .await
            .map_err(DispatchError::from)
    }
}

#[async_trait]
impl<B: Backend> CustomApiHandler<B, Compact> for ServerDispatcher {
    async fn handle(
        server: &CustomServer<B>,
        _client: &ConnectedClient<B>,
        command: Compact,
    ) -> DispatcherResult<Compact> {
        let database = server.database_without_schema(&command.database).await?;
        database.compact().await.map_err(DispatchError::from)
    }
}
