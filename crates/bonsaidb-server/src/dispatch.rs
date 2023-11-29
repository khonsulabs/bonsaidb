use bonsaidb_core::api::ApiName;
use bonsaidb_core::arc_bytes::serde::Bytes;
use bonsaidb_core::async_trait::async_trait;
use bonsaidb_core::connection::{
    AsyncConnection, AsyncLowLevelConnection, AsyncStorageConnection, HasSession,
};
use bonsaidb_core::keyvalue::AsyncKeyValue;
use bonsaidb_core::networking::{
    AlterUserPermissionGroupMembership, AlterUserRoleMembership, ApplyTransaction, AssumeIdentity,
    Compact, CompactCollection, CompactKeyValueStore, Count, CreateDatabase, CreateSubscriber,
    CreateUser, DeleteDatabase, DeleteDocs, DeleteUser, ExecuteKeyOperation, Get, GetMultiple,
    LastTransactionId, List, ListAvailableSchemas, ListDatabases, ListExecutedTransactions,
    ListHeaders, LogOutSession, Publish, PublishToAll, Query, QueryWithDocs, Reduce, ReduceGrouped,
    SubscribeTo, UnregisterSubscriber, UnsubscribeFrom,
};
#[cfg(feature = "password-hashing")]
use bonsaidb_core::networking::{Authenticate, SetUserPassword};
use bonsaidb_core::pubsub::AsyncPubSub;

use crate::api::{Handler, HandlerError, HandlerResult, HandlerSession};
use crate::{Backend, Error, ServerConfiguration};

#[cfg_attr(not(feature = "password-hashing"), allow(unused_mut))]
pub fn register_api_handlers<B: Backend>(
    config: ServerConfiguration<B>,
) -> Result<ServerConfiguration<B>, Error> {
    let mut config = config
        .with_api::<ServerDispatcher, AlterUserPermissionGroupMembership>()?
        .with_api::<ServerDispatcher, AlterUserRoleMembership>()?
        .with_api::<ServerDispatcher, ApplyTransaction>()?
        .with_api::<ServerDispatcher, AssumeIdentity>()?
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
        .with_api::<ServerDispatcher, ListHeaders>()?
        .with_api::<ServerDispatcher, ListAvailableSchemas>()?
        .with_api::<ServerDispatcher, ListDatabases>()?
        .with_api::<ServerDispatcher, ListExecutedTransactions>()?
        .with_api::<ServerDispatcher, LogOutSession>()?
        .with_api::<ServerDispatcher, Publish>()?
        .with_api::<ServerDispatcher, PublishToAll>()?
        .with_api::<ServerDispatcher, Query>()?
        .with_api::<ServerDispatcher, QueryWithDocs>()?
        .with_api::<ServerDispatcher, Reduce>()?
        .with_api::<ServerDispatcher, ReduceGrouped>()?
        .with_api::<ServerDispatcher, SubscribeTo>()?
        .with_api::<ServerDispatcher, UnregisterSubscriber>()?
        .with_api::<ServerDispatcher, UnsubscribeFrom>()?;

    #[cfg(feature = "password-hashing")]
    {
        config = config
            .with_api::<ServerDispatcher, Authenticate>()?
            .with_api::<ServerDispatcher, SetUserPassword>()?;
    }

    Ok(config)
}

#[derive(Debug)]
pub struct ServerDispatcher;
impl ServerDispatcher {
    pub async fn dispatch_api_request<B: Backend>(
        session: HandlerSession<'_, B>,
        name: &ApiName,
        request: Bytes,
    ) -> Result<Bytes, Error> {
        if let Some(dispatcher) = session.server.custom_api_dispatcher(name) {
            dispatcher.handle(session, &request).await
        } else {
            Err(Error::from(bonsaidb_core::Error::ApiNotFound(name.clone())))
        }
    }
}

#[async_trait]
impl<B: Backend> Handler<CreateDatabase, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        request: CreateDatabase,
    ) -> HandlerResult<CreateDatabase> {
        session
            .as_client
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
impl<B: Backend> Handler<DeleteDatabase, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: DeleteDatabase,
    ) -> HandlerResult<DeleteDatabase> {
        session.as_client.delete_database(&command.name).await?;
        Ok(())
    }
}

#[async_trait]
impl<B: Backend> Handler<ListDatabases, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        _command: ListDatabases,
    ) -> HandlerResult<ListDatabases> {
        session
            .as_client
            .list_databases()
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<ListAvailableSchemas, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        _command: ListAvailableSchemas,
    ) -> HandlerResult<ListAvailableSchemas> {
        session
            .as_client
            .list_available_schemas()
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<CreateUser, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: CreateUser,
    ) -> HandlerResult<CreateUser> {
        session
            .as_client
            .create_user(&command.username)
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<DeleteUser, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: DeleteUser,
    ) -> HandlerResult<DeleteUser> {
        session
            .as_client
            .delete_user(command.user)
            .await
            .map_err(HandlerError::from)
    }
}

#[cfg(feature = "password-hashing")]
#[async_trait]
impl<B: Backend> Handler<SetUserPassword, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: SetUserPassword,
    ) -> HandlerResult<SetUserPassword> {
        session
            .as_client
            .set_user_password(command.user, command.password)
            .await
            .map_err(HandlerError::from)
    }
}

#[cfg(feature = "password-hashing")]
#[async_trait]
impl<B: Backend> Handler<Authenticate, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: Authenticate,
    ) -> HandlerResult<Authenticate> {
        let authenticated = session
            .as_client
            .authenticate(command.authentication)
            .await?;
        let new_session = authenticated.session().cloned().unwrap();

        session.client.logged_in_as(new_session.clone());

        if let Err(err) = session
            .server
            .backend()
            .client_authenticated(session.client.clone(), &new_session, session.server)
            .await
        {
            log::error!("[server] Error in `client_authenticated`: {err:?}");
        }

        Ok(new_session)
    }
}

#[async_trait]
impl<B: Backend> Handler<AssumeIdentity, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: AssumeIdentity,
    ) -> HandlerResult<AssumeIdentity> {
        let authenticated = session.as_client.assume_identity(command.0).await?;
        let new_session = authenticated.session().cloned().unwrap();

        session.client.logged_in_as(new_session.clone());

        if let Err(err) = session
            .server
            .backend()
            .client_authenticated(session.client.clone(), &new_session, session.server)
            .await
        {
            log::error!("[server] Error in `client_authenticated`: {err:?}");
        }

        Ok(new_session)
    }
}

#[async_trait]
impl<B: Backend> Handler<LogOutSession, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: LogOutSession,
    ) -> HandlerResult<LogOutSession> {
        if let Some(logged_out) = session.client.log_out(command.0) {
            if let Err(err) = session
                .server
                .backend()
                .client_session_ended(logged_out, session.client, false, session.server)
                .await
            {
                log::error!("[server] Error in `client_session_ended`: {err:?}");
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<B: Backend> Handler<AlterUserPermissionGroupMembership, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: AlterUserPermissionGroupMembership,
    ) -> HandlerResult<AlterUserPermissionGroupMembership> {
        if command.should_be_member {
            session
                .as_client
                .add_permission_group_to_user(command.user, command.group)
                .await
                .map_err(HandlerError::from)
        } else {
            session
                .as_client
                .remove_permission_group_from_user(command.user, command.group)
                .await
                .map_err(HandlerError::from)
        }
    }
}

#[async_trait]
impl<B: Backend> Handler<AlterUserRoleMembership, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: AlterUserRoleMembership,
    ) -> HandlerResult<AlterUserRoleMembership> {
        if command.should_be_member {
            session
                .as_client
                .add_role_to_user(command.user, command.role)
                .await
                .map_err(HandlerError::from)
        } else {
            session
                .as_client
                .remove_role_from_user(command.user, command.role)
                .await
                .map_err(HandlerError::from)
        }
    }
}

#[async_trait]
impl<B: Backend> Handler<Get, B> for ServerDispatcher {
    async fn handle(session: HandlerSession<'_, B>, command: Get) -> HandlerResult<Get> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database
            .get_from_collection(command.id, &command.collection)
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<GetMultiple, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: GetMultiple,
    ) -> HandlerResult<GetMultiple> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database
            .get_multiple_from_collection(&command.ids, &command.collection)
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<List, B> for ServerDispatcher {
    async fn handle(session: HandlerSession<'_, B>, command: List) -> HandlerResult<List> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database
            .list_from_collection(
                command.ids,
                command.order,
                command.limit,
                &command.collection,
            )
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<ListHeaders, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: ListHeaders,
    ) -> HandlerResult<ListHeaders> {
        let database = session
            .as_client
            .database_without_schema(&command.0.database)
            .await?;
        database
            .list_headers_from_collection(
                command.0.ids,
                command.0.order,
                command.0.limit,
                &command.0.collection,
            )
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<Count, B> for ServerDispatcher {
    async fn handle(session: HandlerSession<'_, B>, command: Count) -> HandlerResult<Count> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database
            .count_from_collection(command.ids, &command.collection)
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<Query, B> for ServerDispatcher {
    async fn handle(session: HandlerSession<'_, B>, command: Query) -> HandlerResult<Query> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database
            .query_by_name(
                &command.view,
                command.key,
                command.order,
                command.limit,
                command.access_policy,
            )
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<QueryWithDocs, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: QueryWithDocs,
    ) -> HandlerResult<QueryWithDocs> {
        let database = session
            .as_client
            .database_without_schema(&command.0.database)
            .await?;
        database
            .query_by_name_with_docs(
                &command.0.view,
                command.0.key,
                command.0.order,
                command.0.limit,
                command.0.access_policy,
            )
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<Reduce, B> for ServerDispatcher {
    async fn handle(session: HandlerSession<'_, B>, command: Reduce) -> HandlerResult<Reduce> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database
            .reduce_by_name(&command.view, command.key, command.access_policy)
            .await
            .map(Bytes::from)
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<ReduceGrouped, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: ReduceGrouped,
    ) -> HandlerResult<ReduceGrouped> {
        let database = session
            .as_client
            .database_without_schema(&command.0.database)
            .await?;
        database
            .reduce_grouped_by_name(&command.0.view, command.0.key, command.0.access_policy)
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<ApplyTransaction, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: ApplyTransaction,
    ) -> HandlerResult<ApplyTransaction> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database
            .apply_transaction(command.transaction)
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<DeleteDocs, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: DeleteDocs,
    ) -> HandlerResult<DeleteDocs> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database
            .delete_docs_by_name(&command.view, command.key, command.access_policy)
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<ListExecutedTransactions, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: ListExecutedTransactions,
    ) -> HandlerResult<ListExecutedTransactions> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database
            .list_executed_transactions(command.starting_id, command.result_limit)
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<LastTransactionId, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: LastTransactionId,
    ) -> HandlerResult<LastTransactionId> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database
            .last_transaction_id()
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<CreateSubscriber, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: CreateSubscriber,
    ) -> HandlerResult<CreateSubscriber> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        let subscriber = database.create_subscriber().await?;
        let subscriber_id = subscriber.id();

        session.client.register_subscriber(
            subscriber,
            session.as_client.session().and_then(|session| session.id),
        );

        Ok(subscriber_id)
    }
}

#[async_trait]
impl<B: Backend> Handler<Publish, B> for ServerDispatcher {
    async fn handle(session: HandlerSession<'_, B>, command: Publish) -> HandlerResult<Publish> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database
            .publish_bytes(command.topic.into_vec(), command.payload.into_vec())
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<PublishToAll, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: PublishToAll,
    ) -> HandlerResult<PublishToAll> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database
            .publish_bytes_to_all(
                command.topics.into_iter().map(Bytes::into_vec),
                command.payload.into_vec(),
            )
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<SubscribeTo, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: SubscribeTo,
    ) -> HandlerResult<SubscribeTo> {
        session
            .client
            .subscribe_by_id(
                command.subscriber_id,
                command.topic,
                session.as_client.session().and_then(|session| session.id),
            )
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<UnsubscribeFrom, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: UnsubscribeFrom,
    ) -> HandlerResult<UnsubscribeFrom> {
        session
            .client
            .unsubscribe_by_id(
                command.subscriber_id,
                &command.topic,
                session.as_client.session().and_then(|session| session.id),
            )
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<UnregisterSubscriber, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: UnregisterSubscriber,
    ) -> HandlerResult<UnregisterSubscriber> {
        session
            .client
            .unregister_subscriber_by_id(
                command.subscriber_id,
                session.as_client.session().and_then(|session| session.id),
            )
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<ExecuteKeyOperation, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: ExecuteKeyOperation,
    ) -> HandlerResult<ExecuteKeyOperation> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database
            .execute_key_operation(command.op)
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<CompactCollection, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: CompactCollection,
    ) -> HandlerResult<CompactCollection> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database
            .compact_collection_by_name(command.name)
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<CompactKeyValueStore, B> for ServerDispatcher {
    async fn handle(
        session: HandlerSession<'_, B>,
        command: CompactKeyValueStore,
    ) -> HandlerResult<CompactKeyValueStore> {
        let database = session
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database
            .compact_key_value_store()
            .await
            .map_err(HandlerError::from)
    }
}

#[async_trait]
impl<B: Backend> Handler<Compact, B> for ServerDispatcher {
    async fn handle(client: HandlerSession<'_, B>, command: Compact) -> HandlerResult<Compact> {
        let database = client
            .as_client
            .database_without_schema(&command.database)
            .await?;
        database.compact().await.map_err(HandlerError::from)
    }
}
