use std::{borrow::Cow, marker::PhantomData, ops::Range};

use async_trait::async_trait;
use custodian_password::{
    ClientConfig, ClientFile, ClientRegistration, ExportKey, RegistrationFinalization,
    RegistrationRequest, RegistrationResponse,
};
use serde::{Deserialize, Serialize};

use crate::{
    document::{Document, Header},
    password_config,
    schema::{
        self, view, Key, Map, MappedDocument, MappedValue, NamedReference, Schema, SchemaName,
    },
    transaction::{self, Command, Operation, OperationResult, Transaction},
    Error,
};

/// Defines all interactions with a [`schema::Schema`], regardless of whether it is local or remote.
#[async_trait]
pub trait Connection: Send + Sync {
    /// Accesses a collection for the connected [`schema::Schema`].
    fn collection<'a, C: schema::Collection + 'static>(&'a self) -> Collection<'a, Self, C>
    where
        Self: Sized,
    {
        Collection::new(self)
    }

    /// Inserts a newly created document into the connected [`schema::Schema`] for the [`Collection`] `C`.
    async fn insert<C: schema::Collection>(&self, contents: Vec<u8>) -> Result<Header, Error> {
        let mut tx = Transaction::default();
        tx.push(Operation {
            collection: C::collection_name()?,
            command: Command::Insert {
                contents: Cow::from(contents),
            },
        });
        let results = self.apply_transaction(tx).await?;
        if let OperationResult::DocumentUpdated { header, .. } = &results[0] {
            Ok(header.clone())
        } else {
            unreachable!(
                "apply_transaction on a single insert should yield a single DocumentUpdated entry"
            )
        }
    }

    /// Updates an existing document in the connected [`schema::Schema`] for the
    /// [`Collection`] `C`. Upon success, `doc.revision` will be updated with
    /// the new revision.
    async fn update<C: schema::Collection>(&self, doc: &mut Document<'_>) -> Result<(), Error> {
        let mut tx = Transaction::default();
        tx.push(Operation {
            collection: C::collection_name()?,
            command: Command::Update {
                header: Cow::Owned(doc.header.as_ref().clone()),
                contents: Cow::Owned(doc.contents.to_vec()),
            },
        });
        let results = self.apply_transaction(tx).await?;
        if let OperationResult::DocumentUpdated { header, .. } = &results[0] {
            doc.header = Cow::Owned(header.clone());
            Ok(())
        } else {
            unreachable!(
                "apply_transaction on a single update should yield a single DocumentUpdated entry"
            )
        }
    }

    /// Retrieves a stored document from [`Collection`] `C` identified by `id`.
    async fn get<C: schema::Collection>(&self, id: u64)
        -> Result<Option<Document<'static>>, Error>;

    /// Retrieves all documents matching `ids`. Documents that are not found
    /// are not returned, but no error will be generated.
    async fn get_multiple<C: schema::Collection>(
        &self,
        ids: &[u64],
    ) -> Result<Vec<Document<'static>>, Error>;

    /// Removes a `Document` from the database.
    async fn delete<C: schema::Collection>(&self, doc: &Document<'_>) -> Result<(), Error> {
        let mut tx = Transaction::default();
        tx.push(Operation {
            collection: C::collection_name()?,
            command: Command::Delete {
                header: Cow::Owned(doc.header.as_ref().clone()),
            },
        });
        let results = self.apply_transaction(tx).await?;
        if let OperationResult::DocumentDeleted { .. } = &results[0] {
            Ok(())
        } else {
            unreachable!(
                "apply_transaction on a single update should yield a single DocumentUpdated entry"
            )
        }
    }

    /// Initializes [`View`] for [`schema::View`] `V`.
    #[must_use]
    fn view<V: schema::View>(&'_ self) -> View<'_, Self, V>
    where
        Self: Sized,
    {
        View::new(self)
    }

    /// Queries for view entries matching [`View`].
    #[must_use]
    async fn query<V: schema::View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<Map<V::Key, V::Value>>, Error>
    where
        Self: Sized;

    /// Queries for view entries matching [`View`].
    #[must_use]
    async fn query_with_docs<V: schema::View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedDocument<V::Key, V::Value>>, Error>
    where
        Self: Sized;

    /// Reduces the view entries matching [`View`].
    #[must_use]
    async fn reduce<V: schema::View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<V::Value, Error>
    where
        Self: Sized;

    /// Reduces the view entries matching [`View`], reducing the values by each
    /// unique key.
    #[must_use]
    async fn reduce_grouped<V: schema::View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<V::Key, V::Value>>, Error>
    where
        Self: Sized;

    /// Applies a [`Transaction`] to the [`schema::Schema`]. If any operation in the
    /// [`Transaction`] fails, none of the operations will be applied to the
    /// [`schema::Schema`].
    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<OperationResult>, Error>;

    /// Lists executed [`Transaction`]s from this [`schema::Schema`]. By default, a maximum of
    /// 1000 entries will be returned, but that limit can be overridden by
    /// setting `result_limit`. A hard limit of 100,000 results will be
    /// returned. To begin listing after another known `transaction_id`, pass
    /// `transaction_id + 1` into `starting_id`.
    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<transaction::Executed<'static>>, Error>;

    /// Fetches the last transaction id that has been committed, if any.
    async fn last_transaction_id(&self) -> Result<Option<u64>, Error>;
}

/// Interacts with a collection over a `Connection`.
pub struct Collection<'a, Cn, Cl> {
    connection: &'a Cn,
    _phantom: PhantomData<Cl>, // allows for extension traits to be written for collections of specific types
}

impl<'a, Cn, Cl> Collection<'a, Cn, Cl>
where
    Cn: Connection,
    Cl: schema::Collection,
{
    /// Creates a new instance using `connection`.
    pub fn new(connection: &'a Cn) -> Self {
        Self {
            connection,
            _phantom: PhantomData::default(),
        }
    }

    /// Adds a new `Document<Cl>` with the contents `item`.
    pub async fn push<S: Serialize + Sync>(&self, item: &S) -> Result<Header, crate::Error> {
        let contents = serde_cbor::to_vec(item)?;
        Ok(self.connection.insert::<Cl>(contents).await?)
    }

    /// Retrieves a `Document<Cl>` with `id` from the connection.
    pub async fn get(&self, id: u64) -> Result<Option<Document<'static>>, Error> {
        self.connection.get::<Cl>(id).await
    }
}

/// Parameters to query a `schema::View`.
pub struct View<'a, Cn, V: schema::View> {
    connection: &'a Cn,

    /// Key filtering criteria.
    pub key: Option<QueryKey<V::Key>>,

    /// The view's data access policy. The default value is [`AccessPolicy::UpdateBefore`].
    pub access_policy: AccessPolicy,
}

impl<'a, Cn, V> View<'a, Cn, V>
where
    V: schema::View,
    Cn: Connection,
{
    fn new(connection: &'a Cn) -> Self {
        Self {
            connection,
            key: None,
            access_policy: AccessPolicy::UpdateBefore,
        }
    }

    /// Filters for entries in the view with `key`.
    #[must_use]
    pub fn with_key(mut self, key: V::Key) -> Self {
        self.key = Some(QueryKey::Matches(key));
        self
    }

    /// Filters for entries in the view with `keys`.
    #[must_use]
    pub fn with_keys(mut self, keys: Vec<V::Key>) -> Self {
        self.key = Some(QueryKey::Multiple(keys));
        self
    }

    /// Filters for entries in the view with the range `keys`.
    #[must_use]
    pub fn with_key_range(mut self, range: Range<V::Key>) -> Self {
        self.key = Some(QueryKey::Range(range));
        self
    }

    /// Sets the access policy for queries.
    pub fn with_access_policy(mut self, policy: AccessPolicy) -> Self {
        self.access_policy = policy;
        self
    }

    /// Executes the query and retrieves the results.
    pub async fn query(self) -> Result<Vec<Map<V::Key, V::Value>>, Error> {
        self.connection
            .query::<V>(self.key, self.access_policy)
            .await
    }

    /// Executes the query and retrieves the results with the associated `Document`s.
    pub async fn query_with_docs(self) -> Result<Vec<MappedDocument<V::Key, V::Value>>, Error> {
        self.connection
            .query_with_docs::<V>(self.key, self.access_policy)
            .await
    }

    /// Executes a reduce over the results of the query
    pub async fn reduce(self) -> Result<V::Value, Error> {
        self.connection
            .reduce::<V>(self.key, self.access_policy)
            .await
    }

    /// Executes a reduce over the results of the query
    pub async fn reduce_grouped(self) -> Result<Vec<MappedValue<V::Key, V::Value>>, Error> {
        self.connection
            .reduce_grouped::<V>(self.key, self.access_policy)
            .await
    }
}

/// Filters a [`View`] by key.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum QueryKey<K> {
    /// Matches all entries with the key provided.
    Matches(K),

    /// Matches all entires with keys in the range provided.
    Range(Range<K>),

    /// Matches all entries that have keys that are included in the set provided.
    Multiple(Vec<K>),
}

#[allow(clippy::use_self)] // clippy is wrong, Self is different because of generic parameters
impl<K: Key> QueryKey<K> {
    /// Converts this key to a serialized format using the [`Key`] trait.
    pub fn serialized(&self) -> Result<QueryKey<Vec<u8>>, Error> {
        match self {
            Self::Matches(key) => key
                .as_big_endian_bytes()
                .map_err(|err| Error::Database(view::Error::key_serialization(err).to_string()))
                .map(|v| QueryKey::Matches(v.to_vec())),
            Self::Range(range) => {
                let start = range
                    .start
                    .as_big_endian_bytes()
                    .map_err(|err| {
                        Error::Database(view::Error::key_serialization(err).to_string())
                    })?
                    .to_vec();
                let end = range
                    .end
                    .as_big_endian_bytes()
                    .map_err(|err| {
                        Error::Database(view::Error::key_serialization(err).to_string())
                    })?
                    .to_vec();
                Ok(QueryKey::Range(start..end))
            }
            Self::Multiple(keys) => {
                let keys = keys
                    .iter()
                    .map(|key| {
                        key.as_big_endian_bytes()
                            .map(|key| key.to_vec())
                            .map_err(|err| {
                                Error::Database(view::Error::key_serialization(err).to_string())
                            })
                    })
                    .collect::<Result<Vec<_>, Error>>()?;

                Ok(QueryKey::Multiple(keys))
            }
        }
    }
}

#[allow(clippy::use_self)] // clippy is wrong, Self is different because of generic parameters
impl QueryKey<Vec<u8>> {
    /// Deserializes the bytes into `K` via the [`Key`] trait.
    pub fn deserialized<K: Key>(&self) -> Result<QueryKey<K>, Error> {
        match self {
            Self::Matches(key) => K::from_big_endian_bytes(key)
                .map_err(|err| Error::Database(view::Error::key_serialization(err).to_string()))
                .map(QueryKey::Matches),
            Self::Range(range) => {
                let start = K::from_big_endian_bytes(&range.start).map_err(|err| {
                    Error::Database(view::Error::key_serialization(err).to_string())
                })?;
                let end = K::from_big_endian_bytes(&range.end).map_err(|err| {
                    Error::Database(view::Error::key_serialization(err).to_string())
                })?;
                Ok(QueryKey::Range(start..end))
            }
            Self::Multiple(keys) => {
                let keys = keys
                    .iter()
                    .map(|key| {
                        K::from_big_endian_bytes(key).map_err(|err| {
                            Error::Database(view::Error::key_serialization(err).to_string())
                        })
                    })
                    .collect::<Result<Vec<_>, Error>>()?;

                Ok(QueryKey::Multiple(keys))
            }
        }
    }
}

/// Changes how the view's outdated data will be treated.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum AccessPolicy {
    /// Update any changed documents before returning a response.
    UpdateBefore,

    /// Return the results, which may be out-of-date, and start an update job in
    /// the background. This pattern is useful when you want to ensure you
    /// provide consistent response times while ensuring the database is
    /// updating in the background.
    UpdateAfter,

    /// Returns the restuls, which may be out-of-date, and do not start any
    /// background jobs. This mode is useful if you're using a view as a cache
    /// and have a background process that is responsible for controlling when
    /// data is refreshed and updated. While the default `UpdateBefore`
    /// shouldn't have much overhead, this option removes all overhead related
    /// to view updating from the query.
    NoUpdate,
}

/// Functions for interacting with a multi-database `BonsaiDb` instance.
#[async_trait]
pub trait ServerConnection: Send + Sync {
    /// Creates a database named `name` with the `Schema` provided.
    ///
    /// ## Errors
    ///
    /// * [`Error::InvalidDatabaseName`]: `name` must begin with an alphanumeric
    ///   character (`[a-zA-Z0-9]`), and all remaining characters must be
    ///   alphanumeric, a period (`.`), or a hyphen (`-`).
    /// * [`Error::DatabaseNameAlreadyTaken]: `name` was already used for a
    ///   previous database name. Database names are case insensitive.
    async fn create_database<DB: Schema>(&self, name: &str) -> Result<(), crate::Error> {
        self.create_database_with_schema(name, DB::schema_name()?)
            .await
    }

    /// Creates a database named `name` using the [`SchemaName`] `schema`.
    ///
    /// ## Errors
    ///
    /// * [`Error::InvalidDatabaseName`]: `name` must begin with an alphanumeric
    ///   character (`[a-zA-Z0-9]`), and all remaining characters must be
    ///   alphanumeric, a period (`.`), or a hyphen (`-`).
    /// * [`Error::DatabaseNameAlreadyTaken]: `name` was already used for a
    ///   previous database name. Database names are case insensitive.
    async fn create_database_with_schema(
        &self,
        name: &str,
        schema: SchemaName,
    ) -> Result<(), crate::Error>;

    /// Deletes a database named `name`.
    ///
    /// ## Errors
    ///
    /// * [`Error::DatabaseNotFound`]: database `name` does not exist.
    /// * [`Error::Io)`]: an error occurred while deleting files.
    async fn delete_database(&self, name: &str) -> Result<(), crate::Error>;

    /// Lists the databases on this server.
    async fn list_databases(&self) -> Result<Vec<Database>, crate::Error>;

    /// Lists the [`SchemaName`]s on this server.
    async fn list_available_schemas(&self) -> Result<Vec<SchemaName>, crate::Error>;

    /// Creates a user.
    async fn create_user(&self, username: &str) -> Result<u64, crate::Error>;

    /// Sets a user's password using `custodian-password` to register a password using `OPAQUE-PAKE`.
    async fn set_user_password<'user, U: Into<NamedReference<'user>> + Send + Sync>(
        &self,
        user: U,
        password_request: RegistrationRequest,
    ) -> Result<RegistrationResponse, crate::Error>;

    /// Finishes setting a user's password by finishing the `OPAQUE-PAKE`
    /// registration.
    async fn finish_set_user_password<'user, U: Into<NamedReference<'user>> + Send + Sync>(
        &self,
        user: U,
        password_finalization: RegistrationFinalization,
    ) -> Result<(), crate::Error>;

    /// Sets a user's password with the provided string. The password provided
    /// will never leave the machine that is calling this function. Internally
    /// uses `set_user_password` and `finish_set_user_password` in conjunction
    /// with `custodian-password`.
    async fn set_user_password_str<'user, U: Into<NamedReference<'user>> + Send + Sync>(
        &self,
        user: U,
        password: &str,
    ) -> Result<PasswordResult, crate::Error> {
        let user = user.into();
        let (registration, request) =
            ClientRegistration::register(ClientConfig::new(password_config(), None)?, password)?;
        let response = self.set_user_password(user.clone(), request).await?;
        let (file, finalization, export_key) = registration.finish(response)?;
        self.finish_set_user_password(user, finalization).await?;
        Ok(PasswordResult { file, export_key })
    }

    /// Adds a user to a permission group.
    async fn add_permission_group_to_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), crate::Error>;

    /// Removes a user from a permission group.
    async fn remove_permission_group_from_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), crate::Error>;

    /// Adds a user to a permission group.
    async fn add_role_to_user<
        'user,
        'role,
        U: Into<NamedReference<'user>> + Send + Sync,
        R: Into<NamedReference<'role>> + Send + Sync,
    >(
        &self,
        user: U,
        role: R,
    ) -> Result<(), crate::Error>;

    /// Removes a user from a permission group.
    async fn remove_role_from_user<
        'user,
        'role,
        U: Into<NamedReference<'user>> + Send + Sync,
        R: Into<NamedReference<'role>> + Send + Sync,
    >(
        &self,
        user: U,
        role: R,
    ) -> Result<(), crate::Error>;
}

/// A database on a server.
#[derive(Clone, PartialEq, Deserialize, Serialize, Debug)]
pub struct Database {
    /// The name of the database.
    pub name: String,
    /// The schema defining the database.
    pub schema: SchemaName,
}

/// The result of logging in with a password or setting a password.
pub struct PasswordResult {
    /// A file that can be stored locally that can be used to further validate
    /// future login attempts. This does not need to be stored, but can be used
    /// to detect if the server key has been changed without our knowledge.
    pub file: ClientFile,
    /// A keypair derived from the OPAQUE-KE session. This key is
    /// deterministically derived from the key exchange with the server such
    /// that upon logging in with your password, this key will always be the
    /// same until you change your password.
    pub export_key: ExportKey,
}
