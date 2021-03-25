use std::{marker::PhantomData, ops::Range};

use async_trait::async_trait;
use serde::Serialize;

use crate::{
    document::{Document, Header},
    schema::{self, map},
    transaction::{self, OperationResult, Transaction},
    Error,
};

/// Defines all interactions with a [`schema::Database`], regardless of whether it is local or remote.
#[async_trait]
pub trait Connection<'a>: Send + Sync {
    /// Accesses a collection for the connected [`schema::Database`].
    fn collection<C: schema::Collection + 'static>(
        &'a self,
    ) -> Result<Collection<'a, Self, C>, Error>
    where
        Self: Sized;

    /// Inserts a newly created document into the connected [`schema::Database`] for the [`Collection`] `C`.
    async fn insert<C: schema::Collection>(&self, contents: Vec<u8>) -> Result<Header, Error>;

    /// Updates an existing document in the connected [`schema::Database`] for the
    /// [`Collection`] `C`. Upon success, `doc.revision` will be updated with
    /// the new revision.
    async fn update(&self, doc: &mut Document<'_>) -> Result<(), Error>;

    /// Retrieves a stored document from [`Collection`] `C` identified by `id`.
    async fn get<C: schema::Collection>(&self, id: u64)
        -> Result<Option<Document<'static>>, Error>;

    /// Initializes [`ViewQuery`] for [`schema::View`] `V`.
    #[must_use]
    fn view<'k, V: schema::View<'k>>(&'a self) -> View<'a, 'k, Self, V>
    where
        Self: Sized,
    {
        View::new(self)
    }

    /// Initializes [`ViewQuery`] for [`schema::View`] `V`.
    #[must_use]
    async fn query<'k, V: schema::View<'k>>(
        &self,
        query: View<'a, 'k, Self, V>,
    ) -> Result<Vec<map::Serialized<'static>>, Error>
    where
        Self: Sized;

    /// Applies a [`Transaction`] to the [`schema::Database`]. If any operation in the
    /// [`Transaction`] fails, none of the operations will be applied to the
    /// [`schema::Database`].
    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<OperationResult>, Error>;

    /// Lists executed [`Transaction`]s from this [`schema::Database`]. By default, a maximum of
    /// 1000 entries will be returned, but that limit can be overridden by
    /// setting `result_limit`. A hard limit of 100,000 results will be
    /// returned. To begin listing after another known `transaction_id`, pass
    /// `transaction_id + 1` into `starting_id`.
    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<transaction::Executed<'static>>, Error>;
}

/// Interacts with a collection over a `Connection`.
pub struct Collection<'a, Cn, Cl> {
    connection: &'a Cn,
    _phantom: PhantomData<Cl>, // allows for extension traits to be written for collections of specific types
}

impl<'a, Cn, Cl> Collection<'a, Cn, Cl>
where
    Cn: Connection<'a>,
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
pub struct View<'a, 'k, Cn, V: schema::View<'k>> {
    connection: &'a Cn,
    /// Key filtering criteria.
    pub key: Option<QueryKey<V::MapKey>>,
}

impl<'a, 'k, Cn, V> View<'a, 'k, Cn, V>
where
    V: schema::View<'k>,
    Cn: Connection<'a>,
{
    fn new(connection: &'a Cn) -> Self {
        Self {
            connection,
            key: None,
        }
    }

    /// Filters for entries in the view with `key`.
    #[must_use]
    pub fn with_key(mut self, key: V::MapKey) -> Self {
        self.key = Some(QueryKey::Matches(key));
        self
    }

    /// Executes the query and retrieves the results.
    pub async fn query(self) -> Result<Vec<map::Serialized<'static>>, Error> {
        self.connection.query(self).await
    }
}

/// Filters a [`View`] by key.
pub enum QueryKey<K> {
    /// Matches all entries with the key provided.
    Matches(K),

    /// Matches all entires with keys in the range provided.
    Range(Range<K>),

    /// Matches all entries that have keys that are included in the set provided.
    Multiple(Vec<K>),
}
