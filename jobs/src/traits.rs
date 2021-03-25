use std::{borrow::Cow, fmt::Debug};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::task::Handle;

pub trait Service<Key>
where
    Key: Clone + std::hash::Hash + Eq + Send + Sync + Debug + 'static,
{
    fn enqueue<J: Job>(job: J) -> Handle<J::Output, Key>;
    fn lookup_or_enqueue<J>(job: J) -> Handle<<J as Job>::Output, Key>
    where
        J: Keyed<Key>;
}

#[async_trait]
pub trait Job: Debug + Send + Sync + 'static {
    type Output: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;

    async fn execute(&mut self) -> anyhow::Result<Self::Output>;
}

pub trait Keyed<Key>: Job
where
    Key: Clone + std::hash::Hash + Eq + Send + Sync + Debug + 'static,
{
    fn key(&self) -> Cow<'_, Key>;
}

#[async_trait]
pub trait Executable: Send + Sync + Debug {
    async fn execute(&mut self);
}
