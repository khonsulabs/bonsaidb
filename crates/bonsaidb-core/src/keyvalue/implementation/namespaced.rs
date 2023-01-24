use async_trait::async_trait;

use super::{KeyOperation, KeyValue, Output};
use crate::{keyvalue::AsyncKeyValue, Error};

/// A namespaced key-value store. All operations performed with this will be
/// separate from other namespaces.
pub struct Namespaced<'a, K> {
    namespace: String,
    kv: &'a K,
}

impl<'a, K> Namespaced<'a, K> {
    pub(crate) const fn new(namespace: String, kv: &'a K) -> Self {
        Self { namespace, kv }
    }
}

#[async_trait]
impl<'a, K> KeyValue for Namespaced<'a, K>
where
    K: KeyValue,
{
    fn execute_key_operation(&self, op: KeyOperation) -> Result<Output, Error> {
        self.kv.execute_key_operation(op)
    }

    fn key_namespace(&self) -> Option<&'_ str> {
        Some(&self.namespace)
    }

    fn with_key_namespace(&'_ self, namespace: &str) -> Namespaced<'_, Self>
    where
        Self: Sized,
    {
        Namespaced {
            namespace: format!("{}\u{0}{namespace}", self.namespace),
            kv: self,
        }
    }
}

#[async_trait]
impl<'a, K> AsyncKeyValue for Namespaced<'a, K>
where
    K: AsyncKeyValue,
{
    async fn execute_key_operation(&self, op: KeyOperation) -> Result<Output, Error> {
        self.kv.execute_key_operation(op).await
    }

    fn key_namespace(&self) -> Option<&'_ str> {
        Some(&self.namespace)
    }

    fn with_key_namespace(&'_ self, namespace: &str) -> Namespaced<'_, Self>
    where
        Self: Sized,
    {
        Namespaced {
            namespace: format!("{}\u{0}{namespace}", self.namespace),
            kv: self,
        }
    }
}
