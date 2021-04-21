use std::{
    borrow::Cow,
    ops::Add,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::{future::BoxFuture, Future};
use serde::{Deserialize, Serialize};

use crate::{schema::Key, Error};

/// A timestamp relative to [`UNIX_EPOCH`].
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub struct Timestamp {
    /// The number of whole seconds since [`UNIX_EPOCH`].
    pub seconds: u64,
    /// The number of nanoseconds in the timestamp.
    pub nanos: u32,
}

impl Timestamp {
    /// Returns the current timestamp according to the OS. Uses [`SystemTime::now()`].
    #[must_use]
    pub fn now() -> Self {
        Self::from(SystemTime::now())
    }
}

impl From<SystemTime> for Timestamp {
    fn from(time: SystemTime) -> Self {
        let duration_since_epoch = time
            .duration_since(UNIX_EPOCH)
            .expect("unrealistic system time");
        Self {
            seconds: duration_since_epoch.as_secs(),
            nanos: duration_since_epoch.subsec_nanos(),
        }
    }
}

impl From<Timestamp> for Duration {
    fn from(t: Timestamp) -> Self {
        Self::new(t.seconds, t.nanos)
    }
}

impl std::ops::Sub for Timestamp {
    type Output = Option<Duration>;

    fn sub(self, rhs: Self) -> Self::Output {
        Duration::from(self).checked_sub(Duration::from(rhs))
    }
}

impl std::ops::Add<Duration> for Timestamp {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        let mut nanos = self.nanos + rhs.subsec_nanos();
        let mut seconds = self.seconds + rhs.as_secs();
        while nanos > 1_000_000_000 {
            nanos -= 1_000_000_000;
            seconds += 1;
        }
        Self { seconds, nanos }
    }
}

impl Key for Timestamp {
    fn as_big_endian_bytes(&self) -> anyhow::Result<std::borrow::Cow<'_, [u8]>> {
        let seconds_bytes: &[u8] = &self.seconds.to_be_bytes();
        let nanos_bytes = &self.nanos.to_be_bytes();
        Ok(Cow::Owned([seconds_bytes, nanos_bytes].concat()))
    }

    fn from_big_endian_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() != 12 {
            anyhow::bail!("invalid length of stored bytes for Timestamp");
        }

        Ok(Self {
            seconds: u64::from_big_endian_bytes(&bytes[0..8])?,
            nanos: u32::from_big_endian_bytes(&bytes[8..12])?,
        })
    }
}

/// Key-Value store methods. The Key-Value store is designed to be a
/// high-performance, lightweight storage mechanism.
///
/// When compared to Collections, the Key-Value store does not offer
/// ACID-compliant transactions. Instead, the Key-Value store is made more
/// efficient by periodically flushing the store to disk rather than during each
/// operation. As such, the Key-Value store is intended to be used as a
/// lightweight caching layer. However, because each of the operations it
/// supports are executed atomically, the Key-Value store can also be utilized
/// for synchronized locking.
#[async_trait]
pub trait Kv: Send + Sync {
    /// Executes a single [`Op`].
    async fn execute(&self, op: Op) -> Result<Output, Error>;

    /// Sets `key` to `value`. This function returns a builder that is also a
    /// Future. Awaiting the builder will execute [`Op::Set`] with the options
    /// given.
    fn set<'a, S: Into<String>, V: Serialize>(
        &'a self,
        key: S,
        value: &'a V,
    ) -> SetBuilder<'a, Self, V>
    where
        Self: Sized,
    {
        SetBuilder::new(self, self.namespace().map(Into::into), key.into(), value)
    }

    /// The current namespace.
    fn namespace(&self) -> Option<&'_ str> {
        None
    }

    /// Access this Key-Value store within a namespace. When using the returned
    /// [`Namespaced`] instance, all keys specified will be separated into their
    /// own storage designated by `namespace`.
    fn with_namespace(&'_ self, namespace: &str) -> Namespaced<'_, Self>
    where
        Self: Sized,
    {
        Namespaced {
            namespace: namespace.to_string(),
            kv: self,
        }
    }
}

/// A namespaced key-value store. All operations performed with this will be
/// separate from other namespaces.
pub struct Namespaced<'a, K> {
    namespace: String,
    kv: &'a K,
}

#[async_trait]
impl<'a, K> Kv for Namespaced<'a, K>
where
    K: Kv,
{
    async fn execute(&self, op: Op) -> Result<Output, Error> {
        self.kv.execute(op).await
    }

    fn namespace(&self) -> Option<&'_ str> {
        Some(&self.namespace)
    }

    fn with_namespace(&'_ self, namespace: &str) -> Namespaced<'_, Self>
    where
        Self: Sized,
    {
        Namespaced {
            namespace: format!("{}\u{0}{}", self.namespace, namespace),
            kv: self,
        }
    }
}

/// Executes [`Op::Set`] when awaited. Also offers methods to customize the
/// options for the operation.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct SetBuilder<'a, Kv, V> {
    state: BuilderState<'a, SetBuilderOptions<'a, Kv, V>, Result<Output, Error>>,
}

struct SetBuilderOptions<'a, Kv, V> {
    kv: &'a Kv,
    namespace: Option<String>,
    key: String,
    value: &'a V,
    expiration: Option<Timestamp>,
    keep_existing_expiration: bool,
    check: Option<KeyCheck>,
}

impl<'a, K, V> SetBuilder<'a, K, V>
where
    K: Kv,
{
    fn new(kv: &'a K, namespace: Option<String>, key: String, value: &'a V) -> Self {
        Self {
            state: BuilderState::Pending(Some(SetBuilderOptions {
                key,
                value,
                kv,
                namespace,
                expiration: None,
                keep_existing_expiration: false,
                check: None,
            })),
        }
    }

    fn options(&mut self) -> &mut SetBuilderOptions<'a, K, V> {
        if let BuilderState::Pending(Some(options)) = &mut self.state {
            options
        } else {
            panic!("Attempted to use after retrieving the result")
        }
    }

    /// Set this key to expire after `duration` from now.
    pub fn expire_in(mut self, duration: Duration) -> Self {
        // TODO consider using checked_add here and making it return an error.
        self.options().expiration = Some(Timestamp::from(SystemTime::now().add(duration)));
        self
    }

    /// Set this key to expire at the provided `time`.
    pub fn expire_at(mut self, time: SystemTime) -> Self {
        // TODO consider using checked_add here and making it return an error.
        self.options().expiration = Some(Timestamp::from(time));
        self
    }

    /// If the key already exists, do not update the currently set expiration.
    pub fn keep_existing_expiration(mut self) -> Self {
        self.options().keep_existing_expiration = true;
        self
    }

    /// Only set the value if this key already exists.
    pub fn only_if_exists(mut self) -> Self {
        self.options().check = Some(KeyCheck::OnlyIfPresent);
        self
    }

    /// Only set the value if this key isn't present.
    pub fn only_if_vacant(mut self) -> Self {
        self.options().check = Some(KeyCheck::OnlyIfVacant);
        self
    }
}

enum BuilderState<'a, T, R> {
    Pending(Option<T>),
    Executing(BoxFuture<'a, R>),
}

impl<'a, K, V> Future for SetBuilder<'a, K, V>
where
    K: Kv,
    V: Serialize,
{
    type Output = Result<Output, Error>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match &mut self.state {
            BuilderState::Executing(future) => future.as_mut().poll(cx),
            BuilderState::Pending(builder) => {
                let SetBuilderOptions {
                    kv,
                    namespace,
                    key,
                    value,
                    expiration,
                    keep_existing_expiration,
                    check,
                } = builder.take().expect("expected builder to have options");
                let future = kv.execute(Op::Set {
                    namespace,
                    key,
                    value: serde_cbor::to_vec(value)?,
                    expiration,
                    keep_existing_expiration,
                    check,
                });

                self.state = BuilderState::Executing(future);
                self.poll(cx)
            }
        }
    }
}

/// Checks for existing keys.
#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub enum KeyCheck {
    /// Only allow the operation if an existing key is present.
    OnlyIfPresent,
    /// Only allow the opeartion if the key isn't present.
    OnlyIfVacant,
}

/// Operations for a key-value store.
#[derive(Serialize, Deserialize, Debug)]
pub enum Op {
    /// Set a key/value pair.
    Set {
        /// The namespace for the key.
        namespace: Option<String>,
        /// The key.
        key: String,
        /// The value.
        value: Vec<u8>,
        /// If set, the key will be set to expire automatically.
        expiration: Option<Timestamp>,
        /// If true and the key already exists, the expiration will not be
        /// updated. If false and an expiration is provided, the expiration will
        /// be set.
        keep_existing_expiration: bool,
        /// Conditional checks for whether the key is already present or not.
        check: Option<KeyCheck>,
    },
    /// Delete a key.
    Delete {
        /// The namespace for the key.
        namespace: Option<String>,
        /// The key to remove.
        key: String,
    },
}

/// The result of an [`Op`].
#[derive(Debug)]
pub enum Output {
    /// The operation succeeded.
    Ok,
    /// The key was not modified.
    KeyNotChanged,
    /// A value was returned.
    Value(Option<Vec<u8>>),
}
