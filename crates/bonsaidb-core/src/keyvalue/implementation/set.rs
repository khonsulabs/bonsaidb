use std::{
    ops::Add,
    time::{Duration, SystemTime},
};

use futures::{Future, FutureExt};
use serde::{Deserialize, Serialize};

use super::{
    BuilderState, Command, KeyCheck, KeyOperation, KeyStatus, KeyValue, Output, PendingValue,
    Timestamp,
};
use crate::{keyvalue::Value, Error};

/// Executes [`Command::Set`] when awaited. Also offers methods to customize the
/// options for the operation.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Builder<'a, KeyValue, V> {
    state: BuilderState<'a, Options<'a, KeyValue, V>, Result<KeyStatus, Error>>,
}

struct Options<'a, KeyValue, V> {
    kv: &'a KeyValue,
    namespace: Option<String>,
    key: String,
    value: PendingValue<'a, V>,
    expiration: Option<Timestamp>,
    keep_existing_expiration: bool,
    check: Option<KeyCheck>,
}

impl<'a, K, V> Builder<'a, K, V>
where
    K: KeyValue,
    V: Serialize + Send + Sync,
{
    pub(crate) fn new(
        kv: &'a K,
        namespace: Option<String>,
        key: String,
        value: PendingValue<'a, V>,
    ) -> Self {
        Self {
            state: BuilderState::Pending(Some(Options {
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

    fn options(&mut self) -> &mut Options<'a, K, V> {
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

    /// Executes the Set operation, requesting the previous value be returned.
    /// If no change is made, None will be returned.
    #[allow(clippy::missing_panics_doc)]
    pub async fn returning_previous(self) -> Result<Option<Value>, Error> {
        if let BuilderState::Pending(Some(builder)) = self.state {
            let Options {
                kv,
                namespace,
                key,
                value,
                expiration,
                keep_existing_expiration,
                check,
            } = builder;

            let result = kv
                .execute_key_operation(KeyOperation {
                    namespace,
                    key,
                    command: Command::Set {
                        value: value.prepare()?,
                        expiration,
                        keep_existing_expiration,
                        check,
                        return_previous_value: true,
                    },
                })
                .await?;
            match result {
                Output::Value(value) => Ok(value),
                Output::Status(KeyStatus::NotChanged) => Ok(None),
                Output::Status(_) => unreachable!("Unexpected output from Set"),
            }
        } else {
            panic!("Using future after it's been executed")
        }
    }

    /// Executes the Set operation, requesting the previous value be returned.
    /// If no change is made, None will be returned.
    #[allow(clippy::missing_panics_doc)]
    pub async fn returning_previous_as<OtherV: for<'de> Deserialize<'de>>(
        self,
    ) -> Result<Option<OtherV>, Error> {
        self.returning_previous()
            .await?
            .map(|value| value.deserialize())
            .transpose()
    }
}

impl<'a, K, V> Future for Builder<'a, K, V>
where
    K: KeyValue,
    V: Serialize + Send + Sync,
{
    type Output = Result<KeyStatus, Error>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match &mut self.state {
            BuilderState::Executing(future) => future.as_mut().poll(cx),
            BuilderState::Pending(builder) => {
                let Options {
                    kv,
                    namespace,
                    key,
                    value,
                    expiration,
                    keep_existing_expiration,
                    check,
                } = builder.take().expect("expected builder to have options");
                let future = async move {
                    let result = kv
                        .execute_key_operation(KeyOperation {
                            namespace,
                            key,
                            command: Command::Set {
                                value: value.prepare()?,
                                expiration,
                                keep_existing_expiration,
                                check,
                                return_previous_value: false,
                            },
                        })
                        .await?;
                    if let Output::Status(status) = result {
                        Ok(status)
                    } else {
                        unreachable!("Unexpected output from Set")
                    }
                }
                .boxed();

                self.state = BuilderState::Executing(future);
                self.poll(cx)
            }
        }
    }
}
