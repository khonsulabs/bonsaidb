use futures::{Future, FutureExt};
use serde::Deserialize;

use super::{BuilderState, Command, KeyOperation, KeyValue, Output};
use crate::{keyvalue::Value, Error};

/// Executes [`Command::Get`] when awaited. Also offers methods to customize the
/// options for the operation.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Builder<'a, KeyValue> {
    state: BuilderState<'a, Options<'a, KeyValue>, Result<Option<Value>, Error>>,
}

struct Options<'a, KeyValue> {
    kv: &'a KeyValue,
    namespace: Option<String>,
    key: String,
    delete: bool,
}

impl<'a, K> Builder<'a, K>
where
    K: KeyValue,
{
    pub(crate) fn new(kv: &'a K, namespace: Option<String>, key: String) -> Self {
        Self {
            state: BuilderState::Pending(Some(Options {
                key,
                kv,
                namespace,
                delete: false,
            })),
        }
    }

    fn options(&mut self) -> &mut Options<'a, K> {
        if let BuilderState::Pending(Some(options)) = &mut self.state {
            options
        } else {
            unreachable!("Attempted to use after retrieving the result")
        }
    }

    /// Delete the key after retrieving the value.
    pub fn and_delete(mut self) -> Self {
        self.options().delete = true;
        self
    }

    /// Deserializes the [`Value`] before returning. If the value is a
    /// [`Numeric`](crate::keyvalue::Numeric), an error will be returned.
    pub async fn into<V: for<'de> Deserialize<'de>>(self) -> Result<Option<V>, Error> {
        self.await?.map(|value| value.deserialize()).transpose()
    }

    /// Converts the [`Value`] to an `u64` before returning. If the value is not
    /// a [`Numeric`](crate::keyvalue::Numeric), an error will be returned. If the conversion to `u64`
    /// cannot be done without losing data, an error will be returned.
    #[allow(clippy::cast_sign_loss)]
    pub async fn into_u64(self) -> Result<Option<u64>, Error> {
        match self.await? {
            Some(value) => value.as_u64().map_or_else(
                || {
                    Err(Error::Database(String::from(
                        "value not an u64 or would lose precision when converted to an u64",
                    )))
                },
                |value| Ok(Some(value)),
            ),
            None => Ok(None),
        }
    }

    /// Converts the [`Value`] to an `i64` before returning. If the value is not
    /// a [`Numeric`](crate::keyvalue::Numeric), an error will be returned. If the conversion to `i64`
    /// cannot be done without losing data, an error will be returned.
    #[allow(clippy::cast_possible_wrap)]
    pub async fn into_i64(self) -> Result<Option<i64>, Error> {
        match self.await? {
            Some(value) => value.as_i64().map_or_else(
                || {
                    Err(Error::Database(String::from(
                        "value not an i64 or would lose precision when converted to an i64",
                    )))
                },
                |value| Ok(Some(value)),
            ),
            None => Ok(None),
        }
    }

    /// Converts the [`Value`] to an `f64` before returning. If the value is not
    /// a [`Numeric`](crate::keyvalue::Numeric), an error will be returned. If the conversion to `f64`
    /// cannot be done without losing data, an error will be returned.
    #[allow(clippy::cast_precision_loss)]
    pub async fn into_f64(self) -> Result<Option<f64>, Error> {
        match self.await? {
            Some(value) => value.as_f64().map_or_else(
                || {
                    Err(Error::Database(String::from(
                        "value not an f64 or would lose precision when converted to an f64",
                    )))
                },
                |value| Ok(Some(value)),
            ),
            None => Ok(None),
        }
    }

    /// Converts the [`Value`] to an `u64` before returning. If the value is not
    /// a [`Numeric`](crate::keyvalue::Numeric), an error will be returned. If `saturating` is true, no
    /// overflows will be allowed during conversion.
    #[allow(clippy::cast_sign_loss)]
    pub async fn into_u64_lossy(self, saturating: bool) -> Result<Option<u64>, Error> {
        match self.await? {
            Some(value) => value.as_u64_lossy(saturating).map_or_else(
                || Err(Error::Database(String::from("value not numeric"))),
                |value| Ok(Some(value)),
            ),
            None => Ok(None),
        }
    }

    /// Converts the [`Value`] to an `i64` before returning. If the value is not
    /// a [`Numeric`](crate::keyvalue::Numeric), an error will be returned. If `saturating` is true, no
    /// overflows will be allowed during conversion.
    #[allow(clippy::cast_possible_wrap)]
    pub async fn into_i64_lossy(self, saturating: bool) -> Result<Option<i64>, Error> {
        match self.await? {
            Some(value) => value.as_i64_lossy(saturating).map_or_else(
                || Err(Error::Database(String::from("value not numeric"))),
                |value| Ok(Some(value)),
            ),
            None => Ok(None),
        }
    }

    /// Converts the [`Value`] to an `f64` before returning. If the value is not
    /// a [`Numeric`](crate::keyvalue::Numeric), an error will be returned.
    #[allow(clippy::cast_precision_loss)]
    pub async fn into_f64_lossy(self) -> Result<Option<f64>, Error> {
        match self.await? {
            Some(value) => value.as_f64_lossy().map_or_else(
                || Err(Error::Database(String::from("value not numeric"))),
                |value| Ok(Some(value)),
            ),
            None => Ok(None),
        }
    }
}

impl<'a, K> Future for Builder<'a, K>
where
    K: KeyValue,
{
    type Output = Result<Option<Value>, Error>;

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
                    delete,
                } = builder.take().expect("expected builder to have options");
                let future = async move {
                    let result = kv
                        .execute_key_operation(KeyOperation {
                            namespace,
                            key,
                            command: Command::Get { delete },
                        })
                        .await?;
                    if let Output::Value(value) = result {
                        Ok(value)
                    } else {
                        unreachable!("Unexpected result from get")
                    }
                }
                .boxed();

                self.state = BuilderState::Executing(future);
                self.poll(cx)
            }
        }
    }
}
