use futures::{Future, FutureExt};
use serde::Deserialize;

use super::{BuilderState, Command, KeyOperation, Kv, Output};
use crate::Error;

/// Executes [`Command::Get`] when awaited. Also offers methods to customize the
/// options for the operation.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Builder<'a, Kv, V> {
    state: BuilderState<'a, Options<'a, Kv>, Result<Option<V>, Error>>,
}

struct Options<'a, Kv> {
    kv: &'a Kv,
    namespace: Option<String>,
    key: String,
    delete: bool,
}

impl<'a, K, V> Builder<'a, K, V>
where
    K: Kv,
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
            panic!("Attempted to use after retrieving the result")
        }
    }

    /// Delete the key after retrieving the value.
    pub fn and_delete(mut self) -> Self {
        self.options().delete = true;
        self
    }
}

impl<'a, K, V> Future for Builder<'a, K, V>
where
    K: Kv,
    V: for<'de> Deserialize<'de>,
{
    type Output = Result<Option<V>, Error>;

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
                        .execute(KeyOperation {
                            namespace,
                            key,
                            command: Command::Get { delete },
                        })
                        .await?;
                    if let Output::Value(value) = result {
                        Ok(value
                            .map(|v| serde_cbor::from_slice(&v))
                            .transpose()
                            .unwrap())
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
