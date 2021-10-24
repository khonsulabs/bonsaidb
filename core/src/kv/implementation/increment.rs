use futures::{Future, FutureExt};

use super::{BuilderState, Command, KeyOperation, Kv, Output};
use crate::{
    kv::{IncompatibleTypeError, Numeric, Value},
    Error,
};

/// Executes [`Command::Set`] when awaited. Also offers methods to customize the
/// options for the operation.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Builder<'a, Kv, V> {
    state: BuilderState<'a, Options<'a, Kv>, Result<V, Error>>,
}

struct Options<'a, Kv> {
    kv: &'a Kv,
    namespace: Option<String>,
    key: String,
    increment: bool,
    amount: Numeric,
    saturating: bool,
}

impl<'a, K, V> Builder<'a, K, V>
where
    K: Kv,
{
    pub(crate) fn new(
        kv: &'a K,
        namespace: Option<String>,
        increment: bool,
        key: String,
        amount: Numeric,
    ) -> Self {
        Self {
            state: BuilderState::Pending(Some(Options {
                key,
                kv,
                namespace,
                increment,
                amount,
                saturating: true,
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

    /// Allows overflowing the value.
    pub fn allow_overflow(mut self) -> Self {
        self.options().saturating = false;
        self
    }
}

impl<'a, K, V> Future for Builder<'a, K, V>
where
    K: Kv,
    V: TryFrom<Numeric, Error = IncompatibleTypeError>,
{
    type Output = Result<V, Error>;

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
                    increment,
                    amount,
                    saturating,
                } = builder.take().expect("expected builder to have options");
                let future = async move {
                    let result = kv
                        .execute_key_operation(KeyOperation {
                            namespace,
                            key,
                            command: if increment {
                                Command::Increment { amount, saturating }
                            } else {
                                Command::Decrement { amount, saturating }
                            },
                        })
                        .await?;
                    if let Output::Value(Some(Value::Numeric(value))) = result {
                        Ok(V::try_from(value).expect("server should send back identical type"))
                    } else {
                        unreachable!("Unexpected result from key value operation")
                    }
                }
                .boxed();

                self.state = BuilderState::Executing(future);
                self.poll(cx)
            }
        }
    }
}
