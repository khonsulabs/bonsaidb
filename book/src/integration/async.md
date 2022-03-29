# Async vs Blocking

BonsaiDb supports both async and blocking (threaded) access. Its aim is to
provide a first-class experience no matter which architecture you choose for
your Rust application.

## Local-only

[`Storage`][storage] and [`Database`][database] are the blocking implementations
of BonsaiDb. These types provide the lowest overhead access to BonsaiDb as they
will block the currently executing thread to perform the operations.

[`AsyncStorage`][async-storage] and [`AsyncDatabase`][async-database] are simple
types that "wrap" [`Storage`][storage] and [`Database`][database] instances with
an asynchronous API. BonsaiDb does this by spawning a blocking task in Tokio.
Internally, Tokio uses a pool of threads to drive blocking operations. This may
sound like a lot of overhead, but it is surprisingly lightweight.

Our recommendation is to pick the programming style that fits your needs the
best. Do you need lightweight task concurrency, or is basic threading enough? If
this application grew in scope, would it ever need to be a networked
application?

If you anticipate needing to use BonsaiDb's networked server, you should review
the next section to consider how Tokio benefits a networked server.

## Networked Server

When building a networked server, a common strategy to handle inbound
connections is to allow each connection to have a thread. This is expensive,
however, as each thread needs its own stack allocated and is managed by the
kernel. When designing a server with long-running connections, async allows
handling more connections with fewer system resources. As such, BonsaiDb's
server is built atop Tokio, and the traits used to extend the server are
[`async_trait`][async_trait]s.

The networked server is built atop [`AsyncStorage`][async-storage], which means
that you can convert a server instance into a blocking [`Storage`][storage]
instance, allowing local access to your server to remain blocking.

## Networked Client

BonsaiDb's networked client uses Tokio for all networking on non-WASM targets,
and uses the browser's WebSocket APIs for WASM targets.

On all non-WASM targets, the networked client can be used without a Tokio
runtime present. When instantiated this way, a runtime will automatically be run
powering the client's networking. In the future, it is possible that
non-Tokio-based networking implementations could be provided instead for the
blocking client implementation.

For WASM, the networked client does not provide blocking trait implementations.
If you are building for WASM, you must use the async traits.

## The differences between the APIs

The core traits are split into two types: blocking and async.
  
    | Blocking             |   Async                   |
    |----------------------|---------------------------|
    | `Connection`         | `AsyncConnection`         |
    | `StorageConnection`  | `AsyncStorageConnection`  |
    | `PubSub`             | `AsyncPubSub`             |
    | `Subscriber`         | `AsyncSubscriber`         |
    | `KeyValue`           | `AsyncKeyValue`           |
    | `LowLevelConnection` | `AsyncLowLevelConnection` |

By splitting these traits, BonsaiDb tries to make it harder to accidentally use
a blocking API in an asynchronous context. In general, all other functions are
exposed in pairs: a blocking version, and an async version with the suffix
"_async". For example, `SerializedCollection::get` is the blocking API, and
`SerializedCollection::get_async` is the async API.

When developing a project that uses both async and blocking modes of access, it
is considered a good practice to separate modules based on whether they are
blocking or not. This can help spot mistakes when the wrong type of trait is
imported in the wrong type of module.

[storage]: https://dev.bonsaidb.io/main/docs/bonsaidb_local/struct.Storage.html
[async-storage]:
    https://dev.bonsaidb.io/main/docs/bonsaidb_local/struct.AsyncStorage.html
[database]:
    https://dev.bonsaidb.io/main/docs/bonsaidb_local/struct.Database.html
[async-database]:
    https://dev.bonsaidb.io/main/docs/bonsaidb_local/struct.AsyncDatabase.html
[async_trait]: https://crates.io/crates/async_trait
