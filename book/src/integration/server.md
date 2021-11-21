# Integrating the networked BonsaiDb Server

To access `BonsaiDb` over the network, you're going to be writing two pieces of code: the server code and the client code.

## Your BonsaiDb Server

The first step is to create a [`Server`][storage], which uses local [`Storage`][storage] under the hood. This means that if you're already using `BonsaiDb` in local mode, you can swap your usage of [`Storage`][storage] with [`Server`][server] in your server code without running your database through any tools. Here's the setup code from [`bonsaidb/examples/server.rs`](https://github.com/khonsulabs/bonsaidb/blob/main/crates/bonsaidb/examples/server.rs)

```rust,noplayground,no_run
{{#include ../../../examples/basic-server/examples/basic-server.rs:setup}}
```

Once you have a server initialized, calling [`listen_on`](https://dev.bonsaidb.io/main/bonsaidb/server/struct.CustomServer.html#method.listen_on) will begin listening for connections on the port specified. This uses the preferred native protocol which uses UDP. If you find that UDP is not working for your setup or want to put `BonsaiDb` behind a load balancer that doesn't support UDP, you can enable WebSocket support and call [`listen_for_websockets_on`](https://dev.bonsaidb.io/main/bonsaidb/server/struct.CustomServer.html#method.listen_for_websockets_on).

You can call both, but since these functions don't return until the server is shut down, you should spawn them instead:

```rust,noplayground,no_run
let task_server = server.clone();
tokio::spawn(async move {
    task_server.listen_on(5645).await
});
let server = server.clone();
tokio::spawn(async move {
    task_server.listen_for_websockets_on("localhost:8080", false).await
});
```

If you're not running any of your own code on the server, and you're only using one listening method, you can just await the listen method of your choice in your server's main.

<!-- TODO: Certificates -->

## From the Client

The [`Client`][client] can support both the native protocol and WebSockets. It determines which protocol to use based on the scheme in the URL:

* `bonsaidb://host:port` will connect using the native `BonsaiDb` protocol.
* `ws://host:port` will connect using WebSockets.

Here's how to connect, from [`bonsaidb/examples/server.rs`](https://github.com/khonsulabs/bonsaidb/blob/main/crates/bonsaidb/examples/server.rs):

```rust,noplayground,no_run
Client::new(
    Url::parse("bonsaidb://localhost:5645")?,
    Some(certificate),
)
.await?
```

This is using a pinned certificate to connect. Other methods are supported, but better certificate management is coming soon.

<!-- TODO: Certificates -->

## Common Traits

* [`Server`][server] implements [`ServerConnection`](../traits/server_connection.md).
* [`Server::database()`](https://dev.bonsaidb.io/main/bonsaidb/server/struct.CustomServer.html#method.database) returns a local [`Database`](https://dev.bonsaidb.io/main/bonsaidb/local/struct.Database.html), which implements [`Connection`](../traits/connection.md), [`Kv`](../traits/kv.md), and [`PubSub`](../traits/kv.md). Local access in the server executable doesn't go over the network.
* [`Client`][client] implements [`ServerConnection`](../traits/server_connection.md).
* [`Client::database()`](https://dev.bonsaidb.io/main/bonsaidb/client/struct.Client.html#method.database) returns a [`RemoteDatabase`](https://dev.bonsaidb.io/main/bonsaidb/client/struct.RemoteDatabase.html), which implements [`Connection`](../traits/connection.md), [`Kv`](../traits/kv.md), and [`PubSub`](../traits/kv.md).

[server]: https://dev.bonsaidb.io/main/bonsaidb/server/type.Server.html
[storage]: https://dev.bonsaidb.io/main/bonsaidb/local/struct.Storage.html
[client]: https://dev.bonsaidb.io/main/bonsaidb/client/struct.Client.html
