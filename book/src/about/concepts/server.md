# Server

A [Server](https://pliantdb.dev/main/pliantdb/core/connection/trait.ServerConnection.html) oversees one or more [Schemas](./schema.md) and named [Databases](./database.md). Over time, this concept will be extended to have support for other features including users and permissions.

There are two ways to initialize a `PliantDB` server:

* [`Storage`](https://pliantdb.dev/main/pliantdb/local/struct.Storage.html): A local, file-based server implementation with no networking capabilities.
* [`Server`](https://pliantdb.dev/main/pliantdb/server/struct.Server.html): A networked server implementation, written using `Storage`. This server supports [QUIC](https://en.wikipedia.org/wiki/QUIC)- and [WebSocket](https://en.wikipedia.org/wiki/WebSocket)-based protocols. The QUIC protocol is preferred, but it uses UDP which many load balancers don't support. If you're exposing `PliantDB` behind a load balancer, WebSockets may be the only option depending on your host's capabilities.
