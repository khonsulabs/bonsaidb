# Storage

The [StorageConnection](https://dev.bonsaidb.io/main/bonsaidb/core/connection/trait.StorageConnection.html) trait allows interacting with a BonsaiDb multi-database storage instance.

There are three implementations of the `StorageConnection` trait:

* [`Storage`](https://dev.bonsaidb.io/main/bonsaidb/local/struct.Storage.html): A local, file-based server implementation with no networking capabilities.
* [`Server`](https://dev.bonsaidb.io/main/bonsaidb/server/type.Server.html): A networked server implementation, written using `Storage`. This server supports [QUIC](https://en.wikipedia.org/wiki/QUIC)- and [WebSocket](https://en.wikipedia.org/wiki/WebSocket)-based protocols. The QUIC protocol is preferred, but it uses UDP which many load balancers don't support. If you're exposing `BonsaiDb` behind a load balancer, WebSockets may be the only option depending on your host's capabilities.
* [`Client`](https://dev.bonsaidb.io/main/bonsaidb/client/struct.Client.html): A network client implementation that connects to a `Server`.
