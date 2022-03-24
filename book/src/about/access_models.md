# Use cases of BonsaiDb

## Single database model (No networking)

This use case is most similar to utilizing SQLite for your database. In this mode, BonsaiDb directly interacts with files on your disk to provide your database. Unlike other file-based databases, however, it's easy to migrate to any of these scenarios from this starting position:

```mermaid
graph LR
  code{{Rust Code}}
  local[(bonsaidb-local::Database)]
  code <--> local
```

A working example of how to use a local database can be found at [`examples/basic-local/examples/basic-local.rs`](https://github.com/khonsulabs/bonsaidb/blob/main/examples/basic-local/examples/basic-local.rs).

## Multi-database model (No networking)

This model is most similar to using multiple SQLite databases. In this mode, you interact with a [`Storage`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.Storage.html) that you spawn within your code.

```mermaid
graph LR
  code{{Rust Code}}
  local[(bonsaidb-local::Storage)]
  code <--> server
  server <--> local
```

If you look at the source behind [`Database::open_local`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.Storage.html#method.open_local), you'll see that the single-database model is using `Storage` under the hood.

## Server model (QUIC or WebSockets)

This model is most similar to using other document databases, like CouchDB or MongoDB. In this mode, you interact with a [`Client`]({{DOCS_BASE_URL}}/bonsaidb/client/struct.Client.html) that connects via either QUIC or WebSockets with a server. From the server code's perspective, this model is the same as the multi-database model, except that the server is listening for and responding to network traffic.

```mermaid
graph LR
  client-code{{Rust Client Code}}
  server-code{{Rust Server Code}}
  client[[bonsaidb-client]]
  server[[bonsaidb-server]]
  local[(bonsaidb-local)]
  client-code <--> client
  client <-. network .-> server
  server <--> local
  server-code <--> server
```

A working example of this model can be found at [`examples/basic-server/examples/basic-server.rs`](https://github.com/khonsulabs/bonsaidb/blob/main/examples/basic-server/examples/basic-server.rs). When writing client/server applications that utilize BonsaiDb, you can have the BonsaiDb server running withing your server application. This means that your server still has the ability not use networking to interact with BonsaiDb. Regardless of if you run any other server code, your BonsaiDb server will be accessible through a `Client` over the network.

## API Platform model (QUIC or WebSockets)

If you're finding yourself developing an API for your application, and all of the consumers of this API are already connected to BonsaiDb, you may want to take advantage of the custom api functionality of the server:

```mermaid
graph LR
  client-code{{Rust Client Code}}
  server-code{{Rust Server Code}}
  client[[bonsaidb-client]]
  server[[bonsaidb-server]]
  backend[[Backend]]
  local[(bonsaidb-local)]
  client-code <--> client
  client <-. network .-> server
  server <--> local
  server-code <--> server
  server-code <--> backend
  backend <--> server
```

The BonsaiDb [`CustomServer`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.CustomServer.html) type accepts one generic parameter that implements the [`Backend`]({{DOCS_BASE_URL}}/bonsaidb/server/trait.Backend.html) trait. This trait is used to customize the server in many ways, but one of the associated types is a [`Api`]({{DOCS_BASE_URL}}/bonsaidb/core/api/trait.Api.html) implementor.

See [this page](./access-models/custom-api-server.md) for an overview of how to set up a custom api server.

## Coming Later: Cluster model

When you're at the stage of scaling beyond a single server, you will be able to upgrade your server to a cluster using the hypothetical `bonsaidb-cluster` crate. The clustering model is still being designed, but the goal is something similar to:

```mermaid
graph LR
  client-code{{Rust Client Code}}
  server-code{{Rust Server Code}}
  client[[bonsaidb-client]]
  server1[[server 1]]
  server2[[server 2]]
  server3[[server 3]]
  cluster[[bonsaidb-cluster]]
  client-code <--> client
  client <-. network .-> cluster
  server-code <--> cluster
  cluster <--> server1
  cluster <--> server2
  cluster <--> server3
  server1 <--> server2
  server2 <--> server3
  server1 <--> server3
```

In this model, the local storage element is hidden; Each server has its own storage. This model is very similar from the viewpoint of your server and client code -- the primary difference is that the server-side connection is being established using the cluster crate. From the client's perspective, the cluster behaves as a single entity -- sending a request to any server node will result in the same result within the cluster.

All features of BonsaiDb will be designed to work in cluster mode seamlessly. `PubSub` will ensure that subscribers will receive messages regardless of which server they're connected to.
