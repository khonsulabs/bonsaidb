# Custom Api Server

The [`Api`]({{DOCS_BASE_URL}}/bonsaidb/core/api/trait.Api.html) trait defines two associated types, Response, and Error. The `Api` type is akin to a "request" that the server receives. The server will invoke a [`Handler`][handler], expecting a result with the associated Response and Error types.

> All code on this page comes from this example: [`examples/basic-server/examples/custom-api.rs`][full-example].

This example shows how to derive the `Api` trait. Because an error type isn't specified, the derive macro will use BonsaiDb's [`Infallible`]({{DOCS_BASE_URL}}/bonsaidb/core/api/enum.Infallible.html) type as the error type.

```rust,noplayground,no_run
{{#include ../../../../examples/basic-server/examples/custom-api.rs:api-types}}
```

To implement the server, we must define a [`Handler`][handler], which is invoked each time the `Api` type is received by the server.

```rust,noplayground,no_run
{{#include ../../../../examples/basic-server/examples/custom-api.rs:server-traits}}
```

Finally, the client can issue the API call and receive the response, without needing any extra steps to serialize. This works regardless of whether the client is connected via QUIC or WebSockets.

```rust,noplayground,no_run
{{#include ../../../../examples/basic-server/examples/custom-api.rs:api-call}}
```

## Permissions

One of the strengths of using BonsaiDb's custom api functionality is the ability to tap into the permissions handling that BonsaiDb uses. The Ping request has no permissions, but let's add permission handling to our `IncrementCounter` API. We will do this by creating an `increment_counter` function that expects two parameters: a connection to the storage layer with unrestricted permissions, and a second connection to the storage layer which has been restricted to the permissions the client invoking it is authorized to perform:

```rust,noplayground,no_run
{{#include ../../../../examples/basic-server/examples/custom-api.rs:permission-handles}}
```

The [`Handler`][handler] is provided a [`HandlerSession`][handler-session] as well as the `Api` type, which provides all the context information needed to verify the connected client's authenticated identity and permissions. Additionally, it provides two ways to access the storage layer: with unrestricted permissions or restricted to the permissions granted to the client.

Let's finish configuring the server to allow all unauthenticated users the ability to `Ping`, and all authenticated users the ability to `Increment` the counter:

```rust,noplayground,no_run
{{#include ../../../../examples/basic-server/examples/custom-api.rs:server-init}}
```

For more information on managing permissions, [see Administration/Permissions](../../administration/permissions.md).

The full example these snippets are taken from is [available in the repository][full-example].

[handler]: {{DOCS_BASE_URL}}/bonsaidb/server/api/trait.Handler.html
[handler-session]: {{DOCS_BASE_URL}}/bonsaidb/server/api/struct.HandlerSession.html
[full-example]: {{REPO_BASE_URL}}/examples/basic-server/examples/custom-api.rs
