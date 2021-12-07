# Custom Api Server

The [`CustomApi`](https://dev.bonsaidb.io/main/bonsaidb/core/custom_api/trait.CustomApi.html) trait defines three associated types, Request, Response, and Error. A backend "dispatches" `Request`s and expects a `Result<Response, Error>` in return.

> All code on this page comes from this example: [`examples/basic-server/examples/custom-api.rs`](https://github.com/khonsulabs/bonsaidb/blob/main/examples/basic-server/examples/custom-api.rs).

This example defines a Request and a Response type, but uses `BonsaiDb`'s [`Infallible`](https://dev.bonsaidb.io/main/bonsaidb/core/custom_api/struct.Infallible.html) type for the error:

```rust,noplayground,no_run
{{#include ../../../../examples/basic-server/examples/custom-api.rs:api-types}}
```

To implement the server, we must first implement a custom [`Backend`](https://dev.bonsaidb.io/main/bonsaidb/server/trait.Backend.html) that ties the server to the `CustomApi`. We also must define a [`CustomApiDispatcher`](https://dev.bonsaidb.io/main/bonsaidb/server/trait.CustomApiDispatcher.html), which gives an opportunity for the dispatcher to gain access to the [`ConnectedClient`](https://dev.bonsaidb.io/main/bonsaidb/server/struct.ConnectedClient.html) and/or [`CustomServer](https://dev.bonsaidb.io/main/bonsaidb/server/struct.CustomServer.html) instances if they are needed to handle requests.

Finally, either [`Dispatcher`](https://dev.bonsaidb.io/main/bonsaidb/core/permissions/trait.Dispatcher.html) must be implemented manually or [`actionable`](https://dev.bonsaidb.io/main/bonsaidb/core/actionable/) can be used to derive an implementation that uses individual traits to handle each request. The example uses actionable:

```rust,noplayground,no_run
{{#include ../../../../examples/basic-server/examples/custom-api.rs:server-traits}}
```

Finally, the client can issue the API call and receive the response, without needing any extra steps to serialize. This works regardless of whether the client is connected via QUIC or WebSockets.

```rust,noplayground,no_run
{{#include ../../../../examples/basic-server/examples/custom-api.rs:api-call}}
```

## Permissions

One of the strengths of using BonsaiDb's custom api functionality is the ability to tap into the permissions handling that BonsaiDb uses. The Ping request was defined with `protection = "none"` which skips all permission validation. However, `DoSomethingSimple` uses the "simple" protection model, and `DoSomethingCustom` uses the "custom" protection model. The comments in the example below should help explain the rationale:

```rust,noplayground,no_run
{{#include ../../../../examples/basic-server/examples/custom-api.rs:permission-handles}}
```

This example uses `authenticated_permissions` to grant access to `ExampleAction::DoSomethingSimple` and `ExampleAction::DoSomethingCustom` to all users who have logged in:

```rust,noplayground,no_run
{{#include ../../../../examples/basic-server/examples/custom-api.rs:server-init}}
```

For more information on managing permissions, [see Administration/Permissions](../../administration/permissions.md)