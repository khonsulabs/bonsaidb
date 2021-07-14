# Permission Statements

A [Statement](https://pliantdb.dev/main/pliantdb/core/permissions/struct.Statement.html) grants permissions to execute [`Action`s](https://pliantdb.dev/main/pliantdb/core/permissions/trait.Action.html) on [`ResourceName`s](https://pliantdb.dev/main/pliantdb/core/permissions/struct.ResourceName.html).

## Actions and Resources

`ResourceName`s are simply namespaced [`Identifier`s](https://pliantdb.dev/main/pliantdb/core/permissions/enum.Identifier.html). An example could be: `"pliantdb".*."khonsulabs-admin.users".1`. Each segment can be [a string](https://pliantdb.dev/main/pliantdb/core/permissions/enum.Identifier.html#variant.String), [an integer](https://pliantdb.dev/main/pliantdb/core/permissions/enum.Identifier.html#variant.Integer), or [a wildcard (`*`)](https://pliantdb.dev/main/pliantdb/core/permissions/enum.Identifier.html#variant.Any).

In `PliantDb`, nearly everything has a resource name. The example above refers to a document with ID `1` in the `khonsulabs-admin.users` collection in any database. The [`pliantdb::core::permissions::pliant`](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/index.html) module contains functions to create properly formatted `ResourceName`s.

Also within the same module are the built-in `Action`s. The base enum for all actions used within `PliantDb` is [`PliantAction`](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/enum.PliantAction.html) Below is an overview of the resource names and actions by category.

### Server

The [`ServerAction`](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/enum.ServerAction.html) enum contains the actions that are related to [`ServerConnection`](https://pliantdb.dev/main/pliantdb/core/connection/trait.ServerConnection.html). For APIs that accept a database name parameter, the resource name will be [`database_resource_name(database)`](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/fn.database_resource_name.html). For all other actions, the resource name is [`pliantdb_resource_name()`](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/fn.pliantdb_resource_name.html).

For actions that operate upon users (e.g., creating a user), the resource name is [user_resource_name(username)](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/fn.user_resource_name.html).

#### At-rest Encryption

Access to encrypted information can be controlled by limiting access to the encryption key used. Currently, `PliantDb` only has support for a shared master key, but in the future additional keys will be able to be created. Because [`Encrypt`](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/enum.EncryptionKeyAction.html#variant.Encrypt) and [`Decrypt`](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/enum.EncryptionKeyAction.html#variant.Decrypt) are separate actions, access to read and write can be controlled independently.

The resource name for an encryption key is [`encryption_key_resource_name(key_id)`](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/fn.encryption_key_resource_name.html).

### Database

The [`DatabaseAction`](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/enum.DatabaseAction.html) enum contains the actions that are related to a specific database. Actions that act on the database directly will use the resource name [`database_resource_name(database)`](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/fn.database_resource_name.html).

For `Collection`s, there are three resource names used. For actions that operate on the collection directly, the resource name is [`collection_resource_name(database, collection)`](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/fn.database_resource_name.html). For actions that operate on a document, the resource name is [`document_resource_name(database, collection, id)`](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/fn.document_resource_name.html). Finally, for actions that operate on a `View`, the resource name is [`view_resource_name(database, view)`](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/fn.view_resource_name.html).

For actions that operate upon the key-value entry, the resource name is [`kv_key_resource_name(database, namespace, key)`](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/fn.kv_key_resource_name.html).

For actions that operate on a `PubSub` topic, the resource name is [`pubsub_topic_resource_name(database, topic)`](https://pliantdb.dev/main/pliantdb/core/permissions/pliant/fn.pubsub_topic_resource_name.html).

## Statement Examples

*Coming Soon*.
