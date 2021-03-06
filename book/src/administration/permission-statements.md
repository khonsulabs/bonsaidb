# Permission Statements

A [Statement]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/struct.Statement.html) grants permissions to execute [`Action`s]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/trait.Action.html) on [`ResourceName`s]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/struct.ResourceName.html).

## Actions and Resources

`ResourceName`s are simply namespaced [`Identifier`s]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/enum.Identifier.html). An example could be: `"bonsaidb".*."khonsulabs-admin.users".1`. Each segment can be [a string]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/enum.Identifier.html#variant.String), [an integer]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/enum.Identifier.html#variant.Integer), or [a wildcard (`*`)]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/enum.Identifier.html#variant.Any).

In BonsaiDb, nearly everything has a resource name. The example above refers to a document with ID `1` in the `khonsulabs-admin.users` collection in any database. The [`bonsaidb::core::permissions::bonsai`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/index.html) module contains functions to create properly formatted `ResourceName`s.

Also within the same module are the built-in `Action`s. The base enum for all actions used within BonsaiDb is [`BonsaiAction`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/enum.BonsaiAction.html) Below is an overview of the resource names and actions by category.

### Server

The [`ServerAction`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/enum.ServerAction.html) enum contains the actions that are related to [`StorageConnection`]({{DOCS_BASE_URL}}/bonsaidb/core/connection/trait.StorageConnection.html). For APIs that accept a database name parameter, the resource name will be [`database_resource_name(database)`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/fn.database_resource_name.html). For all other actions, the resource name is [`bonsaidb_resource_name()`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/fn.bonsaidb_resource_name.html).

For actions that operate upon users (e.g., creating a user), the resource name is [user_resource_name(username)]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/fn.user_resource_name.html).

#### At-rest Encryption

Access to encrypted information can be controlled by limiting access to the encryption key used. Currently, BonsaiDb only has support for a shared master key, but in the future additional keys will be able to be created. Because [`Encrypt`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/enum.EncryptionKeyAction.html#variant.Encrypt) and [`Decrypt`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/enum.EncryptionKeyAction.html#variant.Decrypt) are separate actions, access to read and write can be controlled independently.

The resource name for an encryption key is [`encryption_key_resource_name(key_id)`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/fn.encryption_key_resource_name.html).

### Database

The [`DatabaseAction`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/enum.DatabaseAction.html) enum contains the actions that are related to a specific database. Actions that act on the database directly will use the resource name [`database_resource_name(database)`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/fn.database_resource_name.html).

For `Collection`s, there are three resource names used. For actions that operate on the collection directly, the resource name is [`collection_resource_name(database, collection)`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/fn.database_resource_name.html). For actions that operate on a document, the resource name is [`document_resource_name(database, collection, id)`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/fn.document_resource_name.html). Finally, for actions that operate on a `View`, the resource name is [`view_resource_name(database, view)`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/fn.view_resource_name.html).

For actions that operate upon the key-value entry, the resource name is [`keyvalue_key_resource_name(database, namespace, key)`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/fn.keyvalue_key_resource_name.html).

For actions that operate on a `PubSub` topic, the resource name is [`pubsub_topic_resource_name(database, topic)`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/fn.pubsub_topic_resource_name.html).

## Statement Examples

*Coming Soon*.
