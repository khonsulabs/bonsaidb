# Configuration

BonsaiDb attempts to have reasonable default configuration options, but it's important to browse the available options to ensure there aren't options that might help your particular needs.

## Storage Configuration

The [`StorageConfiguration`]({{DOCS_BASE_URL}}/bonsaidb/local/config/struct.StorageConfiguration.html) structure is used to open a local-only database. The `ServerConfiguration` struct contains [an instance of `StorageConfiguration`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerConfiguration.html#structfield.storage), and all configuration optionsl are available on it.

### Vault Key Storage

By default, BonsaiDb sets [`vault_key_storage`]({{DOCS_BASE_URL}}/bonsaidb/local/config/struct.StorageConfiguration.html#structfield.vault_key_storage) to a file stored within the database folder. This is **incredibly insecure and should not be used outside of testing**.

For secure encryption, it is important to store the vault keys in a location that is separate from the database. If the keys are on the same harware as the database, anyone with access to the disk will be able to decrypt the stored data.

If you have more than one server, you can still use [`LocalVaultKeyStorage`]({{DOCS_BASE_URL}}/bonsaidb/local/vault/struct.LocalVaultKeyStorage.html) in conjunction with a mounted network share for reasonable security practices -- assuming the network share itself is properly secured.

If you have an S3-compatible storage service available, you can use [`bonsaidb::keystorage::s3`]({{DOCS_BASE_URL}}/bonsaidb/keystorage/s3/index.html) to store the vault keys with that service.

Note that by storing your keys remotely, your BonsaiDb database will not be able to be opened unless the keys are able to be read.

Vault Key Storage can also be set using [`Builder::vault_key_storage`]({{DOCS_BASE_URL}}/bonsaidb/local/config/trait.Builder.html#tymethod.vault_key_storage).

### Default Encryption Key

By setting [`default_encryption_key`]({{DOCS_BASE_URL}}/bonsaidb/local/config/struct.StorageConfiguration.html#structfield.default_encryption_key) to a key, all data will be encrypted when written to the disk.

If `default_encryption_key` is `None`, encryption will still be performed for collections that return a key from [`Collection::encryption_key()`]({{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.Collection.html#method.encryption_key).

Can also be set using [`Builder::default_encryption_key`]({{DOCS_BASE_URL}}/bonsaidb/local/config/trait.Builder.html#tymethod.default_encryption_key).

### Tasks: Worker Count

The [`tasks.worker_count`]({{DOCS_BASE_URL}}/bonsaidb/local/config/struct.Tasks.html#structfield.worker_count) setting controls the number of worker tasks that are spawned to process background tasks.

Can also be set using [`Builder::tasks_worker_count`]({{DOCS_BASE_URL}}/bonsaidb/local/config/trait.Builder.html#tymethod.tasks_worker_count).

### Views: Check Integrity on Open

When [`views.check_integrity_on_open`]({{DOCS_BASE_URL}}/bonsaidb/local/config/struct.Views.html#structfield.check_integrity_on_open) is true, all views in all databases will be checked on startup for integrity. If this value is false, the integrity of the view will not be checked until it is accessed for the first time.

By default, BonsaiDb delays checking a view's integrity until its accessed for the first time. it may, however, be preferred to have a higher startup time to ensure consistent response times once the server is running after a restart of the server.

Can also be set using [`Builder::check_view_integrity_on_open`]({{DOCS_BASE_URL}}/bonsaidb/local/config/trait.Builder.html#tymethod.check_view_integrity_on_open).

### Key-Value Persistence

The Key-Value store is designed to be a lightweight, atomic data store that is suitable for caching data, tracking metrics, or other situations where a Collection might be overkill.

By default, BonsaiDb persists Key-Value store changes to disk immediately. For light usage, this will not be noticable, and it ensures that no data will ever be lost.

If you're willing to accept potentially losing recent writes, [`key_value_persistence`]({{DOCS_BASE_URL}}/bonsaidb/local/config/trait.Builder.html#tymethod.key_value_persistence) can be configured to lazily commit changes to disk. The documentation for [`KeyValuePersistence`]({{DOCS_BASE_URL}}/bonsaidb/local/config/struct.KeyValuePersistence.html) contains examples as well as an explanation of how the rules are evaluated.

Key-Value Persistence can also be set using [`Builder::key_value_persistence`]({{DOCS_BASE_URL}}/bonsaidb/local/config/trait.Builder.html#tymethod.key_value_persistence).

## Server Configuration

The [`ServerConfiguration`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerConfiguration.html) structure is used to open a BonsaiDb server. Being built atop the local storage engine, this structure exposes [an instance of `StorageConfiguration`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerConfiguration.html#structfield.storage), allowing full customization.

### Server Name

The [`server_name`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerConfiguration.html#structfield.server_name) setting is for the primary DNS name of the server. The server's TLS certificate should be valid for the server's name.

When using ACME, this setting controls the primary certificate requested.

Can also be set using [a builder-style method]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerConfiguration.html#method.server_name).

### Client Simultaneous Request Limit

BonsaiDb's networking protocols support multiple requests to be sent before any responses have been received, sometimes called pipelining. Without a limit, a single malicious client could send a large number of load-inducing requests and cause reliability of service issues for other clients.

By limiting each connection's maximum ability to a reasonable number, it allows clients to take advantage of pipelining without allowing any one client to saturate the server with requests.

This limit is set using the [client_simultaneous_request_limit]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerConfiguration.html#structfield.client_simultaneous_request_limit) field or [builder-style method]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerConfiguration.html#method.client_simultaneous_request_limit).

### Request Worker Count

The [`request_workers`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerConfiguration.html#structfield.request_workers) configuration controls the number of worker tasks that process incoming requests from connected clients. It can also be set via a [builder-style method]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerConfiguration.html#method.request_workers).

### Default Permissions and Authenticated Permissions

When first connecting to a server, the client is unauthenticated and is granted the permissions defined by [`default_permissions`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerConfiguration.html#structfield.default_permissions). Once a connected client has authenticated, the client will be granted [`authenticated_permissions`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerConfiguration.html#structfield.authenticated_permissions) in addition to whatever permissions already granted by the authenticated role.

By default, both `default_permissions` and `authenticated_permissions` contain no granted permissions. This means that by default, no connections are allowed to a server, as the connection hasn't been gramted [`BonsaiAction::Server(`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/enum.BonsaiAction.html#variant.Server)[`ServerAction::Connect() )`]({{DOCS_BASE_URL}}/bonsaidb/core/permissions/bonsai/enum.ServerAction.html#variant.Connect).

### ACME Configuration (LetsEncrypt)

ACME has two configurable options, a contact email and the ACME directory.

#### ACME Contact Email

The contact email is submitted to the ACME directory as part of requesting a TLS certificate. It is optional for the LetsEncrypt directories.

A valid value for this field begins with `mailto:`.

The contact email can be set using [`acme.contact_email`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.AcmeConfiguration.html#structfield.contact_email) or the [builder-style method]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerConfiguration.html#method.acme_contact_email).

#### ACME Directory

By default, BonsaiDb uses the [production LetsEncrypt directory]({{DOCS_BASE_URL}}/bonsaidb/server/constant.LETS_ENCRYPT_PRODUCTION_DIRECTORY.html), but any ACME directory can be specified.

The directory can be set using [`acme.directory`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.AcmeConfiguration.html#structfield.directory) or the [builder-style method]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerConfiguration.html#method.acme_directory).
