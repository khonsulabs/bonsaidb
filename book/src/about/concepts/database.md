# Database

A Database is a set of stored [collections](./collection.md). Each Database is described by a [Schema](./schema.md). Unlike the other concepts, this concept corresponds to multiple types:

- For bonsaidb-local: [`Database`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.Database.html) / [`AsyncDatabase`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.AsyncDatabase.html)
- For bonsaidb-server: [`ServerDatabase`]({{DOCS_BASE_URL}}/bonsaidb/server/struct.ServerDatabase.html)
- For bonsaidb-client: [`AsyncRemoteDatabase`]({{DOCS_BASE_URL}}/bonsaidb/client/struct.AsyncRemoteDatabase.html)/[`BlockingRemoteDatabase`]({{DOCS_BASE_URL}}/bonsaidb/client/struct.BlockingRemoteDatabase.html)

All of these types implement a [`Connection`](../../traits/connection.md) trait.
