# Database

A Database is a set of stored data. Each Database is described by a [Schema](./schema.md). Unlike the other concepts, this concept corresponds to multiple types:

- For pliantdb-local: [`Database`](https://pliantdb.dev/main/pliantdb/local/struct.Database.html)
- For pliantdb-server: [`ServerDatabase`](https://pliantdb.dev/main/pliantdb/server/struct.ServerDatabase.html)
- For pliantdb-client: [`RemoteDatabase`](https://pliantdb.dev/main/pliantdb/client/struct.RemoteDatabase.html)

All of these types implement the [`Connection`](../../traits/connection.md) trait.
