# Database

A Database is a set of stored data. Each Database is described by a [Schema](./schema.md). Unlike the other concepts, this concept corresponds to multiple types:

- For bonsaidb-local: [`Database`](https://bonsaidb.dev/main/bonsaidb/local/struct.Database.html)
- For bonsaidb-server: [`ServerDatabase`](https://bonsaidb.dev/main/bonsaidb/server/struct.ServerDatabase.html)
- For bonsaidb-client: [`RemoteDatabase`](https://bonsaidb.dev/main/bonsaidb/client/struct.RemoteDatabase.html)

All of these types implement the [`Connection`](../../traits/connection.md) trait.
