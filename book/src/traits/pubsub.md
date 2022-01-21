# PubSub Trait

The [`PubSub` trait][pubsub] contains functions for using [PubSub](../about/concepts/pubsub.md) in BonsaiDb. This trait is implemented by the [`Database`](../about/concepts/database.md) types in each crate:

- For bonsaidb-local: [`Database`](https://dev.bonsaidb.io/main/bonsaidb/local/struct.Database.html)
- For bonsaidb-server: [`ServerDatabase`](https://dev.bonsaidb.io/main/bonsaidb/server/struct.ServerDatabase.html)
- For bonsaidb-client: [`RemoteDatabase`](https://dev.bonsaidb.io/main/bonsaidb/client/struct.RemoteDatabase.html)

Using this trait, you can write code that generically can work regardless of whether BonsaiDb is operationg locally with no network connection or across the globe.

This is an [async trait](https://crates.io/crates/async-trait), which unfortunately yields [messy documentation][pubsub].

[pubsub]: https://dev.bonsaidb.io/main/bonsaidb/core/pubsub/trait.PubSub.html
