# Overview

`BonsaiDb` aims to offer the majority of its functionality in [local operation](./local.md). The [networked server](./server.md) adds some functionality on top of the local version, but its main function is to add the ability to use networking to talk to the database.

Because of this model, it makes it easy to transition a local database to a networked database server. Start with whatever model fits your needs today, and when your neeeds change, `BonsaiDb` will adapt.

## When to use the [Local Integration](./local.md)

* You're going to databases from one process at a time. `BonsaiDb` is designed for concurrency and can scale with the capabilities of the hardware. However, the underlying storage layer that `BonsaiDb` is built upon, [sled](http://sled.rs), does not support multiple processes writing its data simultaneously. If you need to access the database from multiple processes, the [server integration](./server.md) is what you should use. While it doesn't offer IPC communication today, a pull-request would be accepted to that added that functionality (along with the corresponding unit tests).
* You have no public API/PubSub/access needs or have implemented those with another stack.

## When to use the [Server Integration](./server.md)

* You need to access databases from more than one process or machine.
* You are OK with downtime due to loss of service when the single server is offline. If you need to have a [highly-available][HA] database, you should use the Cluster Integration (Coming Soon).
* Your database load can be met with a single machine. If you have enough load that you need to share the processing power of multiple servers, you should use the Cluster Integration (Coming Soon)

## Coming Soon: When to use the [Cluster Integration](./cluster.md)

* You need to access databases from more than one machine.
* You need a [highly-available][HA] setup.
* You need/want to split load between multiple machines.

[HA]: https://en.wikipedia.org/wiki/High_availability
