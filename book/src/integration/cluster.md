# Integrating into a PliantDb Cluster

Coming Soon.

The goals of this feature are to make clustering simple. We hope to provide an experience that allows someone who is operating a networked server to desire two types of clusters:

## One-leader mode

When setting up a cluster initially, you will begin with one-leader mode. In this mode, you can add as many nodes to the cluster as you wish, but only one node will be processing all of the data updates. All nodes can handle requests, but requests that can't be served locally will be forwarded to the leader. This allows for the use of read-replicas to alleviate load in some read-heavy situations.

Another benefit of this mode are that it supports a two-node configuration. If you're scaling your app and need a reliable backup for quicker disaster recovery, you can operate a read replica and manually failover when the situation arises.

If you decide to allow automatic failover in this mode, there is a chance for data loss, as the leader does not wait for read-replicas to synchronize data. Any transactions that committed and were not synchronized before the outage occurred would not be on the other servers. Thus, this mode is *not intended for high-availability configurations*, although some users may elect to use it in such a configuration knowing these limitations.

## Quorum mode

Once you have a cluster with at least 3 nodes, you can switch the cluster into quorum mode. For any given `N` nodes, all requests must reach an agreed response by `N / 2 + 1` members. For example, in a cluster of 3 nodes, there must be 2 successful responses before a client can receive a response to its request.

In quorum mode, your data is divided into shards and those shards replicated throughout the cluster onto at least 3 nodes (configurable). Initially, with just 3 nodes available, the only benefits are having a highly-available cluster with no data loss during when a single node goes down.

As you add more nodes to your cluster, however, you can re-balance your databases to move shards. The author of PliantDb did not enjoy this process in CouchDB when he had to do it and aims to make these tools easy and effortless to use. Ideally, there would be a low-maintenance mode that would allow the cluster to re-shard itself authomatically during allowed maintenance periods, ensuring data is distributed more evenly amongst the cluster.

Additional long-term dreams of quorum mode include the ability to customize node selection criteria on a per-database basis. The practical use of node selection is to ensure that at least 3 unique nodes are picked for each shard. However, allowing custom logic to evaluate which nodes should be selected for any database would allow ultimate flexibility. For example, if you have a globally deployed application, and you have some data that is geographically specific, you could locate each region's database on nodes within those locations' data centers.

## When?

Clustering is an important part of the design of [Cosmic Verge](https://github.com/khonsulabs/cosmicverge). As such, it is a priority for us to work on. But, the overall game is a very large project, so we hesitate to make any promises on timelines.
