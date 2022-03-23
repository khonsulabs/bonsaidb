# Commerce Benchmark

This benchmark framework is an attempt at simulating a complex relational
database workload. All benchmarks are measuring fully ACID-compliant reads and
writes.

It starts by generating an initial data set. This data set includes data that
would enable a hypothetical "shopper" the ability to shop using a series of
operations.

Using this initial data set and a shopper configuration, a number of plans are
generated that will be run against each enabled database backend. The plans can
be divided up between any number of agents (worker tasks) when being executed to
simulate various levels of concurrency.

## Current Reports

From GitHub Actions:

[![Commerce Benchmark Overview](https://dev.bonsaidb.io/benchmarks/commerce/Overview.png)](https://dev.bonsaidb.io/benchmarks/commerce/)

Run occasionally at [Scaleway](https://scaleway.com) on a GP1-XS instance running Ubuntu 20.04 with 4 CPU cores, 16GB of RAM, and local NVME storage:

[![Commerce Benchmark Overview](https://khonsulabs-storage.s3.us-west-000.backblazeb2.com/bonsaidb-scaleway-gp1-xs/commerce/Overview.png)](https://khonsulabs-storage.s3.us-west-000.backblazeb2.com/bonsaidb-scaleway-gp1-xs/commerce/index.html)

## Benchmark Notes

### Filesystem Bloat

BonsaiDb is built atop [Nebari](https://github.com/khonsulabs/nebari), which
uses an append-only file format to store data. This has many benefits, but it
also has one significant drawback: to reclaim disk space, the file must be
rewritten. This operation is efficient, but it is not shown in any benchmarks
here as compaction generally is something that is run infrequently and can be
performed during non-peak hours.

With that said, this particular benchmark does not simulate a load that would
traditionally cause a large amount of filesystem bloat. Workloads that modify
large amounts of existing data or insert and delete data on a regular basis will
require compaction more frequently than workflows that primarily insert and
query data with few updates and deletes.

### Foreign Keys

BonsaiDb [doesn't currently
support](https://github.com/khonsulabs/bonsaidb/issues/136) validating foreign
keys, and PostgreSQL does. When running the PostgreSQL benchmark with and without foreign
keys, most benchmarks showed a reduction in speed by leaving the foreign keys
in. As such this benchmark currently has no foreign keys, but they can be
re-enabled easily by removing the SQL comments.

### Product Ratings

Product Ratings are implemented using pure database features for aggregation. In BonsaiDb, this means utilizing a [Map/Reduce view](https://dev.bonsaidb.io/main/guide/about/concepts/view.html), and in PostgreSQL, this means using a [materialized view](https://www.postgresql.org/docs/current/rules-materializedviews.html). Because Views are the primary mechanism for querying data in BonsaiDb, they are more feature rich than PostgreSQL's materialized views.

The current implementation of the product review operation will not only insert or update any existing review, it will also refresh the view to ensure that the new information is available. BonsaiDb has the ability to control when to update the view using the [AccessPolicy](https://dev.bonsaidb.io/main/bonsaidb/core/connection/enum.AccessPolicy.html) enum.

BonsaiDb's implementation isn't optimized in another way: the benchmark is making two database requests to fulfill queries for products -- one for the product data, and another for the ratings. With the local version of BonsaiDb, there is no network latency, so this isn't really a concern. But once network latency is involved, a [`Api`](https://dev.bonsaidb.io/main/guide/about/access-models/custom-api-server.html) could have been written to make it a single request. We didn't take this route because to us `Api` isn't a pure database function, and this benchmark suite is meant to test database functionality.

## Preparing PostgreSQL for Benchmarks

To execute PostgreSQL benchmarks, create a user in your instance of PostgreSQL
and give it ownership over a database.

```sql
CREATE USER commerce_bench_user PASSWORD 'password' LOGIN;
CREATE DATABASE commerce_bench OWNER commerce_bench_user;
```

Next, configure these environment variables. The easiest way is to create a file
named `.env` in the root of the repository with contents similar to this:

```ini
COMMERCE_POSTGRESQL_URL=postgres://commerce_bench_user:password@localhost/commerce_bench
```

Finally, run with the `postgresql` feature enabled:

```sh
cargo bench --bench commerce --features postgresql
```
