# Todos

This is a temporary place to track TODOs for this project, until it's at the stage that it can be integrated into BonsaiDb.

- [x] Refactor to const generics for B-Tree order
- [x] Tree get for Interior
- [x] Tree insert for Interior
- [x] Root split handling
- [x] Chunk caching
- [x] Consider supporting dynamic order

      I don't know the right algorith, but since we know the number of entries in each tree, we can slowly increase order rather than starting at our max order. The reason this is a good idea for this format is that the larger the nodes are, the more data is written out on each change. While pointer nodes are significantly smaller, they still contain keys which can be dynamic in length. For small keys, dynamic order probably will have no impact. But for larger key sizes, I believe it will be important to try to keep the order smaller until we reach a maximum order -- then we allow the tree to grow in depth.

      Trying to think of another way leads me to wonder if CouchDB doesn't actually use traditional order, but rather gives each tree a quota of storage. If a node gets too large by the definition of number of bytes, it splits itself. This implementation changes the idea of what the "order" of a B-Tree is, but it fundamentally wouldn't change the operation of the tree as far as I can reason.
- [ ] Ability to insert multiple records in one write.
- [ ] Ability to scan ranges of keys
- [ ] Benchmarks
- [ ] Ability to sequentially scan and reverse-scan transaction log.
- [ ] Ability to find an entry in the transaction log, using a binary search
- [ ] File versioning
- [ ] Consider a WAL

      In testing SQLite, they have a WAL which is used to batch changes into thir database. It slows down queries, but it allows inserts to be incredibly fast. Originally, I threw out this idea because it seemed like it might require too much memory usage.

      But, I now realize we can contain the memory usage. In the chunk cacher, we can keep a separate list of cached chunks that are required to stay in memory until they're committed from the WAL. We can force this list to be a fixed size to keep memory usage down -- thus forcing a wait for the WAL to commit. Once the WAL has been committed, those chunks can be moved into the chunk cacher.

      Why could this be faster? The WAL can contain just the bare minimum to re-execute the modification. This means less writes need to be flushed to disk to say that the transaction is completed. For flows that are doing small transactions, we could do fake transactions in memory that create temporary nodes that only live in the chunk cache. Then, when the WAL is committed, we do one large write to the database with all the changes. The only reason this might win is by minimizing the total number of node rewrites by chunking the btree operations into one batch.