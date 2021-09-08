# Todos

This is a temporary place to track TODOs for this project, until it's at the stage that it can be integrated into BonsaiDb.

- [x] Refactor to const generics for B-Tree order
- [ ] Tree insert for Interior
- [ ] Root split handling
- [ ] Ability to scan ranges of keys
- [ ] Ability to sequentially scan and reverse-scan transaction log.
- [ ] Ability to find an entry in the transaction log, using a binary search
- [ ] Consider supporting dynamic order

      I don't know the right algorith, but since we know the number of entries in each tree, we can slowly increase order rather than starting at our max order. The reason this is a good idea for this format is that the larger the nodes are, the more data is written out on each change. While pointer nodes are significantly smaller, they still contain keys which can be dynamic in length. For small keys, dynamic order probably will have no impact. But for larger key sizes, I believe it will be important to try to keep the order smaller until we reach a maximum order -- then we allow the tree to grow in depth.

      Trying to think of another way leads me to wonder if CouchDB doesn't actually use traditional order, but rather gives each tree a quota of storage. If a node gets too large by the definition of number of bytes, it splits itself. This implementation changes the idea of what the "order" of a B-Tree is, but it fundamentally wouldn't change the operation of the tree as far as I can reason.