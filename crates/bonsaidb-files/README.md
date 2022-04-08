# BonsaiDb File Storage

This crate provides support for storing large files in
[BonsaiDb](https://bonsaidb.io/). While BonsaiDb's document size limit is 4
gigabytes, the requirement that each document is loaded in memory fully can
cause higher memory usage when storing larger files.

This crate provides a set of collections that store files in smaller chunks and
provide access to the files contents using buffered and random access.
