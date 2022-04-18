//! Limits used within BonsaiDb.
//!
//! Unless otherwise noted, all size limits will be affected by compression, if
//! enabled.
//!
//! # Transaction Limits
//!
//! The serialized summary of [`Changes`](crate::transaction::Changes) of a
//! transaction must be less than 16 megabytes. This limit comes from
//! [Nebari][nebari]'s transaction log entry size limit.
//!
//! When querying previously executed transactions using
//! [`Connection::list_executed_transactions()`](crate::connection::Connection::list_executed_transactions),
//! the result set will be limited to [`LIST_TRANSACTIONS_MAX_RESULTS`] entries.
//!
//! # Document Limits
//!
//! ## Primary Key Limits
//!
//! [`DocumentId::MAX_LENGTH`](crate::document::DocumentId::MAX_LENGTH) is the
//! maximum number of bytes a document ID can contain when in its serialized
//! form. This is currently 64 kilobytes of data.
//!
//! ## Size Limits
//!
//! Each document can be up to 4 gigabytes in size (4,294,967,296 bytes).
//! However, care should be used when storing large documents, as the only way
//! to load a document is for its entire data to be read from disk into memory.
//! Storing large documents can lead to higher memory usage than you might
//! anticipate.
//!
//! Many NoSQL databases enforce document size limits at 16 megabytes or
//! smaller, which encourages usage patterns that require less RAM. While
//! BonsaiDb doesn't have as restrictive of a limit, users should consider
//! approaches that keep document sizes smaller if RAM is a constraint.
//!
//! # View Limits
//!
//! The serialized representation of the `Key` type must be less than 64
//! kilobytes (65,536 bytes). It should be noted that using large keys will slow
//! the view's performance. This is one reason why document IDs enforce a
//! smaller size limit. By not enforcing as restrictive of a limit on views,
//! more complex indexes can be built such as allowing tuples of strings of
//! arbitrary length.
//!
//! The serialized representation of all mappings emitted for a single `Key`
//! must be less than 4 gigabytes in size.
//!
//! [nebari]: https://github.com/khonsulabs/nebari

/// The maximum number of results allowed to be returned from `list_executed_transactions`.
pub const LIST_TRANSACTIONS_MAX_RESULTS: u32 = 1000;
/// If no `result_limit` is specified, this value is the limit used by default.
pub const LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT: u32 = 100;
