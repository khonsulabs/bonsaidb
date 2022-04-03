use bonsaidb_core::key::time::TimestampAsSeconds;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Metadata {
    pub created_at: TimestampAsSeconds,
    pub last_updated_at: TimestampAsSeconds,
}
