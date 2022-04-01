use bonsaidb_core::key::time::TimestampAsSeconds;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Metadata {
    pub kind: Kind,
    pub created_at: TimestampAsSeconds,
    pub last_updated_at: TimestampAsSeconds,
    pub permissions: Option<Permissions>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Kind {
    File,
    Directory,
    Link { to: u32 },
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Permissions {
    pub owner: IdentityPermissions,
    pub group: IdentityPermissions,
    pub others: Abilities,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct IdentityPermissions {
    pub id: Option<u64>,
    pub abilities: Abilities,
}

// TODO switch to a bit mask?
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Abilities {
    pub create: bool,
    pub read: bool,
    pub append: bool,
    pub truncate: bool,
    pub delete: bool,
}
