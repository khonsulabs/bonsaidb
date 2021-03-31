//! `PliantDB` command line tools.
//!
//! Available commands:
//!
//! - [`local-backup`](pliantdb_local::backup)
use structopt::StructOpt;

/// The command line interface for `pliantdb`.
#[derive(StructOpt, Debug)]
pub enum Command {
    /// Back up or restore a local database
    LocalBackup(pliantdb_local::backup::Cli),
}

impl Command {
    /// Executes the command.
    pub async fn execute(self) -> anyhow::Result<()> {
        let Self::LocalBackup(backup) = self;

        backup.command.execute(backup.database_path).await
    }
}
