//! Database command line tools.
//!
//! Available commands:
//!
//! - [`local-backup`](pliantdb_local::dump::Cli)
use structopt::StructOpt;

/// The command line interface for `pliantdb`.
#[derive(StructOpt, Debug)]
pub enum Command {
    /// Back up or restore a local database
    LocalBackup(pliantdb_local::dump::Cli),
}

impl Command {
    /// Executes the command.
    pub async fn execute(self) -> anyhow::Result<()> {
        let Self::LocalBackup(dump) = self;

        dump.command.execute(dump.database_path).await
    }
}
