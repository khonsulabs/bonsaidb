use std::path::PathBuf;

use clap::Subcommand;

use crate::{config::StorageConfiguration, Error, Storage};

/// Commands for administering the bonsaidb server.
pub mod admin;

/// Commands operating on local database storage.
#[derive(Subcommand, Debug)]
pub enum StorageCommand {
    /// Back up the storage.
    #[clap(subcommand)]
    Backup(Location),
    /// Restore the storage from backup.
    #[clap(subcommand)]
    Restore(Location),
    /// Executes an admin command.
    #[clap(subcommand)]
    Admin(admin::Command),
}

/// A backup location.
#[derive(Subcommand, Debug)]
pub enum Location {
    /// A filesystem-based backup location.
    Path {
        /// The path to the backup directory.
        path: PathBuf,
    },
}

impl StorageCommand {
    /// Executes the command after opening a [`Storage`] instance using `config`.
    pub fn execute(self, config: StorageConfiguration) -> Result<(), Error> {
        let storage = Storage::open(config)?;
        self.execute_on(&storage)
    }

    /// Executes the command on `storage`.
    pub fn execute_on(self, storage: &Storage) -> Result<(), Error> {
        match self {
            StorageCommand::Backup(location) => location.backup(storage),
            StorageCommand::Restore(location) => location.restore(storage),
            StorageCommand::Admin(admin) => admin.execute(storage),
        }
    }

    /// Executes the command on `storage`.
    #[cfg(feature = "async")]
    pub async fn execute_on_async(self, storage: &crate::AsyncStorage) -> Result<(), Error> {
        match self {
            StorageCommand::Backup(location) => location.backup_async(storage).await,
            StorageCommand::Restore(location) => location.restore_async(storage).await,
            StorageCommand::Admin(admin) => admin.execute_async(storage).await,
        }
    }
}

impl Location {
    /// Backs-up `storage` to `self`.
    pub fn backup(&self, storage: &Storage) -> Result<(), Error> {
        match self {
            Location::Path { path } => storage.backup(path),
        }
    }

    /// Restores `storage` from `self`.
    pub fn restore(&self, storage: &Storage) -> Result<(), Error> {
        match self {
            Location::Path { path } => storage.restore(path),
        }
    }
    /// Backs-up `storage` to `self`.
    #[cfg(feature = "async")]
    pub async fn backup_async(&self, storage: &crate::AsyncStorage) -> Result<(), Error> {
        match self {
            Location::Path { path } => storage.backup(path.clone()).await,
        }
    }

    /// Restores `storage` from `self`.
    #[cfg(feature = "async")]
    pub async fn restore_async(&self, storage: &crate::AsyncStorage) -> Result<(), Error> {
        match self {
            Location::Path { path } => storage.restore(path.clone()).await,
        }
    }
}

/// Reads a password from stdin, wrapping the result in a
/// [`SensitiveString`](bonsaidb_core::connection::SensitiveString). If
/// `confirm` is true, the user will be prompted to enter the password a second
/// time, and the passwords will be compared to ensure they are the same before
/// returning.
#[cfg(feature = "password-hashing")]
pub fn read_password_from_stdin(
    confirm: bool,
) -> Result<bonsaidb_core::connection::SensitiveString, ReadPasswordError> {
    let password = read_sensitive_input_from_stdin("Enter Password:")?;
    if confirm {
        let confirmed = read_sensitive_input_from_stdin("Re-enter the same password:")?;
        if password != confirmed {
            return Err(ReadPasswordError::PasswordConfirmationFailed);
        }
    }
    Ok(password)
}

/// An error that may occur from reading a password from the terminal.
#[cfg(feature = "password-hashing")]
#[derive(thiserror::Error, Debug)]
pub enum ReadPasswordError {
    /// The password input was cancelled.
    #[error("password input cancelled")]
    Cancelled,
    /// The confirmation password did not match the originally entered password.
    #[error("password confirmation did not match")]
    PasswordConfirmationFailed,
    /// An error occurred interacting with the terminal.
    #[error("terminal error: {0}")]
    Terminal(#[from] crossterm::ErrorKind),
}

#[cfg(feature = "password-hashing")]
fn read_sensitive_input_from_stdin(
    prompt: &str,
) -> Result<bonsaidb_core::connection::SensitiveString, ReadPasswordError> {
    use std::io::stdout;

    use crossterm::{
        cursor::MoveToColumn,
        terminal::{Clear, ClearType},
        ExecutableCommand,
    };

    println!("{prompt} (input Enter or Return when done, or Escape to cancel)");

    crossterm::terminal::enable_raw_mode()?;
    let password = read_password_loop();
    drop(
        stdout()
            .execute(MoveToColumn(1))?
            .execute(Clear(ClearType::CurrentLine)),
    );
    crossterm::terminal::disable_raw_mode()?;
    if let Some(password) = password? {
        println!("********");
        Ok(password)
    } else {
        Err(ReadPasswordError::Cancelled)
    }
}

#[cfg(feature = "password-hashing")]
fn read_password_loop(
) -> Result<Option<bonsaidb_core::connection::SensitiveString>, crossterm::ErrorKind> {
    const ESCAPE: u8 = 27;
    const BACKSPACE: u8 = 127;
    const CANCEL: u8 = 3;
    const EOF: u8 = 4;
    const CLEAR_BEFORE_CURSOR: u8 = 21;

    use std::io::Read;

    use crossterm::{
        cursor::{MoveLeft, MoveToColumn},
        style::Print,
        terminal::{Clear, ClearType},
        ExecutableCommand,
    };
    let mut stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut buffer = [0; 1];
    let mut password = bonsaidb_core::connection::SensitiveString::default();
    loop {
        if stdin.read(&mut buffer)? == 0 {
            return Ok(Some(password));
        }
        match buffer[0] {
            ESCAPE | CANCEL => return Ok(None),
            BACKSPACE => {
                password.pop();
                stdout
                    .execute(MoveLeft(1))?
                    .execute(Clear(ClearType::UntilNewLine))?;
            }
            CLEAR_BEFORE_CURSOR => {
                password.clear();
                stdout
                    .execute(MoveToColumn(1))?
                    .execute(Clear(ClearType::CurrentLine))?;
            }
            b'\n' | b'\r' | EOF => {
                return Ok(Some(password));
            }
            other if other.is_ascii_control() => {}
            other => {
                password.push(other as char);
                stdout.execute(Print('*'))?;
            }
        }
    }
}
