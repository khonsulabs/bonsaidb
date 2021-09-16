use rusqlite::{params, Connection};
use tempfile::NamedTempFile;

use super::{InsertConfig, LogEntry, ReadConfig};
use crate::{BenchConfig, SimpleBench};

pub struct InsertLogs {
    sqlite: Connection,
    _tempfile: NamedTempFile,
}

impl SimpleBench for InsertLogs {
    type Config = InsertConfig;

    fn name(config: &Self::Config) -> String {
        format!(
            "logs-insert-{}x{}-sqlite",
            config.transactions, config.entries_per_transaction
        )
    }

    fn initialize(_config: &Self::Config) -> Result<Self, anyhow::Error> {
        // For fair testing, this needs to use ACID-compliant settings that a
        // user would use in production. While a WAL might be used in
        // production, it alters more than just insert performance. A more
        // complete benchmark which includes both inserts and queries would be
        // better to compare roots against sqlite's WAL performance.
        let tempfile = NamedTempFile::new()?;
        let sqlite = Connection::open(tempfile.path())?;
        // Sets the journal to what seems to be the most optimal, safe setting
        // for @ecton. See:
        // https://www.sqlite.org/pragma.html#pragma_journal_mode
        sqlite.pragma_update(None, "journal_mode", &"TRUNCATE")?;
        // Sets synchronous to NORMAL, which "should" be safe and provides
        // better performance. See:
        // https://www.sqlite.org/pragma.html#pragma_synchronous
        sqlite.pragma_update(None, "synchronous", &"NORMAL")?;
        sqlite.execute(
            "create table logs (id integer primary key, timestamp integer, message text)",
            [],
        )?;
        Ok(Self {
            sqlite,
            _tempfile: tempfile,
        })
    }

    fn execute_measured(
        &mut self,
        batch: &<Self::Config as BenchConfig>::Batch,
        _config: &Self::Config,
    ) -> Result<(), anyhow::Error> {
        self.sqlite.execute("begin transaction;", [])?;

        let mut prepared = self
            .sqlite
            .prepare("insert into logs (id, timestamp, message) values (?, ?, ?)")?;
        for log in batch {
            // sqlite doesn't support u64, so we're going to cast to i64
            prepared.execute(params![log.id as i64, log.timestamp as i64, log.message])?;
        }

        self.sqlite.execute("commit transaction;", [])?;

        Ok(())
    }
}

pub struct ReadLogs {
    sqlite: Connection,
    _tempfile: NamedTempFile,
}

impl SimpleBench for ReadLogs {
    type Config = ReadConfig;

    fn name(config: &Self::Config) -> String {
        format!("logs-read-{}-sqlite", config.database_size)
    }

    fn initialize(config: &Self::Config) -> Result<Self, anyhow::Error> {
        let tempfile = NamedTempFile::new()?;
        let sqlite = Connection::open(tempfile.path())?;
        sqlite.execute(
            "create table logs (id integer primary key, timestamp integer, message text)",
            [],
        )?;

        let mut prepared =
            sqlite.prepare("insert into logs (id, timestamp, message) values (?, ?, ?)")?;
        for chunk in config.database.chunks(1_000_000) {
            sqlite.execute("begin transaction;", [])?;
            for log in chunk {
                // sqlite doesn't support u64, so we're going to cast to i64
                prepared.execute(params![log.id as i64, log.timestamp as i64, log.message])?;
            }

            sqlite.execute("commit transaction;", [])?;
        }
        drop(prepared);
        Ok(Self {
            sqlite,
            _tempfile: tempfile,
        })
    }

    fn execute_measured(
        &mut self,
        batch: &<Self::Config as BenchConfig>::Batch,
        _config: &Self::Config,
    ) -> Result<(), anyhow::Error> {
        for entry in batch {
            let fetched = self.sqlite.query_row(
                "SELECT id, timestamp, message FROM logs WHERE id = ?",
                [entry.id as i64],
                |row| {
                    Ok(LogEntry {
                        id: row.get::<_, i64>(0)? as u64,
                        timestamp: row.get::<_, i64>(1)? as u64,
                        message: row.get(2)?,
                    })
                },
            )?;
            assert_eq!(&fetched, entry);
        }
        Ok(())
    }
}
