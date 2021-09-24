use persy::{ByteVec, Config, Persy, ValueMode};

use super::{InsertConfig, LogEntry, LogEntryBatchGenerator, ReadConfig, ReadState};
use crate::{BenchConfig, SimpleBench};

pub struct InsertLogs {
    // _tempfile: NamedTempFile,
    db: persy::Persy,
    state: LogEntryBatchGenerator,
}

impl SimpleBench for InsertLogs {
    type GroupState = ();
    type Config = InsertConfig;
    const BACKEND: &'static str = "Persy";

    fn initialize_group(
        _config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
    }

    fn initialize(
        _group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        let _ = std::fs::remove_file("/tmp/persy");
        let db = Persy::open_or_create_with("/tmp/persy", Config::default(), |persy| {
            let mut tx = persy.begin()?;
            tx.create_index::<u64, ByteVec>("index", ValueMode::Replace)?;
            let prepared = tx.prepare()?;
            prepared.commit()?;
            Ok(())
        })
        .unwrap();

        Ok(Self {
            // _tempfile: tempfile,
            db,
            state: config.initialize(config_group_state),
        })
    }

    fn execute_measured(&mut self, _config: &Self::Config) -> Result<(), anyhow::Error> {
        let batch = self.state.next().unwrap();
        let mut tx = self.db.begin()?;

        for entry in &batch {
            tx.put("index", entry.id, ByteVec::new(pot::to_vec(&entry)?))
                .unwrap();
        }
        let prepared = tx.prepare().unwrap();
        prepared.commit().unwrap();
        Ok(())
    }
}

pub struct ReadLogs {
    db: Persy,
    state: ReadState,
}

impl SimpleBench for ReadLogs {
    type GroupState = ();
    type Config = ReadConfig;
    const BACKEND: &'static str = "Persy";

    fn initialize_group(
        config: &Self::Config,
        _group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Self::GroupState {
        let _ = std::fs::remove_file("/tmp/persy");
        let db = Persy::open_or_create_with("/tmp/persy", Config::default(), |persy| {
            let mut tx = persy.begin()?;
            tx.create_index::<u64, ByteVec>("index", ValueMode::Replace)?;
            let prepared = tx.prepare()?;
            prepared.commit()?;
            Ok(())
        })
        .unwrap();

        config.for_each_database_chunk(100_000, |chunk| {
            let mut tx = db.begin().unwrap();
            for entry in chunk {
                tx.put(
                    "index",
                    entry.id,
                    ByteVec::new(pot::to_vec(&entry).unwrap()),
                )
                .unwrap();
            }
            let prepared = tx.prepare().unwrap();
            prepared.commit().unwrap();
        });
    }

    fn initialize(
        _group_state: &Self::GroupState,
        config: &Self::Config,
        config_group_state: &<Self::Config as BenchConfig>::GroupState,
    ) -> Result<Self, anyhow::Error> {
        let db = Persy::open("/tmp/persy", Config::default()).unwrap();
        let state = config.initialize(config_group_state);
        Ok(Self { db, state })
    }

    fn execute_measured(&mut self, config: &Self::Config) -> Result<(), anyhow::Error> {
        // To be fair, we're only evaluating that content equals when it's a single get
        if config.get_count == 1 {
            let entry = self.state.next().unwrap();
            let bytes: ByteVec = self
                .db
                .get("index", &entry.id)?
                .next()
                .expect("value not found");
            let decoded = pot::from_slice::<LogEntry>(&bytes)?;
            assert_eq!(&decoded, &entry);
        } else {
            for _ in 0..config.get_count {
                let entry = self.state.next().unwrap();
                self.db
                    .get::<_, ByteVec>("index", &entry.id)?
                    .next()
                    .expect("value not found");
            }
        }
        Ok(())
    }
}
