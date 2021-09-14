use async_trait::async_trait;
use bonsaidb_roots::{
    tree::{Modification, Operation, State, TreeFile},
    AsyncFile, AsyncFileManager, Buffer, ChunkCache,
};
use tempfile::TempDir;

use super::{LogConfig, LogEntry};
use crate::AsyncBench;

pub struct RootsLogs<F: AsyncFile> {
    _tempfile: TempDir,
    tree: TreeFile<F, 1_000>,
    logs: Vec<Vec<LogEntry>>,
}

#[async_trait(?Send)]
impl<F: AsyncFile> AsyncBench for RootsLogs<F> {
    type Config = LogConfig;

    async fn initialize(config: &Self::Config) -> Result<Self, anyhow::Error> {
        let tempfile = TempDir::new()?;
        let manager = <F::Manager as Default>::default();
        let file = manager.append(tempfile.path().join("tree")).await?;
        let tree = TreeFile::<F, 1_000>::new(
            file,
            State::default(),
            None,
            Some(ChunkCache::new(100, 160_384)),
        )
        .await?;
        let logs = LogEntry::generate(config);
        Ok(Self {
            _tempfile: tempfile,
            tree,
            logs,
        })
    }

    fn run(
        target: impl Into<String>,
        config: &Self::Config,
    ) -> Result<crate::BenchReport, anyhow::Error> {
        F::Manager::run(async {
            Self::initialize(config)
                .await?
                .execute_iterations(target, config)
                .await
        })
    }

    async fn execute_measured(&mut self, _config: &Self::Config) -> Result<(), anyhow::Error> {
        let entries = self.logs.pop().expect("ran out of logs");

        // While it might be tempting to move serialization out of the measured
        // function, that isn't fair to sql databases which necessarily require
        // encoding the data at least once before saving. While we could pick a
        // faster serialization framework, the goal of our benchmarks aren't to
        // reach maximum speed at all costs: it's to have realistic scenarios
        // measured, and in BonsaiDb, the storage format is going to be `pot`.
        // println!("Starting");
        self.tree
            .modify(Modification {
                transaction_id: 0,
                keys: entries
                    .iter()
                    .map(|e| Buffer::from(e.id.to_be_bytes()))
                    .collect(),
                operation: Operation::SetEach(
                    entries
                        .iter()
                        .map(|e| Buffer::from(pot::to_vec(e).unwrap()))
                        .collect(),
                ),
            })
            .await?;
        // println!("Done");
        Ok(())
    }
}
