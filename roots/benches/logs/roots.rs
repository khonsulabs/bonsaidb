use bonsaidb_roots::{
    tree::{Modification, Operation, State, TreeFile},
    Buffer, ChunkCache, FileManager, ManagedFile,
};
use tempfile::TempDir;

use super::LogConfig;
use crate::{BenchConfig, SimpleBench};

pub struct RootsLogs<F: ManagedFile> {
    _tempfile: TempDir,
    tree: TreeFile<F, 1_000>,
}

impl<F: ManagedFile> SimpleBench for RootsLogs<F> {
    type Config = LogConfig;

    fn initialize(_config: &Self::Config) -> Result<Self, anyhow::Error> {
        let tempfile = TempDir::new()?;
        let manager = <F::Manager as Default>::default();
        let file = manager.append(tempfile.path().join("tree"))?;
        let tree = TreeFile::<F, 1_000>::new(
            file,
            State::default(),
            None,
            Some(ChunkCache::new(100, 160_384)),
        )?;
        Ok(Self {
            _tempfile: tempfile,
            tree,
        })
    }

    fn execute_measured(
        &mut self,
        batch: &<Self::Config as BenchConfig>::Batch,
        _config: &Self::Config,
    ) -> Result<(), anyhow::Error> {
        // While it might be tempting to move serialization out of the measured
        // function, that isn't fair to sql databases which necessarily require
        // encoding the data at least once before saving. While we could pick a
        // faster serialization framework, the goal of our benchmarks aren't to
        // reach maximum speed at all costs: it's to have realistic scenarios
        // measured, and in BonsaiDb, the storage format is going to be `pot`.
        self.tree.modify(Modification {
            transaction_id: 0,
            keys: batch
                .iter()
                .map(|e| Buffer::from(e.id.to_be_bytes()))
                .collect(),
            operation: Operation::SetEach(
                batch
                    .iter()
                    .map(|e| Buffer::from(pot::to_vec(e).unwrap()))
                    .collect(),
            ),
        })?;
        Ok(())
    }
}
