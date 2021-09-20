use std::{
    borrow::Cow,
    collections::HashMap,
    fs,
    marker::PhantomData,
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::Mutex;

use crate::{
    context::Context,
    transaction::{TransactionHandle, TransactionManager},
    tree::{Modification, Operation, State, TreeFile},
    Buffer, ChunkCache, Error, ManagedFile, Vault,
};

const MAX_ORDER: usize = 1000;

/// A multi-tree transactional B-Tree database.
pub struct Roots<F: ManagedFile> {
    data: Arc<Data<F>>,
}

struct Data<F: ManagedFile> {
    context: Context<F::Manager>,
    transactions: TransactionManager,
    path: PathBuf,
    tree_states: Mutex<HashMap<String, State<MAX_ORDER>>>,
    _file: PhantomData<F>,
}

impl<F: ManagedFile> Roots<F> {
    fn open<P: Into<PathBuf> + Send>(path: P, context: Context<F::Manager>) -> Result<Self, Error> {
        let path = path.into();
        if !path.exists() {
            fs::create_dir(&path)?;
        } else if !path.is_dir() {
            return Err(Error::message(format!(
                "'{:?}' already exists, but is not a directory.",
                path
            )));
        }

        let transactions = TransactionManager::spawn::<F>(&path, context.clone())?;
        Ok(Self {
            data: Arc::new(Data {
                context,
                path,
                transactions,
                tree_states: Mutex::default(),
                _file: PhantomData,
            }),
        })
    }

    /// Returns the path to the database directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.data.path
    }

    /// Returns the vault used to encrypt this database.
    #[must_use]
    pub fn context(&self) -> &Context<F::Manager> {
        &self.data.context
    }

    /// Opens a tree named `name`.
    // TODO enforce name restrictions.
    pub fn tree(&self, name: impl Into<Cow<'static, str>>) -> Tree<F> {
        let name = name.into();
        let state = self.tree_state(&name);
        Tree {
            roots: self.clone(),
            state,
            name,
        }
    }

    fn tree_state(&self, name: &str) -> State<MAX_ORDER> {
        self.tree_states([name].iter().copied())
            .into_iter()
            .next()
            .unwrap()
    }

    fn tree_states<'i, Names: Iterator<Item = &'i str> + ExactSizeIterator>(
        &self,
        names: Names,
    ) -> Vec<State<MAX_ORDER>> {
        let mut tree_states = self.data.tree_states.lock();
        let mut output = Vec::with_capacity(names.len());
        for name in names {
            let state = tree_states
                .entry(name.to_string())
                .or_insert_with(State::default)
                .clone();
            output.push(state);
        }
        output
    }

    /// Begins a transaction over `trees`. All trees will be exclusively
    /// accessible by the transaction. Dropping the executing transaction will
    /// roll the transaction back.
    pub fn transaction(&self, trees: &[&str]) -> Result<ExecutingTransaction<F>, Error> {
        // TODO this extra vec here is annoying. We should have a treename type
        // that we can use instead of str.
        let transaction = self
            .data
            .transactions
            .new_transaction(&trees.iter().map(|t| t.as_bytes()).collect::<Vec<_>>());
        let states = self.tree_states(trees.iter().copied());
        let trees = trees
            .iter()
            .zip(states.into_iter())
            .map(|(tree, state)| {
                let tree = TreeFile::write(self.path().join(tree), state, self.context())?;

                Ok(TransactionTree {
                    tree,
                    transaction_id: transaction.id,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(ExecutingTransaction {
            transaction: Some(transaction),
            trees,
            transaction_manager: self.data.transactions.clone(),
        })
    }
}

impl<M: ManagedFile> Clone for Roots<M> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

#[must_use]
pub struct ExecutingTransaction<F: ManagedFile> {
    transaction_manager: TransactionManager,
    transaction: Option<TransactionHandle>,
    trees: Vec<TransactionTree<F>>,
}

impl<F: ManagedFile> ExecutingTransaction<F> {
    pub fn commit(mut self) -> Result<(), Error> {
        self.transaction_manager
            .push(self.transaction.take().unwrap())?;

        for tree in self.trees.drain(..) {
            let state = tree.tree.state.lock();
            state.publish(&tree.tree.state);
        }

        Ok(())
    }

    pub fn tree(&mut self, index: usize) -> Option<&mut TransactionTree<F>> {
        self.trees.get_mut(index)
    }

    fn rollback_tree_states(&mut self) {
        for tree in self.trees.drain(..) {
            let mut state = tree.tree.state.lock();
            state.rollback(&tree.tree.state);
        }
    }
}

impl<F: ManagedFile> Drop for ExecutingTransaction<F> {
    fn drop(&mut self) {
        if let Some(transaction) = self.transaction.take() {
            self.rollback_tree_states();
            // Now the transaction can be dropped safely, freeing up access to the trees.
            drop(transaction);
        }
    }
}

pub struct TransactionTree<F: ManagedFile> {
    transaction_id: u64,
    tree: TreeFile<F, MAX_ORDER>,
}

impl<F: ManagedFile> TransactionTree<F> {
    pub fn set(
        &mut self,
        key: impl Into<Buffer<'static>>,
        value: impl Into<Buffer<'static>>,
    ) -> Result<(), Error> {
        self.tree
            .modify(Modification {
                transaction_id: self.transaction_id,
                keys: vec![key.into()],
                operation: Operation::Set(value.into()),
            })
            .map(|_| {})
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Buffer<'static>>, Error> {
        self.tree.get(key)
    }

    pub fn get_multiple(&mut self, keys: &[&[u8]]) -> Result<Vec<Buffer<'static>>, Error> {
        self.tree.get_multiple(keys)
    }

    pub fn scan<'b, B: RangeBounds<Buffer<'b>> + std::fmt::Debug + 'static>(
        &mut self,
        range: B,
    ) -> Result<Vec<Buffer<'static>>, Error> {
        self.tree.scan(range)
    }
}

/// A database configuration used to open a database.
#[derive(Debug)]
#[must_use]
pub struct Config<F: ManagedFile> {
    path: PathBuf,
    vault: Option<Arc<dyn Vault>>,
    cache: Option<ChunkCache>,
    _file: PhantomData<F>,
}

impl<F: ManagedFile> Config<F> {
    /// Creates a new config to open a database located at `path`.
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            vault: None,
            cache: None,
            _file: PhantomData,
        }
    }

    /// Sets the vault to use for this database.
    pub fn vault<V: Vault>(mut self, vault: V) -> Self {
        self.vault = Some(Arc::new(vault));
        self
    }

    /// Sets the chunk cache to use for this database.
    pub fn cache(mut self, cache: ChunkCache) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Opens the database, or creates one if the target path doesn't exist.
    pub fn open(self) -> Result<Roots<F>, Error> {
        Roots::open(
            self.path,
            Context {
                file_manager: F::Manager::default(),
                vault: self.vault,
                cache: self.cache,
            },
        )
    }
}

pub struct Tree<F: ManagedFile> {
    roots: Roots<F>,
    state: State<MAX_ORDER>,
    name: Cow<'static, str>,
}

impl<F: ManagedFile> Tree<F> {
    pub fn set(
        &self,
        key: impl Into<Buffer<'static>>,
        value: impl Into<Buffer<'static>>,
    ) -> Result<(), Error> {
        let mut tree = TreeFile::<F, MAX_ORDER>::write(
            self.roots.path().join(self.name.as_ref()),
            self.state.clone(),
            self.roots.context(),
        )?;

        tree.modify(Modification {
            transaction_id: 0,
            keys: vec![key.into()],
            operation: Operation::Set(value.into()),
        })
        .map(|_| {})
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Buffer<'static>>, Error> {
        let mut tree = TreeFile::<F, MAX_ORDER>::read(
            self.roots.path().join(self.name.as_ref()),
            self.state.clone(),
            self.roots.context(),
        )?;

        tree.get(key)
    }

    pub fn get_multiple(&self, keys: &[&[u8]]) -> Result<Vec<Buffer<'static>>, Error> {
        let mut tree = TreeFile::<F, MAX_ORDER>::read(
            self.roots.path().join(self.name.as_ref()),
            self.state.clone(),
            self.roots.context(),
        )?;

        tree.get_multiple(keys)
    }

    pub fn scan<'b, B: RangeBounds<Buffer<'b>> + std::fmt::Debug + 'static>(
        &self,
        range: B,
    ) -> Result<Vec<Buffer<'static>>, Error> {
        let mut tree = TreeFile::<F, MAX_ORDER>::read(
            self.roots.path().join(self.name.as_ref()),
            self.state.clone(),
            self.roots.context(),
        )?;

        tree.scan(range)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::{managed_file::memory::MemoryFile, ManagedFile, StdFile};

    fn basic_get_set<F: ManagedFile>() {
        let tempdir = tempdir().unwrap();
        let roots = Config::<F>::new(tempdir.path()).open().unwrap();

        let tree = roots.tree("test");
        tree.set(b"test", b"value").unwrap();
        let result = tree.get(b"test").unwrap().expect("key not found");

        assert_eq!(result.as_slice(), b"value");
    }

    #[test]
    fn memory_basic_get_set() {
        basic_get_set::<MemoryFile>();
    }

    #[test]
    fn std_basic_get_set() {
        basic_get_set::<StdFile>();
    }

    #[test]
    fn basic_transaction_isolation_test() {
        let tempdir = tempdir().unwrap();

        let roots = Config::<StdFile>::new(tempdir.path()).open().unwrap();
        let tree = roots.tree("test");
        tree.set(b"test", b"value").unwrap();

        // Begin a transaction
        let mut transaction = roots.transaction(&["test"]).unwrap();

        // Replace the key with a new value.
        transaction
            .tree(0)
            .unwrap()
            .set(b"test", b"updated value")
            .unwrap();

        // Check that the transaction can read the new value
        let result = transaction
            .tree(0)
            .unwrap()
            .get(b"test")
            .unwrap()
            .expect("key not found");
        assert_eq!(result.as_slice(), b"updated value");

        // Ensure that existing read-access doesn't see the new value
        let result = tree.get(b"test").unwrap().expect("key not found");
        assert_eq!(result.as_slice(), b"value");

        // Commit the transaction
        transaction.commit().unwrap();

        // Ensure that the reader now sees the new value
        let result = tree.get(b"test").unwrap().expect("key not found");
        assert_eq!(result.as_slice(), b"updated value");
    }

    #[test]
    fn basic_transaction_rollback_test() {
        let tempdir = tempdir().unwrap();

        let roots = Config::<StdFile>::new(tempdir.path()).open().unwrap();
        let tree = roots.tree("test");
        tree.set(b"test", b"value").unwrap();

        // Begin a transaction
        let mut transaction = roots.transaction(&["test"]).unwrap();

        // Replace the key with a new value.
        transaction
            .tree(0)
            .unwrap()
            .set(b"test", b"updated value")
            .unwrap();

        // Roll the transaction back
        drop(transaction);

        // Ensure that the reader still sees the old value
        let result = tree.get(b"test").unwrap().expect("key not found");
        assert_eq!(result.as_slice(), b"value");

        // Begin a new transaction
        let mut transaction = roots.transaction(&["test"]).unwrap();
        // Check that the transaction has the original value
        let result = transaction
            .tree(0)
            .unwrap()
            .get(b"test")
            .unwrap()
            .expect("key not found");
        assert_eq!(result.as_slice(), b"value");
    }
}
