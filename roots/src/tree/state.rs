use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};

use super::BTreeRoot;

const UNINITIALIZED_SEQUENCE: u64 = 0;

/// The current state of a tree file. Must be initialized before passing to
/// `TreeFile::new` if the file already exists.
#[derive(Debug, Default, Clone)]
pub struct State<const MAX_ORDER: usize> {
    state: Arc<Mutex<ActiveState<MAX_ORDER>>>,
}

impl<const MAX_ORDER: usize> State<MAX_ORDER> {
    /// Locks the state.
    pub(crate) fn lock(&self) -> MutexGuard<'_, ActiveState<MAX_ORDER>> {
        self.state.lock()
    }

    // pub fn next_sequence(&self) -> u64 {
    //     self.state.next_sequence.load(Ordering::SeqCst)
    // }
}

#[derive(Debug, Default)]
pub struct ActiveState<const MAX_ORDER: usize> {
    pub current_position: u64,
    pub header: BTreeRoot<MAX_ORDER>,
}

impl<const MAX_ORDER: usize> ActiveState<MAX_ORDER> {
    pub const fn initialized(&self) -> bool {
        self.header.sequence != UNINITIALIZED_SEQUENCE
    }

    pub fn initialize(&mut self, file_length: u64, header: BTreeRoot<MAX_ORDER>) {
        assert_eq!(
            self.header.sequence, UNINITIALIZED_SEQUENCE,
            "state already initialized"
        );
        self.current_position = file_length;
        self.header = header;
    }
}
