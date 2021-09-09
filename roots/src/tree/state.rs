use std::sync::Arc;

use tokio::sync::{Mutex, MutexGuard};

use super::TreeRoot;

const UNINITIALIZED_SEQUENCE: u64 = 0;

#[derive(Debug, Default, Clone)]
pub struct State<const MAX_ORDER: usize> {
    state: Arc<Mutex<ActiveState<MAX_ORDER>>>,
}

impl<const MAX_ORDER: usize> State<MAX_ORDER> {
    pub async fn lock(&self) -> MutexGuard<'_, ActiveState<MAX_ORDER>> {
        self.state.lock().await
    }

    // pub fn next_sequence(&self) -> u64 {
    //     self.state.next_sequence.load(Ordering::SeqCst)
    // }
}

#[derive(Debug, Default)]
pub struct ActiveState<const MAX_ORDER: usize> {
    pub current_position: u64,
    pub header: TreeRoot<MAX_ORDER>,
}

impl<const MAX_ORDER: usize> ActiveState<MAX_ORDER> {
    pub const fn initialized(&self) -> bool {
        self.header.sequence != UNINITIALIZED_SEQUENCE
    }

    pub fn initialize(&mut self, file_length: u64, header: TreeRoot<MAX_ORDER>) {
        assert_eq!(
            self.header.sequence, UNINITIALIZED_SEQUENCE,
            "state already initialized"
        );
        self.current_position = file_length;
        self.header = header;
    }
}
