use std::sync::Arc;

use tokio::sync::{Mutex, MutexGuard};

use super::TreeRoot;

const UNINITIALIZED_SEQUENCE: u64 = 0;

#[derive(Debug, Default, Clone)]
pub struct State {
    state: Arc<Mutex<ActiveState>>,
}

impl State {
    pub async fn lock(&self) -> MutexGuard<'_, ActiveState> {
        self.state.lock().await
    }

    // pub fn next_sequence(&self) -> u64 {
    //     self.state.next_sequence.load(Ordering::SeqCst)
    // }
}

#[derive(Debug, Default)]
pub struct ActiveState {
    pub current_position: u64,
    pub header: TreeRoot<'static>,
}

impl ActiveState {
    pub const fn initialized(&self) -> bool {
        self.header.sequence != UNINITIALIZED_SEQUENCE
    }

    pub fn initialize(&mut self, file_length: u64, header: TreeRoot<'static>) {
        assert_eq!(
            self.header.sequence, UNINITIALIZED_SEQUENCE,
            "state already initialized"
        );
        self.current_position = file_length;
        self.header = header;
    }
}
