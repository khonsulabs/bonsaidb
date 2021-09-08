use std::sync::Arc;

use crate::Vault;

/// A shared environment for database operations.
#[derive(Debug)]
pub struct Context<M> {
    /// The file manager for the [`AsyncFile`](crate::AsyncFile) implementor.
    pub file_manager: M,
    /// The optional vault in use.
    pub vault: Option<Arc<dyn Vault>>,
}

impl<M: Clone> Clone for Context<M> {
    fn clone(&self) -> Self {
        Self {
            file_manager: self.file_manager.clone(),
            vault: self.vault.clone(),
        }
    }
}

impl<M> Context<M> {
    /// Returns the vault as a dynamic reference.
    pub fn vault(&self) -> Option<&dyn Vault> {
        self.vault.as_deref()
    }
}
