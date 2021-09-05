pub trait Vault: Send + Sync + 'static {
    fn current_key_id(&self) -> u32;
    fn encrypt(&self, payload: &[u8]) -> Vec<u8>;
    fn decrypt(&self, payload: &[u8]) -> Vec<u8>;
}
