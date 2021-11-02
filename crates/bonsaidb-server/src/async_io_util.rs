use async_trait::async_trait;
use tokio::{
    fs,
    io::{self, AsyncReadExt, AsyncWriteExt},
};

#[async_trait]
pub trait FileExt {
    async fn read_all(self) -> Result<Vec<u8>, io::Error>;
    async fn write_all(self, data: &[u8]) -> Result<(), io::Error>;
}

#[async_trait]
impl FileExt for fs::File {
    async fn read_all(mut self) -> Result<Vec<u8>, io::Error> {
        let mut buffer = Vec::new();
        self.read_to_end(&mut buffer).await?;
        Ok(buffer)
    }

    async fn write_all(mut self, data: &[u8]) -> Result<(), io::Error> {
        (&mut self).write_all(data).await?;
        self.shutdown().await?;
        Ok(())
    }
}
