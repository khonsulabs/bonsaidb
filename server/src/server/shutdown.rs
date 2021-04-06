use flume::{Receiver, Sender};

#[derive(Debug)]
pub struct Signal {
    sender: Sender<()>,
    receiver: Receiver<()>,
}

impl Signal {
    pub fn new() -> Self {
        let (sender, receiver) = flume::bounded(1);
        Self { sender, receiver }
    }
    pub async fn wait(&self) {
        let _ = self.receiver.recv_async().await;
    }

    pub fn shutdown(&self) {
        let _ = self.sender.try_send(());
    }
}
