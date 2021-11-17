use std::{marker::PhantomData, net::SocketAddr};

use structopt::StructOpt;

use crate::{Backend, CustomServer, Error};

/// Execute the server
#[derive(StructOpt, Debug)]
pub struct Serve<B: Backend> {
    /// The port for the BonsaiDb protocol. Defaults to 5645
    #[structopt(short = "l", long = "listen-on")]
    pub listen_on: Option<u16>,

    #[cfg(feature = "websockets")]
    /// The bind port and address for HTTP traffic. Defaults to 80.
    #[structopt(long = "http")]
    pub http_port: Option<SocketAddr>,

    #[cfg(feature = "websockets")]
    /// The bind port and address for HTTPS traffic. Defaults to 443.
    #[structopt(long = "https")]
    pub https_port: Option<SocketAddr>,

    #[structopt(skip)]
    _backend: PhantomData<B>,
}

impl<B: Backend> Serve<B> {
    /// Starts the server.
    pub async fn execute(&self, server: CustomServer<B>) -> Result<(), Error> {
        // Try to initialize a logger, but ignore it if it fails. This API is
        // public and another logger may already be installed.
        drop(env_logger::try_init());
        let listen_on = self.listen_on.unwrap_or(5645);

        let task_server = server.clone();
        tokio::task::spawn(async move { task_server.listen_on(listen_on).await });

        #[cfg(feature = "websockets")]
        {
            if let Some(http_port) = self.http_port {
                let task_server = server.clone();
                tokio::task::spawn(async move {
                    task_server.listen_for_websockets_on(http_port, false).await
                });
            }

            if let Some(https_port) = self.https_port {
                let task_server = server.clone();
                tokio::task::spawn(async move {
                    task_server.listen_for_websockets_on(https_port, true).await
                });
            }
        }

        server.listen_for_shutdown().await?;

        Ok(())
    }
}
