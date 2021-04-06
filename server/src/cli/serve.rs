use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use structopt::StructOpt;

use crate::Server;

/// Execute the server
#[derive(StructOpt, Debug)]
pub struct Serve {
    /// The bind address and port. Defaults to 127.0.0.1:5000
    // TODO actually pick a port
    #[structopt(short = "l", long = "listen-on")]
    pub listen_on: Option<SocketAddr>,
}

impl Serve {
    /// Starts the server.
    pub async fn execute(&self, server: Server) -> anyhow::Result<()> {
        let listen_on = self
            .listen_on
            .unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5000));

        server.listen_on(listen_on).await?;

        Ok(())
    }
}
