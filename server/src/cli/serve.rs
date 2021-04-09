use structopt::StructOpt;

use crate::Server;

/// Execute the server
#[derive(StructOpt, Debug)]
pub struct Serve {
    /// The bind address and port. Defaults to 5000
    // TODO actually pick a port
    #[structopt(short = "l", long = "listen-on")]
    pub listen_on: Option<u16>,
}

impl Serve {
    /// Starts the server.
    pub async fn execute(&self, server: Server) -> anyhow::Result<()> {
        let listen_on = self.listen_on.unwrap_or(5000);

        server.listen_on(listen_on).await?;

        Ok(())
    }
}
