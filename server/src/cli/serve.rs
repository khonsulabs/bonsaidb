use structopt::StructOpt;

use crate::Server;

/// Execute the server
#[derive(StructOpt, Debug)]
pub struct Serve {
    /// The bind address and port. Defaults to 5645
    // TODO IANA port reservation for port 5645 has been submitted.
    #[structopt(short = "l", long = "listen-on")]
    pub listen_on: Option<u16>,
}

impl Serve {
    /// Starts the server.
    pub async fn execute(&self, server: Server) -> anyhow::Result<()> {
        let listen_on = self.listen_on.unwrap_or(5645);

        server.listen_on(listen_on).await?;

        Ok(())
    }
}
