use std::path::PathBuf;

use pliantdb_networking::fabruic::{Certificate, PrivateKey};
use structopt::StructOpt;
use tokio::io::AsyncReadExt;

use crate::Server;

/// Command to manage the server's certificates.
#[derive(StructOpt, Debug)]
pub enum Command {
    /// Installs a self-signed certificate into the server. The server can only
    /// have one global self-signed certificate. If `overwrite` is true, any
    /// existing certificate will be overwritten. If `overwrite` is false and a
    /// certificate already exists, [`Error::Configuration`](crate::Error::Configuration) is returned.
    InstallSelfSigned {
        /// The name of the server. If this server has a DNS name, you should
        /// use the hostname here. This value is required to be passed in when
        /// connecting for validation.
        #[structopt(short = "n", long)]
        server_name: String,

        /// If an existing certificate exists, an error will be returned unless
        /// `overwrite` is true.
        #[structopt(short, long)]
        overwrite: bool,
    },
    /// Installs a X.509 certificate and associated private key in binary DER
    /// format.
    ///
    /// This command reads the files `private_key` and `certificate` and
    /// executes [`Server::install_certificate()`].
    Install {
        /// A private key used to generate `certificate` in binary DER format.
        private_key: PathBuf,
        /// The X.509 certificate in binary DER format.
        certificate: PathBuf,
    },
}

impl Command {
    /// Executes the command.
    pub async fn execute(&self, server: Server) -> anyhow::Result<()> {
        match self {
            Self::InstallSelfSigned {
                server_name,
                overwrite,
            } => {
                server
                    .install_self_signed_certificate(server_name, *overwrite)
                    .await?;
            }
            Self::Install {
                private_key,
                certificate,
            } => {
                let mut private_key_file = tokio::fs::File::open(&private_key).await?;
                let mut private_key = Vec::new();
                private_key_file.read_to_end(&mut private_key).await?;

                let mut certificate_file = tokio::fs::File::open(&certificate).await?;
                let mut certificate = Vec::new();
                certificate_file.read_to_end(&mut certificate).await?;

                server
                    .install_certificate(&Certificate(certificate), &PrivateKey(private_key))
                    .await?;
            }
        }

        Ok(())
    }
}
