use std::path::PathBuf;

use structopt::StructOpt;
use tokio::io::AsyncReadExt;

use crate::{Backend, CustomServer, Error};

/// Command to manage the server's certificates.
#[derive(StructOpt, Debug)]
pub enum Command {
    /// Installs a self-signed certificate into the server. The server can only
    /// have one global self-signed certificate. If `overwrite` is true, any
    /// existing certificate will be overwritten. If `overwrite` is false and a
    /// certificate already exists,
    /// [`Error::Configuration`](bonsaidb_core::Error::Configuration) is
    /// returned.
    InstallSelfSigned {
        /// If an existing certificate exists, an error will be returned unless
        /// `overwrite` is true.
        #[structopt(short, long)]
        overwrite: bool,
    },
    /// Installs a X.509 certificate and associated private key in PEM format.
    ///
    /// This command reads the files `private_key` and `certificate` and
    /// executes
    /// [`Server::install_certificate()`](crate::CustomServer::install_certificate).
    Install {
        /// A private key used to generate `certificate` in the ASCII PEM format.
        private_key: PathBuf,
        /// The X.509 certificate chain in the ASCII PEM format.
        certificate_chain: PathBuf,
    },
}

impl Command {
    /// Executes the command.
    pub async fn execute<B: Backend>(&self, server: CustomServer<B>) -> Result<(), Error> {
        match self {
            Self::InstallSelfSigned { overwrite } => {
                server.install_self_signed_certificate(*overwrite).await?;
            }
            Self::Install {
                private_key,
                certificate_chain,
            } => {
                let mut private_key_file = tokio::fs::File::open(&private_key).await?;
                let mut private_key = Vec::new();
                private_key_file.read_to_end(&mut private_key).await?;

                let mut certificate_chain_file = tokio::fs::File::open(&certificate_chain).await?;
                let mut certificate_chain = Vec::new();
                certificate_chain_file
                    .read_to_end(&mut certificate_chain)
                    .await?;

                server
                    .install_pem_certificate(&certificate_chain, &private_key)
                    .await?;
            }
        }

        Ok(())
    }
}
