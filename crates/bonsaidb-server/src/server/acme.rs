use std::{sync::Arc, time::Duration};

use async_acme::cache::AcmeCache;
use async_trait::async_trait;
use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    connection::Connection,
    define_basic_unique_mapped_view,
    document::{CollectionDocument, Emit, KeyId},
    schema::{Collection, SerializedCollection},
};
use serde::{Deserialize, Serialize};

use crate::{Backend, CustomServer, Error};

#[derive(Clone, Debug, Serialize, Deserialize, Collection)]
#[collection(name = "acme-accounts", authority = "khonsulabs", views = [AcmeAccountByContacts])]
#[collection(encryption_key = Some(KeyId::Master), encryption_optional, core = bonsaidb_core)]
pub struct AcmeAccount {
    pub contacts: Vec<String>,
    pub data: Bytes,
}

define_basic_unique_mapped_view!(
    AcmeAccountByContacts,
    AcmeAccount,
    1,
    "by-contacts",
    String,
    |document: CollectionDocument<AcmeAccount>| {
        document
            .header
            .emit_key(document.contents.contacts.join(";"))
    }
);

#[async_trait]
impl<B: Backend> AcmeCache for CustomServer<B> {
    type Error = Error;

    async fn read_account(&self, contacts: &[&str]) -> Result<Option<Vec<u8>>, Self::Error> {
        let db = self.hosted().await;
        let contact = db
            .view::<AcmeAccountByContacts>()
            .with_key(contacts.join(";"))
            .query_with_collection_docs()
            .await?
            .documents
            .into_iter()
            .next();

        if let Some((_, contact)) = contact {
            Ok(Some(contact.contents.data.into_vec()))
        } else {
            Ok(None)
        }
    }

    async fn write_account(&self, contacts: &[&str], contents: &[u8]) -> Result<(), Self::Error> {
        let db = self.hosted().await;
        let mapped_account = db
            .view::<AcmeAccountByContacts>()
            .with_key(contacts.join(";"))
            .query_with_collection_docs()
            .await?
            .documents
            .into_iter()
            .next();
        if let Some((_, mut account)) = mapped_account {
            account.contents.data = Bytes::from(contents);
            account.update(&db).await?;
        } else {
            AcmeAccount {
                contacts: contacts.iter().map(|&c| c.to_string()).collect(),
                data: Bytes::from(contents),
            }
            .push_into(&db)
            .await?;
        }

        Ok(())
    }

    async fn write_certificate(
        &self,
        _domains: &[String],
        _directory_url: &str,
        key_pem: &str,
        certificate_pem: &str,
    ) -> Result<(), Self::Error> {
        self.install_pem_certificate(certificate_pem.as_bytes(), key_pem.as_bytes())
            .await
    }
}

impl<B: Backend> CustomServer<B> {
    pub(crate) async fn update_acme_certificates(&self) -> Result<(), Error> {
        loop {
            {
                let key = self.data.primary_tls_key.lock().clone();
                while async_acme::rustls_helper::duration_until_renewal_attempt(key.as_deref(), 0)
                    > Duration::from_secs(24 * 60 * 60 * 14)
                {
                    tokio::time::sleep(Duration::from_secs(60 * 60)).await;
                }
            }

            log::info!(
                "requesting new tls certificate for {}",
                self.data.primary_domain
            );
            let domains = vec![self.data.primary_domain.clone()];
            async_acme::rustls_helper::order(
                |domain, key| {
                    let mut auth_keys = self.data.alpn_keys.lock().unwrap();
                    auth_keys.insert(domain, Arc::new(key));
                    Ok(())
                },
                &self.data.acme.directory,
                &domains,
                Some(self),
                &self
                    .data
                    .acme
                    .contact_email
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>(),
            )
            .await?;
        }
    }
}
