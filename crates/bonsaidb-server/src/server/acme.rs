use std::{sync::Arc, time::Duration};

use async_acme::cache::AcmeCache;
use async_trait::async_trait;
use bonsaidb_core::{
    connection::Connection,
    define_basic_unique_mapped_view,
    document::KeyId,
    schema::{
        Collection, CollectionDocument, CollectionName, InvalidNameError, Schema, SchemaName,
        Schematic,
    },
};
use serde::{Deserialize, Serialize};

use crate::{Backend, CustomServer, Error};

#[derive(Debug)]
pub struct Acme;

impl Schema for Acme {
    fn schema_name() -> Result<SchemaName, InvalidNameError> {
        SchemaName::new("khonsulabs", "acme")
    }

    fn define_collections(schema: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
        schema.define_collection::<AcmeAccount>()?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AcmeAccount {
    pub contacts: Vec<String>,
    pub data: Vec<u8>,
}

impl Collection for AcmeAccount {
    fn encryption_key() -> Option<KeyId> {
        Some(KeyId::Master)
    }

    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("khonsulabs", "acme-accounts")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
        schema.define_view(AcmeAccountByContacts)?;
        Ok(())
    }
}

define_basic_unique_mapped_view!(
    AcmeAccountByContacts,
    AcmeAccount,
    1,
    "by-contacts",
    String,
    |document: CollectionDocument<AcmeAccount>| {
        vec![document
            .header
            .emit_key(document.contents.contacts.join(";"))]
    }
);

#[async_trait]
impl<B: Backend> AcmeCache for CustomServer<B> {
    type Error = Error;

    async fn read_account(&self, contacts: &[&str]) -> Result<Option<Vec<u8>>, Self::Error> {
        let db = self.database::<Acme>("acme").await?;
        let contact = db
            .view::<AcmeAccountByContacts>()
            .with_key(contacts.join(";"))
            .query_with_docs()
            .await?
            .into_iter()
            .next();

        if let Some(contact) = contact {
            let contact = contact.document.contents::<AcmeAccount>()?;
            Ok(Some(contact.data))
        } else {
            Ok(None)
        }
    }

    async fn write_account(&self, contacts: &[&str], contents: &[u8]) -> Result<(), Self::Error> {
        let db = self.database::<Acme>("acme").await?;
        let mapped_account = db
            .view::<AcmeAccountByContacts>()
            .with_key(contacts.join(";"))
            .query_with_docs()
            .await?
            .into_iter()
            .next();
        if let Some(mapped_account) = mapped_account {
            let mut account = CollectionDocument::<AcmeAccount>::try_from(mapped_account.document)?;
            account.contents.data = contents.to_vec();
            account.update(&db).await?;
        } else {
            AcmeAccount {
                contacts: contacts.iter().map(|&c| c.to_string()).collect(),
                data: contents.to_vec(),
            }
            .insert_into(&db)
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

            let domains = vec![self.data.acme.primary_domain.clone()];
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
