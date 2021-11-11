use async_acme::acme::AcmeCache;
use async_trait::async_trait;
use bonsaidb_core::{
    connection::Connection,
    document::{Document, KeyId},
    schema::{
        Collection, CollectionName, InvalidNameError, MapResult, Name, Schema, SchemaName,
        Schematic, View,
    },
};
use rcgen::{Certificate, CertificateParams, CustomExtension, PKCS_ECDSA_P256_SHA256};
use rustls::{
    sign::{any_ecdsa_type, CertifiedKey},
    PrivateKey,
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

#[derive(Debug)]
pub struct AcmeAccountByContacts;

impl View for AcmeAccountByContacts {
    type Collection = AcmeAccount;
    type Key = String;
    type Value = ();

    fn unique(&self) -> bool {
        true
    }

    fn version(&self) -> u64 {
        1
    }

    fn name(&self) -> Result<Name, InvalidNameError> {
        Name::new("by-contacts")
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::Key, Self::Value> {
        let account = document.contents::<AcmeAccount>()?;
        Ok(vec![document.emit_key(account.contacts.join(";"))])
    }
}

pub fn gen_acme_cert(
    domains: Vec<String>,
    acme_hash: &[u8],
) -> Result<CertifiedKey, rcgen::RcgenError> {
    let mut params = CertificateParams::new(domains);
    params.alg = &PKCS_ECDSA_P256_SHA256;
    params.custom_extensions = vec![CustomExtension::new_acme_identifier(acme_hash)];
    let cert = Certificate::from_params(params)?;
    let key = any_ecdsa_type(&PrivateKey(cert.serialize_private_key_der())).unwrap();
    Ok(CertifiedKey::new(
        vec![rustls::Certificate(cert.serialize_der()?)],
        key,
    ))
}

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
            let contact = dbg!(contact.document.contents::<AcmeAccount>()?);
            Ok(Some(contact.data))
        } else {
            Ok(None)
        }
    }

    async fn write_account(&self, contacts: &[&str], contents: &[u8]) -> Result<(), Self::Error> {
        let db = self.database::<Acme>("acme").await?;
        let contact = db
            .view::<AcmeAccountByContacts>()
            .with_key(contacts.join(";"))
            .query_with_docs()
            .await?
            .into_iter()
            .next();
        if let Some(contact) = contact {
            db.delete::<AcmeAccount>(&contact.document).await?;
        }

        AcmeAccount {
            contacts: contacts.iter().map(|&c| c.to_string()).collect(),
            data: contents.to_vec(),
        }
        .insert_into(&db)
        .await?;

        Ok(())
    }

    async fn write_certificate(
        &self,
        domains: &[String],
        directory_url: &str,
        key_pem: &str,
        certificate_pem: &str,
    ) -> Result<(), Self::Error> {
        self.install_pem_certificate(certificate_pem.as_bytes(), key_pem.as_bytes())
            .await
    }
}
