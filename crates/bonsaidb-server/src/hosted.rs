use bonsaidb_core::{
    define_basic_mapped_view, define_basic_unique_mapped_view,
    document::KeyId,
    schema::{
        Collection, CollectionDocument, CollectionName, DefaultSerialization, InvalidNameError,
        NamedCollection, Schema, SchemaName, Schematic,
    },
    ENCRYPTION_ENABLED,
};
use fabruic::{CertificateChain, PrivateKey};
use serde::{de::Visitor, Deserialize, Serialize};

#[derive(Debug)]
pub struct Hosted;

impl Schema for Hosted {
    fn schema_name() -> Result<SchemaName, InvalidNameError> {
        SchemaName::new("khonsulabs", "hosted")
    }

    fn define_collections(schema: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
        schema.define_collection::<TlsCertificate>()?;
        #[cfg(feature = "acme")]
        schema.define_collection::<crate::server::acme::AcmeAccount>()?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TlsCertificate {
    pub domains: Vec<String>,
    pub private_key: SerializablePrivateKey,
    pub certificate_chain: CertificateChain,
}

impl NamedCollection for TlsCertificate {
    type ByNameView = TlsCertificateByAllDomains;
}

impl Collection for TlsCertificate {
    fn encryption_key() -> Option<KeyId> {
        if ENCRYPTION_ENABLED {
            Some(KeyId::Master)
        } else {
            None
        }
    }

    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("khonsulabs", "tls-certificates")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
        schema.define_view(TlsCertificatesByDomain)?;
        schema.define_view(TlsCertificateByAllDomains)?;
        Ok(())
    }
}

impl DefaultSerialization for TlsCertificate {}

define_basic_unique_mapped_view!(
    TlsCertificateByAllDomains,
    TlsCertificate,
    1,
    "by-all-domains",
    String,
    |document: CollectionDocument<TlsCertificate>| {
        document.emit_key(document.contents.domains.join(";"))
    }
);

define_basic_mapped_view!(
    TlsCertificatesByDomain,
    TlsCertificate,
    1,
    "by-domain",
    String,
    CertificateChain,
    |document: CollectionDocument<TlsCertificate>| {
        document
            .contents
            .domains
            .into_iter()
            .map(|domain| {
                document
                    .header
                    .emit_key_and_value(domain, document.contents.certificate_chain.clone())
            })
            .collect()
    }
);

#[derive(Debug)]
pub struct SerializablePrivateKey(pub PrivateKey);

impl Serialize for SerializablePrivateKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        fabruic::dangerous::PrivateKey::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for SerializablePrivateKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        PrivateKey::deserialize(deserializer).map(Self)
    }
}

struct SerializablePrivateKeyVisitor;

impl<'de> Visitor<'de> for SerializablePrivateKeyVisitor {
    type Value = SerializablePrivateKey;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("byte array")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(SerializablePrivateKey(PrivateKey::unchecked_from_der(
            v.to_vec(),
        )))
    }
}
