use bonsaidb_core::{
    define_basic_mapped_view, define_basic_unique_mapped_view,
    document::{CollectionDocument, KeyId},
    schema::{Collection, NamedCollection, Schema},
};
use fabruic::{CertificateChain, PrivateKey};
use serde::{de::Visitor, Deserialize, Serialize};

#[derive(Debug, Schema)]
#[schema(name = "hosted", authority = "khonsulabs", collections = [TlsCertificate], core = bonsaidb_core)]
#[cfg_attr(feature = "acme", schema(collections = [crate::server::acme::AcmeAccount]))]
pub struct Hosted;

#[derive(Debug, Serialize, Deserialize, Collection)]
#[collection(name = "tls-certificates", authority = "khonsulabs", views = [TlsCertificatesByDomain, TlsCertificateByAllDomains])]
#[collection(encryption_key = Some(KeyId::Master), encryption_optional, core = bonsaidb_core)]
pub struct TlsCertificate {
    pub domains: Vec<String>,
    pub private_key: SerializablePrivateKey,
    pub certificate_chain: CertificateChain,
}

impl NamedCollection for TlsCertificate {
    type ByNameView = TlsCertificateByAllDomains;
}

define_basic_unique_mapped_view!(
    TlsCertificateByAllDomains,
    TlsCertificate,
    1,
    "by-all-domains",
    String,
    |document: CollectionDocument<TlsCertificate>| {
        document
            .header
            .emit_key(document.contents.domains.join(";"))
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
