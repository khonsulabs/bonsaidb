use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::{
    connection::Connection,
    custodian_password::ServerConfig,
    document::{Doc, Document},
    password_config,
    schema::{
        view::{DefaultViewSerialization, ViewSchema},
        Collection, CollectionName, DefaultSerialization, Name, View, ViewMapResult,
    },
};

#[derive(Debug, Serialize, Deserialize)]
pub struct PasswordConfig {
    config: ServerConfig,
}

impl PasswordConfig {
    #[allow(clippy::missing_panics_doc)]
    pub async fn load<C: Connection>(connection: &C) -> Result<Self, crate::Error> {
        if let Some(existing) = Self::existing_configuration(connection).await? {
            Ok(existing)
        } else {
            let new_config = Self {
                config: ServerConfig::new(password_config()),
            };
            match connection.collection::<Self>().push(&new_config).await {
                Ok(_) => Ok(new_config),
                Err(crate::Error::UniqueKeyViolation { .. }) => {
                    // Raced to create it. This shouldn't be possible
                    Ok(Self::existing_configuration(connection).await?.unwrap())
                }
                Err(err) => Err(err),
            }
        }
    }

    async fn existing_configuration<C: Connection>(
        connection: &C,
    ) -> Result<Option<Self>, crate::Error> {
        let mapped_document = connection
            .view::<Singleton>()
            .query_with_docs()
            .await?
            .into_iter()
            .next();
        if let Some(mapped_document) = mapped_document {
            let config = mapped_document.document.contents::<Self>().unwrap();
            Ok(Some(config))
        } else {
            Ok(None)
        }
    }
}

impl Deref for PasswordConfig {
    type Target = ServerConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl Collection for PasswordConfig {
    fn collection_name() -> CollectionName {
        CollectionName::new("khonsulabs", "password-config")
    }

    fn define_views(schema: &mut crate::schema::Schematic) -> Result<(), crate::Error> {
        schema.define_view(Singleton)?;
        Ok(())
    }
}

impl DefaultSerialization for PasswordConfig {}

#[derive(Debug, Clone)]
struct Singleton;

impl View for Singleton {
    type Collection = PasswordConfig;
    type Key = ();
    type Value = ();

    fn name(&self) -> Name {
        Name::new("singleton")
    }
}

impl ViewSchema for Singleton {
    type View = Self;

    fn unique(&self) -> bool {
        true
    }

    fn version(&self) -> u64 {
        1
    }

    fn map<'a>(&self, document: &'a Document<'_>) -> ViewMapResult<Self::View> {
        Ok(document.emit())
    }
}

impl DefaultViewSerialization for Singleton {}
