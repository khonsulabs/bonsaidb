use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::{
    connection::Connection,
    custodian_password::ServerConfig,
    document::{Document, KeyId},
    schema::{Collection, CollectionName, InvalidNameError, MapResult, Name, View},
    PASSWORD_CONFIG,
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
                config: ServerConfig::new(PASSWORD_CONFIG),
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
    fn collection_name() -> Result<crate::schema::CollectionName, InvalidNameError> {
        CollectionName::new("khonsulabs", "password-config")
    }

    fn define_views(schema: &mut crate::schema::Schematic) -> Result<(), crate::Error> {
        schema.define_view(Singleton)?;
        Ok(())
    }
}

#[derive(Debug)]
struct Singleton;

impl View for Singleton {
    type Collection = PasswordConfig;

    type Key = ();

    type Value = ();

    fn unique(&self) -> bool {
        true
    }

    fn version(&self) -> u64 {
        1
    }

    fn name(&self) -> Result<Name, InvalidNameError> {
        Name::new("singleton")
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::Key, Self::Value> {
        Ok(Some(document.emit()))
    }
}
