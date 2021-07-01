use custodian_password::ServerConfig;
use pliantdb_core::{
    document::Document,
    schema::{Collection, CollectionName, InvalidNameError, MapResult, Name, View},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct PasswordConfig(ServerConfig);

impl Collection for PasswordConfig {
    fn collection_name() -> Result<pliantdb_core::schema::CollectionName, InvalidNameError> {
        CollectionName::new("khonsulabs", "user")
    }

    fn define_views(
        schema: &mut pliantdb_core::schema::Schematic,
    ) -> Result<(), pliantdb_core::Error> {
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
