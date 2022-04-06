use std::{path::Path, time::Duration};

#[cfg(feature = "compression")]
use bonsaidb::local::config::Compression;
use bonsaidb::{
    client::{url::Url, Client},
    core::{
        async_trait::async_trait,
        connection::{
            AccessPolicy, AsyncConnection, AsyncLowLevelConnection, AsyncStorageConnection,
        },
        define_basic_unique_mapped_view,
        document::{CollectionDocument, CollectionHeader, Emit},
        schema::{
            view::map::Mappings, Collection, CollectionName, CollectionViewSchema,
            DefaultSerialization, InsertError, NamedCollection, Qualified, ReduceResult, Schema,
            Schematic, SerializedCollection, View, ViewMapResult, ViewMappedValue,
        },
        transaction::{self, Transaction},
        Error,
    },
    local::config::Builder,
    server::{DefaultPermissions, Server, ServerConfiguration},
    AnyDatabase,
};
use serde::{Deserialize, Serialize};

use crate::{
    execute::{Backend, BackendOperator, Measurements, Metric, Operator},
    model::{Cart, Category, Customer, Order, Product, ProductReview},
    plan::{
        AddProductToCart, Checkout, CreateCart, FindProduct, Load, LookupProduct, OperationResult,
        ReviewProduct,
    },
};

pub enum Bonsai {
    Local,
    LocalLz4,
    Quic,
    WebSockets,
}

impl Bonsai {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Local => "bonsaidb-local",
            Self::LocalLz4 => "bonsaidb-local+lz4",
            Self::Quic => "bonsaidb-quic",
            Self::WebSockets => "bonsaidb-ws",
        }
    }
}

pub struct BonsaiBackend {
    server: Server,
    kind: Bonsai,
}

pub struct BonsaiOperator {
    label: &'static str,
    database: AnyDatabase,
}

#[derive(Debug, Schema)]
#[schema(name = "commerce", authority = "benchmarks", collections = [Product, Category, Customer, Order, Cart, ProductReview])]
pub enum Commerce {}

#[async_trait]
impl Backend for BonsaiBackend {
    type Operator = BonsaiOperator;
    type Config = Bonsai;

    fn label(&self) -> &'static str {
        self.kind.label()
    }

    #[cfg_attr(not(feature = "compression"), allow(unused_mut))]
    async fn new(config: Self::Config) -> Self {
        let path = Path::new("commerce-benchmarks.bonsaidb");
        if path.exists() {
            std::fs::remove_dir_all(path).unwrap();
        }
        let mut server_config = ServerConfiguration::new(path)
            .default_permissions(DefaultPermissions::AllowAll)
            .with_schema::<Commerce>()
            .unwrap();

        #[cfg(feature = "compression")]
        {
            if matches!(config, Bonsai::LocalLz4) {
                server_config = server_config.default_compression(Compression::Lz4);
            }
        }

        let server = Server::open(server_config).await.unwrap();
        server.install_self_signed_certificate(false).await.unwrap();
        server
            .create_database::<Commerce>("commerce", false)
            .await
            .unwrap();

        match config {
            Bonsai::Quic => {
                let server = server.clone();
                tokio::spawn(async move {
                    server.listen_on(7022).await.unwrap();
                });
            }
            Bonsai::WebSockets => {
                let server = server.clone();
                tokio::spawn(async move {
                    server
                        .listen_for_websockets_on("0.0.0.0:7023", false)
                        .await
                        .unwrap();
                });
            }
            Bonsai::Local | Bonsai::LocalLz4 => {}
        }
        // Allow the server time to start listening
        tokio::time::sleep(Duration::from_millis(1000)).await;

        BonsaiBackend {
            server,
            kind: config,
        }
    }

    async fn new_operator_async(&self) -> Self::Operator {
        let database = match self.kind {
            Bonsai::Local | Bonsai::LocalLz4 => {
                AnyDatabase::Local(self.server.database::<Commerce>("commerce").await.unwrap())
            }

            Bonsai::Quic => {
                let client = Client::build(Url::parse("bonsaidb://localhost:7022").unwrap())
                    .with_certificate(
                        self.server
                            .certificate_chain()
                            .await
                            .unwrap()
                            .into_end_entity_certificate(),
                    )
                    .finish()
                    .unwrap();
                AnyDatabase::Networked(client.database::<Commerce>("commerce").await.unwrap())
            }
            Bonsai::WebSockets => {
                let client = Client::build(Url::parse("ws://localhost:7023").unwrap())
                    .finish()
                    .unwrap();
                AnyDatabase::Networked(client.database::<Commerce>("commerce").await.unwrap())
            }
        };
        BonsaiOperator {
            database,
            label: self.label(),
        }
    }
}

impl BackendOperator for BonsaiOperator {
    type Id = u32;
}

#[async_trait]
impl Operator<Load, u32> for BonsaiOperator {
    async fn operate(
        &mut self,
        operation: &Load,
        _results: &[OperationResult<u32>],
        measurements: &Measurements,
    ) -> OperationResult<u32> {
        let measurement = measurements.begin(self.label, Metric::Load);
        let mut tx = Transaction::default();
        for (id, category) in &operation.initial_data.categories {
            tx.push(
                transaction::Operation::insert_serialized::<Category>(Some(*id), category).unwrap(),
            );
        }
        for (id, product) in &operation.initial_data.products {
            tx.push(
                transaction::Operation::insert_serialized::<Product>(Some(*id), product).unwrap(),
            );
        }
        for (id, customer) in &operation.initial_data.customers {
            tx.push(
                transaction::Operation::insert_serialized::<Customer>(Some(*id), customer).unwrap(),
            );
        }
        for (id, order) in &operation.initial_data.orders {
            tx.push(transaction::Operation::insert_serialized::<Order>(Some(*id), order).unwrap());
        }
        for review in &operation.initial_data.reviews {
            tx.push(
                transaction::Operation::insert_serialized::<ProductReview>(None, review).unwrap(),
            );
        }
        self.database.apply_transaction(tx).await.unwrap();
        measurement.finish();
        OperationResult::Ok
    }
}

#[async_trait]
impl Operator<FindProduct, u32> for BonsaiOperator {
    async fn operate(
        &mut self,
        operation: &FindProduct,
        _results: &[OperationResult<u32>],
        measurements: &Measurements,
    ) -> OperationResult<u32> {
        let measurement = measurements.begin(self.label, Metric::FindProduct);
        let doc = Product::load_async(&operation.name, &self.database)
            .await
            .unwrap()
            .unwrap();
        let rating = self
            .database
            .view::<ProductReviewsByProduct>()
            .with_key(doc.header.id)
            .with_access_policy(AccessPolicy::NoUpdate)
            .reduce()
            .await
            .unwrap();
        measurement.finish();
        OperationResult::Product {
            id: doc.header.id,
            product: doc.contents,
            rating: rating.average(),
        }
    }
}

#[async_trait]
impl Operator<LookupProduct, u32> for BonsaiOperator {
    async fn operate(
        &mut self,
        operation: &LookupProduct,
        _results: &[OperationResult<u32>],
        measurements: &Measurements,
    ) -> OperationResult<u32> {
        let measurement = measurements.begin(self.label, Metric::LookupProduct);
        let doc = Product::get_async(operation.id, &self.database)
            .await
            .unwrap()
            .unwrap();
        let rating = self
            .database
            .view::<ProductReviewsByProduct>()
            .with_key(doc.header.id)
            .with_access_policy(AccessPolicy::NoUpdate)
            .reduce()
            .await
            .unwrap();
        measurement.finish();
        OperationResult::Product {
            id: doc.header.id,
            product: doc.contents,
            rating: rating.average(),
        }
    }
}

#[async_trait]
impl Operator<CreateCart, u32> for BonsaiOperator {
    async fn operate(
        &mut self,
        _operation: &CreateCart,
        _results: &[OperationResult<u32>],
        measurements: &Measurements,
    ) -> OperationResult<u32> {
        let measurement = measurements.begin(self.label, Metric::CreateCart);
        let cart = Cart::default()
            .push_into_async(&self.database)
            .await
            .unwrap();
        measurement.finish();
        OperationResult::Cart { id: cart.header.id }
    }
}

#[async_trait]
impl Operator<AddProductToCart, u32> for BonsaiOperator {
    async fn operate(
        &mut self,
        operation: &AddProductToCart,
        results: &[OperationResult<u32>],
        measurements: &Measurements,
    ) -> OperationResult<u32> {
        let cart = match &results[operation.cart.0] {
            OperationResult::Cart { id } => *id,
            _ => unreachable!("Invalid operation result"),
        };
        let product = match &results[operation.product.0] {
            OperationResult::Product { id, .. } => *id,
            _ => unreachable!("Invalid operation result"),
        };

        let measurement = measurements.begin(self.label, Metric::AddProductToCart);
        let mut cart = Cart::get_async(cart, &self.database)
            .await
            .unwrap()
            .unwrap();
        cart.contents.product_ids.push(product);
        cart.update_async(&self.database).await.unwrap();
        measurement.finish();

        OperationResult::CartProduct { id: product }
    }
}

#[async_trait]
impl Operator<Checkout, u32> for BonsaiOperator {
    async fn operate(
        &mut self,
        operation: &Checkout,
        results: &[OperationResult<u32>],
        measurements: &Measurements,
    ) -> OperationResult<u32> {
        let cart = match &results[operation.cart.0] {
            OperationResult::Cart { id } => *id,
            _ => unreachable!("Invalid operation result"),
        };

        let measurement = measurements.begin(self.label, Metric::Checkout);
        let cart = Cart::get_async(cart, &self.database)
            .await
            .unwrap()
            .unwrap();
        cart.delete_async(&self.database).await.unwrap();
        Order {
            customer_id: operation.customer_id,
            product_ids: cart.contents.product_ids,
        }
        .push_into_async(&self.database)
        .await
        .unwrap();
        measurement.finish();

        OperationResult::Ok
    }
}

#[async_trait]
impl Operator<ReviewProduct, u32> for BonsaiOperator {
    async fn operate(
        &mut self,
        operation: &ReviewProduct,
        results: &[OperationResult<u32>],
        measurements: &Measurements,
    ) -> OperationResult<u32> {
        let product_id = match &results[operation.product_id.0] {
            OperationResult::Product { id, .. } => *id,
            OperationResult::CartProduct { id, .. } => *id,
            other => unreachable!("Invalid operation result {:?}", other),
        };

        let measurement = measurements.begin(self.label, Metric::RateProduct);
        let review = ProductReview {
            customer_id: operation.customer_id,
            product_id,
            review: operation.review.clone(),
            rating: operation.rating,
        };
        // https://github.com/khonsulabs/bonsaidb/issues/189
        match review.push_into_async(&self.database).await {
            Ok(_) => {}
            Err(InsertError {
                error:
                    bonsaidb::core::Error::UniqueKeyViolation {
                        existing_document, ..
                    },
                contents,
            }) => {
                CollectionDocument::<ProductReview> {
                    header: CollectionHeader::try_from(*existing_document).unwrap(),
                    contents,
                }
                .update_async(&self.database)
                .await
                .unwrap();
            }
            other => {
                other.unwrap();
            }
        }
        // Force the view to update.
        self.database
            .view::<ProductReviewsByProduct>()
            .with_key(0)
            .reduce()
            .await
            .unwrap();
        measurement.finish();

        OperationResult::Ok
    }
}

impl Collection for Product {
    type PrimaryKey = u32;

    fn collection_name() -> CollectionName {
        CollectionName::new("benchmarks", "products")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_view(ProductsByName)?;
        schema.define_view(ProductsByCategoryId)?;
        Ok(())
    }
}

impl DefaultSerialization for Product {}

define_basic_unique_mapped_view!(
    ProductsByName,
    Product,
    1,
    "by-name",
    String,
    (),
    |document: CollectionDocument<Product>| { document.header.emit_key(document.contents.name) },
);

#[derive(Debug, Clone, View)]
#[view(collection = Product, key = u32, value = u32, name = "by-category")]
pub struct ProductsByCategoryId;

impl CollectionViewSchema for ProductsByCategoryId {
    type View = Self;

    fn map(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> ViewMapResult<Self::View> {
        let mut mappings = Mappings::default();
        for &id in &document.contents.category_ids {
            mappings = mappings.and(document.header.emit_key_and_value(id, 1)?);
        }
        Ok(mappings)
    }
}

impl NamedCollection for Product {
    type ByNameView = ProductsByName;
}

impl Collection for ProductReview {
    type PrimaryKey = u32;

    fn collection_name() -> CollectionName {
        CollectionName::new("benchmarks", "reviews")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_view(ProductReviewsByProduct)?;
        Ok(())
    }
}

impl DefaultSerialization for ProductReview {}

#[derive(Debug, Clone, View)]
#[view(collection = ProductReview, key = u32, value = ProductRatings, name = "by-product")]
pub struct ProductReviewsByProduct;

impl CollectionViewSchema for ProductReviewsByProduct {
    type View = Self;

    fn map(
        &self,
        document: CollectionDocument<<Self as View>::Collection>,
    ) -> ViewMapResult<Self::View> {
        document.header.emit_key_and_value(
            document.contents.product_id,
            ProductRatings {
                total_score: document.contents.rating as u32,
                ratings: 1,
            },
        )
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<Self::View>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings
            .iter()
            .map(|mapping| mapping.value.clone())
            .reduce(|a, b| ProductRatings {
                total_score: a.total_score + b.total_score,
                ratings: a.ratings + b.ratings,
            })
            .unwrap_or_default())
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct ProductRatings {
    pub total_score: u32,
    pub ratings: u32,
}

impl ProductRatings {
    pub fn average(&self) -> Option<f32> {
        if self.ratings > 0 {
            Some(self.total_score as f32 / self.ratings as f32)
        } else {
            None
        }
    }
}

impl Collection for Category {
    type PrimaryKey = u32;

    fn collection_name() -> CollectionName {
        CollectionName::new("benchmarks", "categories")
    }

    fn define_views(_schema: &mut Schematic) -> Result<(), Error> {
        Ok(())
    }
}

impl DefaultSerialization for Category {}

impl Collection for Customer {
    type PrimaryKey = u32;

    fn collection_name() -> CollectionName {
        CollectionName::new("benchmarks", "customers")
    }

    fn define_views(_schema: &mut Schematic) -> Result<(), Error> {
        Ok(())
    }
}

impl DefaultSerialization for Customer {}

impl Collection for Order {
    type PrimaryKey = u32;

    fn collection_name() -> CollectionName {
        CollectionName::new("benchmarks", "orders")
    }

    fn define_views(_schema: &mut Schematic) -> Result<(), Error> {
        Ok(())
    }
}

impl DefaultSerialization for Order {}

impl Collection for Cart {
    type PrimaryKey = u32;

    fn collection_name() -> CollectionName {
        CollectionName::new("benchmarks", "carts")
    }

    fn define_views(_schema: &mut Schematic) -> Result<(), Error> {
        Ok(())
    }
}

impl DefaultSerialization for Cart {}
