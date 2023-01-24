use std::collections::BTreeMap;

use bonsaidb::core::async_trait::async_trait;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{doc, Document};
use mongodb::options::{
    Acknowledgment, ClientOptions, CreateCollectionOptions, IndexOptions, WriteConcern,
};
use mongodb::{Client, IndexModel};
use serde::{Deserialize, Serialize};

use crate::execute::{Backend, BackendOperator, Measurements, Metric, Operator};
use crate::model::{Cart, Order, Product, ProductReview};
use crate::plan::{
    AddProductToCart, Checkout, CreateCart, FindProduct, Load, LookupProduct, OperationResult,
    ReviewProduct,
};

pub struct MongoBackend {
    client: mongodb::Client,
}

pub struct MongoOperator {
    client: mongodb::Client,
}

#[async_trait]
impl Backend for MongoBackend {
    type Config = String;
    type Operator = MongoOperator;

    fn label(&self) -> &'static str {
        "mongodb"
    }

    async fn new(url: Self::Config) -> Self {
        async fn create_collection(db: &mongodb::Database, name: &str) {
            db.create_collection(
                name,
                CreateCollectionOptions::builder()
                    .write_concern(
                        WriteConcern::builder()
                            .w(Acknowledgment::Majority)
                            .journal(true)
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap();
        }
        let mut client_options = ClientOptions::parse(&url).await.unwrap();
        client_options.retry_writes = Some(false);
        client_options.app_name = Some("Commerce Bench".to_string());
        let client = Client::with_options(client_options).unwrap();
        // Clean up any old data
        client.database("commerce").drop(None).await.unwrap();

        let db = client.database("commerce");
        create_collection(&db, "categories").await;
        create_collection(&db, "products").await;
        create_collection(&db, "customers").await;
        create_collection(&db, "orders").await;
        create_collection(&db, "reviews").await;
        create_collection(&db, "carts").await;

        // Unique index of product by name
        db.collection::<MongoDoc<Product>>("products")
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "name": 1 })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
                None,
            )
            .await
            .unwrap();

        // unique index of product reviews, first by product id and then by
        // customer id. This index is used both for grouping in aggregation as
        // well as upserting reviews.
        db.collection::<ProductReview>("reviews")
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "product_id": 1, "customer_id": 1 })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
                None,
            )
            .await
            .unwrap();

        MongoBackend { client }
    }

    async fn new_operator_async(&self) -> Self::Operator {
        MongoOperator {
            client: self.client.clone(),
        }
    }
}

impl BackendOperator for MongoOperator {
    type Id = ObjectId;
}

#[derive(Serialize, Deserialize)]
pub struct MongoDoc<T> {
    #[serde(rename = "_id")]
    pub id: u32,
    #[serde(flatten)]
    pub contents: T,
}

#[async_trait]
impl Operator<Load, ObjectId> for MongoOperator {
    async fn operate(
        &mut self,
        operation: &Load,
        _results: &[OperationResult<ObjectId>],
        measurements: &Measurements,
    ) -> OperationResult<ObjectId> {
        async fn insert_mongo_docs<T: Serialize + Clone>(
            db: &mongodb::Database,
            collection_name: &str,
            initial_data: &BTreeMap<u32, T>,
        ) {
            db.collection::<MongoDoc<T>>(collection_name)
                .insert_many(
                    initial_data.iter().map(|(id, contents)| MongoDoc {
                        id: *id,
                        contents: contents.clone(),
                    }),
                    None,
                )
                .await
                .unwrap();
        }
        let measurement = measurements.begin("mongodb", Metric::Load);
        let db = self.client.database("commerce");

        insert_mongo_docs(&db, "categories", &operation.initial_data.categories).await;
        insert_mongo_docs(&db, "products", &operation.initial_data.products).await;
        insert_mongo_docs(&db, "customers", &operation.initial_data.customers).await;
        insert_mongo_docs(&db, "orders", &operation.initial_data.orders).await;

        db.collection::<ProductReview>("reviews")
            .insert_many(operation.initial_data.reviews.iter().cloned(), None)
            .await
            .unwrap();

        measurement.finish();
        OperationResult::Ok
    }
}

#[async_trait]
impl Operator<FindProduct, ObjectId> for MongoOperator {
    async fn operate(
        &mut self,
        operation: &FindProduct,
        _results: &[OperationResult<ObjectId>],
        measurements: &Measurements,
    ) -> OperationResult<ObjectId> {
        let measurement = measurements.begin("mongodb", Metric::FindProduct);
        let database = self.client.database("commerce");
        let product = database
            .collection::<MongoDoc<Product>>("products")
            .find_one(doc! { "name": &operation.name }, None)
            .await
            .unwrap()
            .unwrap();
        let rating = database
            .collection::<Document>("ratings-by-product")
            .find_one(doc!("_id": product.id), None)
            .await
            .unwrap()
            .and_then(|doc| doc.get_f64("rating").ok().map(|f| f as f32));

        measurement.finish();
        OperationResult::Product {
            id: product.id,
            product: product.contents,
            rating,
        }
    }
}

#[async_trait]
impl Operator<LookupProduct, ObjectId> for MongoOperator {
    async fn operate(
        &mut self,
        operation: &LookupProduct,
        _results: &[OperationResult<ObjectId>],
        measurements: &Measurements,
    ) -> OperationResult<ObjectId> {
        let measurement = measurements.begin("mongodb", Metric::LookupProduct);
        let database = self.client.database("commerce");
        let product = database
            .collection::<MongoDoc<Product>>("products")
            .find_one(doc! { "_id": operation.id }, None)
            .await
            .unwrap()
            .unwrap();
        let rating = database
            .collection::<Document>("ratings-by-product")
            .find_one(doc!("_id": product.id), None)
            .await
            .unwrap()
            .and_then(|doc| doc.get_f64("rating").ok().map(|f| f as f32));

        measurement.finish();
        OperationResult::Product {
            id: product.id,
            product: product.contents,
            rating,
        }
    }
}

#[async_trait]
impl Operator<CreateCart, ObjectId> for MongoOperator {
    async fn operate(
        &mut self,
        _operation: &CreateCart,
        _results: &[OperationResult<ObjectId>],
        measurements: &Measurements,
    ) -> OperationResult<ObjectId> {
        let measurement = measurements.begin("mongodb", Metric::CreateCart);
        let carts = self.client.database("commerce").collection::<Cart>("carts");
        let result = carts.insert_one(Cart::default(), None).await.unwrap();

        measurement.finish();
        OperationResult::Cart {
            id: result.inserted_id.as_object_id().unwrap(),
        }
    }
}

#[async_trait]
impl Operator<AddProductToCart, ObjectId> for MongoOperator {
    async fn operate(
        &mut self,
        operation: &AddProductToCart,
        results: &[OperationResult<ObjectId>],
        measurements: &Measurements,
    ) -> OperationResult<ObjectId> {
        let cart_id = match &results[operation.cart.0] {
            OperationResult::Cart { id } => *id,
            _ => unreachable!("Invalid operation result"),
        };
        let product = match &results[operation.product.0] {
            OperationResult::Product { id, .. } => *id,
            _ => unreachable!("Invalid operation result"),
        };

        let measurement = measurements.begin("mongodb", Metric::AddProductToCart);
        let carts = self.client.database("commerce").collection::<Cart>("carts");

        carts
            .update_one(
                doc! {"_id": cart_id },
                doc! { "$push": { "product_ids": product } },
                None,
            )
            .await
            .unwrap();
        measurement.finish();

        OperationResult::CartProduct { id: product }
    }
}

#[async_trait]
impl Operator<Checkout, ObjectId> for MongoOperator {
    async fn operate(
        &mut self,
        operation: &Checkout,
        results: &[OperationResult<ObjectId>],
        measurements: &Measurements,
    ) -> OperationResult<ObjectId> {
        let cart_id = match &results[operation.cart.0] {
            OperationResult::Cart { id } => *id,
            _ => unreachable!("Invalid operation result"),
        };

        let measurement = measurements.begin("mongodb", Metric::Checkout);
        let carts = self.client.database("commerce").collection::<Cart>("carts");
        let cart = carts
            .find_one_and_delete(doc! { "_id": cart_id }, None)
            .await
            .unwrap()
            .unwrap();

        let orders = self
            .client
            .database("commerce")
            .collection::<Order>("orders");

        orders
            .insert_one(
                Order {
                    customer_id: operation.customer_id,
                    product_ids: cart.product_ids,
                },
                None,
            )
            .await
            .unwrap();

        measurement.finish();

        OperationResult::Ok
    }
}

#[async_trait]
impl Operator<ReviewProduct, ObjectId> for MongoOperator {
    async fn operate(
        &mut self,
        operation: &ReviewProduct,
        results: &[OperationResult<ObjectId>],
        measurements: &Measurements,
    ) -> OperationResult<ObjectId> {
        let product_id = match &results[operation.product_id.0] {
            OperationResult::Product { id, .. } => *id,
            OperationResult::CartProduct { id, .. } => *id,
            other => unreachable!("Invalid operation result {:?}", other),
        };

        let measurement = measurements.begin("mongodb", Metric::RateProduct);
        let reviews = self
            .client
            .database("commerce")
            .collection::<ProductReview>("reviews");

        // Upsert the review for the customer.
        reviews
            .update_one(
                doc! { "product_id": product_id, "customer_id": operation.customer_id },
                doc! { "$set": {
                    "review": operation.review.clone(),
                    "rating": u32::from(operation.rating)
                }},
                None,
            )
            .await
            .unwrap();

        // Re-aggregate the ratings for the product that was reviewed. This
        // updates the materialized view, "ratings-by-product", but only on
        // the $match.
        reviews
            .aggregate(
                [
                    doc! {
                        "$match": { "product_id": product_id },
                    },
                    doc! {
                        "$group": { "_id": "$product_id", "rating": { "$avg": "$rating" } },
                    },
                    doc! {
                        "$merge": {"into": "ratings-by-product", "whenMatched": "replace" }
                    },
                ],
                None,
            )
            .await
            .unwrap();
        measurement.finish();

        OperationResult::Ok
    }
}
