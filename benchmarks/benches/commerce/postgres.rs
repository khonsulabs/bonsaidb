use bonsaidb::core::actionable::async_trait;
use futures::StreamExt;
use sqlx::postgres::PgArguments;
use sqlx::{Arguments, Connection, Executor, PgPool, Row, Statement};

use crate::execute::{Backend, BackendOperator, Measurements, Metric, Operator};
use crate::model::Product;
use crate::plan::{
    AddProductToCart, Checkout, CreateCart, FindProduct, Load, LookupProduct, OperationResult,
    ReviewProduct,
};

pub struct Postgres {
    pool: PgPool,
}

#[async_trait]
impl Backend for Postgres {
    type Config = String;
    type Operator = PostgresOperator;

    fn label(&self) -> &'static str {
        "postgresql"
    }

    async fn new(url: Self::Config) -> Self {
        let pool = PgPool::connect(&url).await.unwrap();

        let mut conn = pool.acquire().await.unwrap();
        conn.execute(r#"DROP SCHEMA IF EXISTS commerce_bench CASCADE"#)
            .await
            .unwrap();
        conn.execute(r#"CREATE SCHEMA commerce_bench"#)
            .await
            .unwrap();
        conn.execute("SET search_path='commerce_bench';")
            .await
            .unwrap();
        conn.execute(
            r#"CREATE TABLE customers (
            id SERIAL PRIMARY KEY,
            name TEXT, 
            email TEXT, 
            address TEXT, 
            city TEXT, 
            region TEXT, 
            country TEXT, 
            postal_code TEXT, 
            phone TEXT
        )"#,
        )
        .await
        .unwrap();
        conn.execute(
            r#"CREATE TABLE products (
                    id SERIAL PRIMARY KEY,
                    name TEXT
                )"#,
        )
        .await
        .unwrap();
        conn.execute(r#"CREATE INDEX products_by_name ON products(name)"#)
            .await
            .unwrap();
        conn.execute(
            r#"CREATE TABLE product_reviews (
                product_id INTEGER NOT NULL,-- REFERENCES products(id),
                customer_id INTEGER NOT NULL,-- REFERENCES customers(id),
                rating INTEGER NOT NULL,
                review TEXT
            )"#,
        )
        .await
        .unwrap();
        conn.execute(r#"CREATE INDEX product_reviews_by_product ON product_reviews(product_id)"#)
            .await
            .unwrap();
        conn.execute(r#"CREATE UNIQUE INDEX product_reviews_by_customer ON product_reviews(customer_id, product_id)"#)
            .await
            .unwrap();
        conn.execute(
            r#"CREATE MATERIALIZED VIEW 
                    product_ratings 
                AS 
                    SELECT 
                        product_id,
                        sum(rating)::int as total_rating, 
                        count(rating)::int as ratings
                    FROM
                        product_reviews
                    GROUP BY product_id
            "#,
        )
        .await
        .unwrap();
        conn.execute(
            r#"CREATE TABLE categories (
                    id SERIAL PRIMARY KEY,
                    name TEXT
                )"#,
        )
        .await
        .unwrap();
        conn.execute(
            r#"CREATE TABLE product_categories (
                    product_id INTEGER,-- REFERENCES products(id),
                    category_id INTEGER-- REFERENCES categories(id)
                )"#,
        )
        .await
        .unwrap();
        conn.execute(
            r#"CREATE INDEX product_categories_by_product ON product_categories(product_id)"#,
        )
        .await
        .unwrap();
        conn.execute(
            r#"CREATE INDEX product_categories_by_category ON product_categories(category_id)"#,
        )
        .await
        .unwrap();
        conn.execute(
            r#"CREATE TABLE orders (
                    id SERIAL PRIMARY KEY,
                    customer_id INTEGER -- REFERENCES customers(id)
                )"#,
        )
        .await
        .unwrap();
        conn.execute(
            r#"CREATE TABLE order_products (
                    order_id INTEGER NOT NULL,-- REFERENCES orders(id),
                    product_id INTEGER NOT NULL -- REFERENCES products(id)
                )"#,
        )
        .await
        .unwrap();
        conn.execute(
            r#"CREATE TABLE carts (
            id SERIAL PRIMARY KEY,
            customer_id INTEGER
        )"#,
        )
        .await
        .unwrap();
        conn.execute(
            r#"CREATE TABLE cart_products (
                cart_id INTEGER,
                product_id INTEGER
            )"#,
        )
        .await
        .unwrap();

        Self { pool }
    }

    async fn new_operator_async(&self) -> Self::Operator {
        PostgresOperator {
            sqlite: self.pool.clone(),
        }
    }
}

pub struct PostgresOperator {
    sqlite: PgPool,
}

impl BackendOperator for PostgresOperator {
    type Id = u32;
}

#[async_trait]
impl Operator<Load, u32> for PostgresOperator {
    async fn operate(
        &mut self,
        operation: &Load,
        _results: &[OperationResult<u32>],
        measurements: &Measurements,
    ) -> OperationResult<u32> {
        let measurement = measurements.begin("postgresql", Metric::Load);
        let mut conn = self.sqlite.acquire().await.unwrap();
        let mut tx = conn.begin().await.unwrap();
        let insert_category = tx
            .prepare("INSERT INTO commerce_bench.categories (id, name) VALUES ($1, $2)")
            .await
            .unwrap();
        for (id, category) in &operation.initial_data.categories {
            let mut args = PgArguments::default();
            args.reserve(2, 0);
            args.add(*id);
            args.add(&category.name);
            tx.execute(insert_category.query_with(args)).await.unwrap();
        }

        let insert_product = tx
            .prepare("INSERT INTO commerce_bench.products (id, name) VALUES ($1, $2)")
            .await
            .unwrap();
        let insert_product_category = tx
            .prepare("INSERT INTO commerce_bench.product_categories (product_id, category_id) VALUES ($1, $2)")
            .await
            .unwrap();
        for (&id, product) in &operation.initial_data.products {
            let mut args = PgArguments::default();
            args.reserve(2, 0);
            args.add(id);
            args.add(&product.name);
            tx.execute(insert_product.query_with(args)).await.unwrap();
            for &category_id in &product.category_ids {
                let mut args = PgArguments::default();
                args.reserve(2, 0);
                args.add(id);
                args.add(category_id);
                tx.execute(insert_product_category.query_with(args))
                    .await
                    .unwrap();
            }
        }

        let insert_customer = tx
            .prepare("INSERT INTO commerce_bench.customers (id, name, email, address, city, region, country, postal_code, phone) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)")
            .await
            .unwrap();
        for (id, customer) in &operation.initial_data.customers {
            let mut args = PgArguments::default();
            args.reserve(9, 0);
            args.add(*id);
            args.add(&customer.name);
            args.add(&customer.email);
            args.add(&customer.address);
            args.add(&customer.city);
            args.add(&customer.region);
            args.add(&customer.country);
            args.add(&customer.postal_code);
            args.add(&customer.phone);
            tx.execute(insert_customer.query_with(args)).await.unwrap();
        }

        let insert_order = tx
            .prepare("INSERT INTO commerce_bench.orders (id, customer_id) VALUES ($1, $2)")
            .await
            .unwrap();
        let insert_order_product = tx
            .prepare(
                "INSERT INTO commerce_bench.order_products (order_id, product_id) VALUES ($1, $2)",
            )
            .await
            .unwrap();
        for (&id, order) in &operation.initial_data.orders {
            let mut args = PgArguments::default();
            args.reserve(2, 0);
            args.add(id);
            args.add(order.customer_id);
            tx.execute(insert_order.query_with(args)).await.unwrap();
            for &product_id in &order.product_ids {
                let mut args = PgArguments::default();
                args.reserve(2, 0);
                args.add(id);
                args.add(product_id);
                tx.execute(insert_order_product.query_with(args))
                    .await
                    .unwrap();
            }
        }

        let insert_review = tx
            .prepare("INSERT INTO commerce_bench.product_reviews (product_id, customer_id, rating, review) VALUES ($1, $2, $3, $4)")
            .await
            .unwrap();
        for review in &operation.initial_data.reviews {
            let mut args = PgArguments::default();
            args.reserve(4, 0);
            args.add(review.product_id);
            args.add(review.customer_id);
            args.add(review.rating as u32);
            args.add(&review.review);
            tx.execute(insert_review.query_with(args)).await.unwrap();
        }
        tx.execute(
            "SELECT setval('commerce_bench.orders_id_seq', COALESCE((SELECT MAX(id)+1 FROM commerce_bench.orders), 1), false)",
        )
        .await
        .unwrap();

        tx.commit().await.unwrap();
        // Make sure all ratings show up in the view.
        conn.execute("REFRESH MATERIALIZED VIEW commerce_bench.product_ratings")
            .await
            .unwrap();
        // This makes a significant difference.
        conn.execute("ANALYZE").await.unwrap();
        measurement.finish();

        OperationResult::Ok
    }
}
#[async_trait]
impl Operator<CreateCart, u32> for PostgresOperator {
    async fn operate(
        &mut self,
        _operation: &CreateCart,
        _results: &[OperationResult<u32>],
        measurements: &Measurements,
    ) -> OperationResult<u32> {
        let measurement = measurements.begin("postgresql", Metric::CreateCart);
        let mut conn = self.sqlite.acquire().await.unwrap();
        let mut tx = conn.begin().await.unwrap();
        let statement = tx
            .prepare("insert into commerce_bench.carts (customer_id) values (null) returning id")
            .await
            .unwrap();

        let result = tx.fetch_one(statement.query()).await.unwrap();
        tx.commit().await.unwrap();
        let id: i32 = result.get(0);
        measurement.finish();

        OperationResult::Cart { id: id as u32 }
    }
}
#[async_trait]
impl Operator<AddProductToCart, u32> for PostgresOperator {
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

        let measurement = measurements.begin("postgresql", Metric::AddProductToCart);
        let mut conn = self.sqlite.acquire().await.unwrap();
        let mut tx = conn.begin().await.unwrap();
        let statement = tx
            .prepare(
                "insert into commerce_bench.cart_products (cart_id, product_id) values ($1, $2)",
            )
            .await
            .unwrap();

        let mut args = PgArguments::default();
        args.reserve(2, 0);
        args.add(cart);
        args.add(product);

        tx.execute(statement.query_with(args)).await.unwrap();
        tx.commit().await.unwrap();
        measurement.finish();

        OperationResult::CartProduct { id: product }
    }
}
#[async_trait]
impl Operator<FindProduct, u32> for PostgresOperator {
    async fn operate(
        &mut self,
        operation: &FindProduct,
        _results: &[OperationResult<u32>],
        measurements: &Measurements,
    ) -> OperationResult<u32> {
        let measurement = measurements.begin("postgresql", Metric::FindProduct);
        let mut conn = self.sqlite.acquire().await.unwrap();
        let statement = conn
            .prepare(
                r#"
                SELECT 
                    id, 
                    name, 
                    category_id,
                    commerce_bench.product_ratings.total_rating as "total_rating: Option<i32>",
                    commerce_bench.product_ratings.ratings as "ratings: Option<i32>"
                FROM 
                    commerce_bench.products 
                LEFT OUTER JOIN commerce_bench.product_categories ON 
                    commerce_bench.product_categories.product_id = id 
                LEFT OUTER JOIN commerce_bench.product_ratings ON
                    commerce_bench.product_ratings.product_id = id
                WHERE name = $1
                GROUP BY id, name, category_id, commerce_bench.product_ratings.total_rating, commerce_bench.product_ratings.ratings
            "#,
            )
            .await
            .unwrap();

        let mut args = PgArguments::default();
        args.reserve(1, 0);
        args.add(&operation.name);

        let mut results = conn.fetch(statement.query_with(args));
        let mut id: Option<i32> = None;
        let mut name = None;
        let mut category_ids = Vec::new();
        let mut total_rating: Option<i32> = None;
        let mut rating_count: Option<i32> = None;
        while let Some(row) = results.next().await {
            let row = row.unwrap();
            id = Some(row.get(0));
            name = Some(row.get(1));
            total_rating = row.get(2);
            rating_count = row.get(3);
            if let Some(category_id) = row.get::<Option<i32>, _>(2) {
                category_ids.push(category_id as u32);
            }
        }
        let rating_count = rating_count.unwrap_or_default();
        let total_rating = total_rating.unwrap_or_default();
        measurement.finish();
        OperationResult::Product {
            id: id.unwrap() as u32,
            product: Product {
                name: name.unwrap(),
                category_ids,
            },
            rating: if rating_count > 0 {
                Some(total_rating as f32 / rating_count as f32)
            } else {
                None
            },
        }
    }
}
#[async_trait]
impl Operator<LookupProduct, u32> for PostgresOperator {
    async fn operate(
        &mut self,
        operation: &LookupProduct,
        _results: &[OperationResult<u32>],
        measurements: &Measurements,
    ) -> OperationResult<u32> {
        let measurement = measurements.begin("postgresql", Metric::LookupProduct);
        let mut conn = self.sqlite.acquire().await.unwrap();
        let statement = conn
            .prepare(
                r#"
                    SELECT 
                        id, 
                        name, 
                        category_id,
                        commerce_bench.product_ratings.total_rating as "total_rating: Option<i32>",
                        commerce_bench.product_ratings.ratings as "ratings: Option<i32>"
                    FROM 
                        commerce_bench.products 
                    LEFT OUTER JOIN commerce_bench.product_categories ON 
                        commerce_bench.product_categories.product_id = id 
                    LEFT OUTER JOIN commerce_bench.product_ratings ON
                        commerce_bench.product_ratings.product_id = id
                    WHERE id = $1
                    GROUP BY id, name, category_id, commerce_bench.product_ratings.total_rating, commerce_bench.product_ratings.ratings
                "#,
            )
            .await
            .unwrap();

        let mut args = PgArguments::default();
        args.reserve(1, 0);
        args.add(operation.id);

        let mut results = conn.fetch(statement.query_with(args));
        let mut id: Option<i32> = None;
        let mut name = None;
        let mut category_ids = Vec::new();
        let mut total_rating: Option<i32> = None;
        let mut rating_count: Option<i32> = None;
        while let Some(row) = results.next().await {
            let row = row.unwrap();
            id = Some(row.get(0));
            name = Some(row.get(1));
            total_rating = row.get(2);
            rating_count = row.get(3);
            if let Some(category_id) = row.get::<Option<i32>, _>(2) {
                category_ids.push(category_id as u32);
            }
        }
        let rating_count = rating_count.unwrap_or_default();
        let total_rating = total_rating.unwrap_or_default();

        measurement.finish();
        OperationResult::Product {
            id: id.unwrap() as u32,
            product: Product {
                name: name.unwrap(),
                category_ids,
            },
            rating: if rating_count > 0 {
                Some(total_rating as f32 / rating_count as f32)
            } else {
                None
            },
        }
    }
}

#[async_trait]
impl Operator<Checkout, u32> for PostgresOperator {
    async fn operate(
        &mut self,
        operation: &Checkout,
        results: &[OperationResult<u32>],
        measurements: &Measurements,
    ) -> OperationResult<u32> {
        let cart = match &results[operation.cart.0] {
            OperationResult::Cart { id } => *id as i32,
            _ => unreachable!("Invalid operation result"),
        };

        let measurement = measurements.begin("postgresql", Metric::Checkout);
        let mut conn = self.sqlite.acquire().await.unwrap();
        let mut tx = conn.begin().await.unwrap();
        // Create a new order
        let statement = tx
            .prepare(r#"INSERT INTO commerce_bench.orders (customer_id) VALUES ($1) RETURNING ID"#)
            .await
            .unwrap();
        let mut args = PgArguments::default();
        args.reserve(1, 0);
        args.add(operation.customer_id);
        let result = tx.fetch_one(statement.query_with(args)).await.unwrap();
        let order_id: i32 = result.get(0);

        let statement = tx
            .prepare(r#"
                WITH products_in_cart AS (
                    DELETE FROM commerce_bench.cart_products WHERE cart_id = $1 RETURNING $2::int as order_id, product_id
                )
                INSERT INTO commerce_bench.order_products (order_id, product_id) SELECT * from products_in_cart;"#)
            .await
            .unwrap();
        let mut args = PgArguments::default();
        args.reserve(2, 0);
        args.add(cart);
        args.add(order_id);
        tx.execute(statement.query_with(args)).await.unwrap();

        let statement = tx
            .prepare(r#"DELETE FROM commerce_bench.carts WHERE id = $1"#)
            .await
            .unwrap();
        let mut args = PgArguments::default();
        args.reserve(1, 0);
        args.add(cart);
        tx.execute(statement.query_with(args)).await.unwrap();
        tx.commit().await.unwrap();

        measurement.finish();

        OperationResult::Ok
    }
}

#[async_trait]
impl Operator<ReviewProduct, u32> for PostgresOperator {
    async fn operate(
        &mut self,
        operation: &ReviewProduct,
        results: &[OperationResult<u32>],
        measurements: &Measurements,
    ) -> OperationResult<u32> {
        let product = match &results[operation.product_id.0] {
            OperationResult::Product { id, .. } => *id,
            OperationResult::CartProduct { id, .. } => *id,
            _ => unreachable!("Invalid operation result"),
        };
        let measurement = measurements.begin("postgresql", Metric::RateProduct);
        let mut conn = self.sqlite.acquire().await.unwrap();
        let mut tx = conn.begin().await.unwrap();
        let statement = tx
            .prepare(
                r#"INSERT INTO commerce_bench.product_reviews (
                        product_id, 
                        customer_id, 
                        rating, 
                        review)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (customer_id, product_id) DO UPDATE SET rating = $3, review = $4"#,
            )
            .await
            .unwrap();

        let mut args = PgArguments::default();
        args.reserve(4, 0);
        args.add(product);
        args.add(operation.customer_id);
        args.add(operation.rating as u32);
        args.add(&operation.review);

        tx.execute(statement.query_with(args)).await.unwrap();
        tx.commit().await.unwrap();
        // Make this rating show up
        conn.execute("REFRESH MATERIALIZED VIEW commerce_bench.product_ratings")
            .await
            .unwrap();
        measurement.finish();

        OperationResult::Ok
    }
}
