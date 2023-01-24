use std::ops::RangeInclusive;
use std::sync::Arc;

use rand::distributions::Standard;
use rand::prelude::Distribution;
use rand::Rng;

use crate::model::{InitialDataSet, Product};
use crate::utils::gen_range;

/// A single database operation.
#[derive(Clone, Debug)]
pub enum Operation {
    LookupProduct(LookupProduct),
    FindProduct(FindProduct),
    CreateCart(CreateCart),
    AddProductToCart(AddProductToCart),
    RateProduct(ReviewProduct),
    Checkout(Checkout),
}

/// Bulk-load data.
#[derive(Clone, Debug)]
pub struct Load {
    pub initial_data: Arc<InitialDataSet>,
}

/// Lookup a product by id.
#[derive(Clone, Debug)]
pub struct LookupProduct {
    pub id: u32,
}

/// Find a product by name (indexed).
#[derive(Clone, Debug)]
pub struct FindProduct {
    pub name: String,
}

/// Create a shopping cart.
#[derive(Clone, Debug)]
pub struct CreateCart;

/// Add a previously-searched-for product to a shopping cart.
#[derive(Clone, Debug)]
pub struct AddProductToCart {
    pub product: ResultId,
    pub cart: ResultId,
}

/// Convert a cart to an order.
#[derive(Clone, Debug)]
pub struct Checkout {
    pub customer_id: u32,
    pub cart: ResultId,
}

/// Review a product
#[derive(Clone, Debug)]
pub struct ReviewProduct {
    pub customer_id: u32,
    pub product_id: ResultId,
    pub review: Option<String>,
    pub rating: u8,
}

/// A sequence of [`Operation`]s.
#[derive(Clone, Debug, Default)]
pub struct Plan {
    pub operations: Vec<Operation>,
}

impl Plan {
    pub fn push(&mut self, operation: Operation) -> ResultId {
        let id = self.operations.len();
        self.operations.push(operation);
        ResultId(id)
    }
}

/// An identifier for a result from a previous [`Operation`].
#[derive(Debug, Clone, Copy)]
pub struct ResultId(pub usize);
#[derive(Debug)]
#[must_use]
pub enum OperationResult<T> {
    Ok,
    Product {
        id: u32,
        product: Product,
        rating: Option<f32>,
    },
    Cart {
        id: T,
    },
    CartProduct {
        id: u32,
    },
}

pub struct ShopperPlanConfig {
    pub chance_of_adding_product_to_cart: f64,
    pub chance_of_purchasing: f64,
    pub chance_of_rating: f64,

    pub product_search_attempts: RangeInclusive<u32>,
}

impl ShopperPlanConfig {
    pub fn random_plan<R: Rng>(&self, rng: &mut R, dataset: &InitialDataSet) -> Plan {
        let mut plan = Plan::default();
        let mut shopping_cart = None;

        let attempts = if self.product_search_attempts.start() == self.product_search_attempts.end()
        {
            *self.product_search_attempts.start()
        } else {
            rng.gen_range(self.product_search_attempts.clone())
        };
        let mut products_in_cart = Vec::new();
        for _ in 0..attempts {
            // Find a product
            let product = match rng.gen::<ProductSearchStrategy>() {
                ProductSearchStrategy::ByName => {
                    let product_to_find = dataset
                        .products
                        .values()
                        .nth(rng.gen_range(0..dataset.products.len()))
                        .unwrap();

                    plan.push(Operation::FindProduct(FindProduct {
                        name: product_to_find.name.clone(),
                    }))
                }
                ProductSearchStrategy::Direct => {
                    let product_to_find = dataset
                        .products
                        .keys()
                        .nth(rng.gen_range(0..dataset.products.len()))
                        .unwrap();

                    plan.push(Operation::LookupProduct(LookupProduct {
                        id: *product_to_find,
                    }))
                }
            };

            // Add it to the cart?
            if rng.gen_bool(self.chance_of_adding_product_to_cart) {
                let cart = if let Some(shopping_cart) = shopping_cart {
                    shopping_cart
                } else {
                    // Create a shopping cart
                    let new_cart = plan.push(Operation::CreateCart(CreateCart));
                    shopping_cart = Some(new_cart);
                    new_cart
                };

                products_in_cart.push(plan.push(Operation::AddProductToCart(AddProductToCart {
                    product,
                    cart,
                })));
            }
        }

        if !dataset.customers.is_empty() {
            if let Some(cart) = shopping_cart {
                if rng.gen_bool(self.chance_of_purchasing) {
                    // TODO checkout instead of using an existing customer.
                    let customer_index = gen_range(rng, 0..=(dataset.customers.len() - 1));
                    let customer_id = *dataset.customers.keys().nth(customer_index).unwrap();
                    plan.push(Operation::Checkout(Checkout { cart, customer_id }));
                    for product_id in products_in_cart {
                        if rng.gen_bool(self.chance_of_rating) {
                            plan.push(Operation::RateProduct(ReviewProduct {
                                customer_id,
                                product_id,
                                rating: rng.gen_range(1..=5),
                                review: None,
                            }));
                        }
                    }
                }
            } /*else if rng.gen_bool(self.chance_of_rating) {
                  todo!("allow ratings to happen for existing orders")
              }*/
        }

        plan
    }
}

pub enum ProductSearchStrategy {
    Direct,
    ByName,
}

impl Distribution<ProductSearchStrategy> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> ProductSearchStrategy {
        match rng.gen_range(0..2) {
            0 => ProductSearchStrategy::Direct,
            _ => ProductSearchStrategy::ByName,
        }
    }
}
