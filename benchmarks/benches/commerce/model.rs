use std::{
    collections::{BTreeMap, HashSet},
    ops::RangeInclusive,
};

use fake::{
    faker::{
        address::raw::{
            BuildingNumber, CityName, CountryCode, PostCode, StateName, StreetName, StreetSuffix,
        },
        company::raw::{Bs, BsAdj},
        internet::raw::SafeEmail,
        lorem::raw::Paragraphs,
        name::raw::Name,
        phone_number::raw::PhoneNumber,
    },
    locales::EN,
    Dummy, Fake,
};
use rand::{seq::SliceRandom, Rng};
use serde::{Deserialize, Serialize};

use crate::utils::gen_range;

#[derive(Default, Clone, Debug)]
pub struct InitialDataSet {
    pub customers: BTreeMap<u32, Customer>,
    pub products: BTreeMap<u32, Product>,
    pub categories: BTreeMap<u32, Category>,
    pub orders: BTreeMap<u32, Order>,
    pub reviews: Vec<ProductReview>,
}

pub struct InitialDataSetConfig {
    pub number_of_customers: RangeInclusive<u32>,
    pub number_of_products: RangeInclusive<u32>,
    pub number_of_categories: RangeInclusive<u32>,
    pub number_of_orders: RangeInclusive<u32>,
    pub number_of_reviews: RangeInclusive<u32>,
}

impl InitialDataSetConfig {
    pub fn fake<R: Rng>(&self, rng: &mut R) -> InitialDataSet {
        let mut data = InitialDataSet::default();
        let mut customer_names = HashSet::new();
        for customer_id in 0..gen_range(rng, self.number_of_customers.clone()) {
            let customer = Customer::fake(rng, &customer_names);
            customer_names.insert(customer.name.clone());
            data.customers.insert(customer_id, customer);
        }
        for category_id in 0..gen_range(rng, self.number_of_categories.clone()) {
            data.categories.insert(category_id, Category::fake(rng));
        }
        let mut product_names = HashSet::new();
        for product_id in 0..gen_range(rng, self.number_of_products.clone()) {
            let product = Product::fake(rng, &data.categories, &product_names);
            product_names.insert(product.name.clone());
            data.products.insert(product_id, product);
        }
        let customer_ids = data.customers.keys().copied().collect::<Vec<_>>();
        let product_ids = data.products.keys().copied().collect::<Vec<_>>();
        for order_id in 0..gen_range(rng, self.number_of_orders.clone()) {
            data.orders
                .insert(order_id, Order::fake(rng, &customer_ids, &product_ids));
        }
        let products_available_to_rate = data
            .orders
            .values()
            .map(|order| {
                order
                    .product_ids
                    .iter()
                    .map(|&product_id| (order.customer_id, product_id))
            })
            .flatten()
            .collect::<Vec<_>>();
        let mut rated_products = HashSet::new();
        if !products_available_to_rate.is_empty() {
            for _ in 0..gen_range(rng, self.number_of_reviews.clone()) {
                if let Some(review) =
                    ProductReview::fake(rng, &products_available_to_rate, &mut rated_products)
                {
                    data.reviews.push(review);
                }
            }
        }

        data
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct BenchmarkReport {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Customer {
    pub name: String,
    pub email: String,
    pub address: String,
    pub city: String,
    pub region: String,
    pub country: String,
    pub postal_code: String,
    pub phone: String,
}

impl Customer {
    pub fn fake<R: Rng>(rng: &mut R, taken_names: &HashSet<String>) -> Self {
        let name = loop {
            let name = format!(
                "{} {}",
                Name(EN).fake_with_rng::<String, _>(rng),
                rng.gen::<u8>()
            );
            if !taken_names.contains(&name) {
                break name;
            }
        };
        Self {
            name,
            email: SafeEmail(EN).fake_with_rng(rng),
            address: format!(
                "{} {} {}",
                BuildingNumber(EN).fake_with_rng::<String, _>(rng),
                StreetName(EN).fake_with_rng::<String, _>(rng),
                StreetSuffix(EN).fake_with_rng::<String, _>(rng)
            ),
            city: CityName(EN).fake_with_rng(rng),
            region: StateName(EN).fake_with_rng(rng),
            country: CountryCode(EN).fake_with_rng(rng),
            postal_code: PostCode(EN).fake_with_rng(rng),
            phone: PhoneNumber(EN).fake_with_rng(rng),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Product {
    pub name: String,
    pub category_ids: Vec<u32>,
}

impl Product {
    pub fn fake<R: Rng>(
        rng: &mut R,
        available_categories: &BTreeMap<u32, Category>,
        taken_names: &HashSet<String>,
    ) -> Self {
        let mut available_category_ids = available_categories.keys().copied().collect::<Vec<_>>();
        let number_of_categories = if available_category_ids.is_empty() {
            0
        } else {
            rng.gen_range(0..(available_category_ids.len() + 1) / 2)
        };
        let mut category_ids = Vec::with_capacity(number_of_categories);
        for _ in 0..number_of_categories {
            let category_index = rng.gen_range(0..available_category_ids.len());
            category_ids.push(available_category_ids.remove(category_index));
        }

        let name = loop {
            let name = format!(
                "{} {}",
                Bs(EN).fake_with_rng::<String, _>(rng),
                rng.gen::<u8>()
            );
            if !taken_names.contains(&name) {
                break name;
            }
        };

        Self { name, category_ids }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Category {
    pub name: String,
}

impl Category {
    pub fn fake<R: Rng>(rng: &mut R) -> Self {
        Self {
            name: BsAdj(EN).fake_with_rng(rng),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Order {
    pub customer_id: u32,
    pub product_ids: Vec<u32>,
}

impl Order {
    pub fn fake<R: Rng>(
        rng: &mut R,
        available_customers: &[u32],
        available_products: &[u32],
    ) -> Self {
        let mut available_product_ids = available_products.to_vec();
        let number_of_products = if available_product_ids.is_empty() {
            0
        } else {
            rng.gen_range(0..((available_product_ids.len() + 2) / 3).min(20))
        };
        let mut product_ids = Vec::with_capacity(number_of_products);
        for _ in 0..number_of_products {
            let product_index = rng.gen_range(0..available_product_ids.len());
            product_ids.push(available_product_ids.remove(product_index));
        }

        Self {
            customer_id: *available_customers.choose(rng).unwrap(),
            product_ids,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Cart {
    pub customer_id: Option<u32>,
    pub product_ids: Vec<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProductReview {
    pub customer_id: u32,
    pub product_id: u32,
    pub rating: u8,
    pub review: Option<String>,
}

impl ProductReview {
    pub fn fake<R: Rng>(
        rng: &mut R,
        available_customer_products: &[(u32, u32)],
        already_rated: &mut HashSet<(u32, u32)>,
    ) -> Option<Self> {
        while available_customer_products.len() > already_rated.len() {
            let index = rng.gen_range(0..available_customer_products.len());
            let (customer_id, product_id) = available_customer_products[index];
            if already_rated.insert((customer_id, product_id)) {
                return Some(Self {
                    customer_id,
                    product_id,
                    rating: rng.gen_range(1..=5),
                    review: if rng.gen_bool(0.25) {
                        Some(Vec::<String>::dummy_with_rng(&Paragraphs(EN, 1..5), rng).join("\n\n"))
                    } else {
                        None
                    },
                });
            }
        }

        None
    }
}
