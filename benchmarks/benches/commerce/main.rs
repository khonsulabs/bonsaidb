//! Commerce Benchmark
//!
//! For more information about this benchmark, see ./README.md.

use std::{collections::BTreeMap, io::ErrorKind, sync::Arc, time::Duration};

use clap::Parser;
use serde::{Deserialize, Serialize};
use tera::{Context, Tera};

use crate::{
    model::InitialDataSetConfig,
    utils::{current_timestamp_string, format_nanoseconds, local_git_rev},
};

mod bonsai;
mod plan;
#[cfg(feature = "postgresql")]
mod postgres;
mod utils;
use plan::ShopperPlanConfig;
mod execute;
mod model;
mod plot;

#[derive(Debug, Parser)]
struct Options {
    #[clap(long = "bench")]
    run_from_cargo_bench: bool,
    #[clap(long = "nocapture")]
    _cargo_bench_compat_nocapture: bool,

    benchmark_name: Option<String>,

    #[clap(long)]
    suite: bool,

    #[clap(long, short, default_value = "Commerce Benchmark")]
    label: String,

    #[clap(long, short)]
    seed: Option<u64>,

    #[clap(long, default_value = "1")]
    min_customers: u32,
    #[clap(long, default_value = "500")]
    max_customers: u32,

    #[clap(long, default_value = "1")]
    min_products: u32,
    #[clap(long, default_value = "200")]
    max_products: u32,

    #[clap(long, default_value = "1")]
    min_categories: u32,
    #[clap(long, default_value = "30")]
    max_categories: u32,

    #[clap(long, default_value = "10")]
    min_orders: u32,
    #[clap(long, default_value = "1000")]
    max_orders: u32,

    #[clap(long, default_value = "10")]
    min_reviews: u32,
    #[clap(long, default_value = "500")]
    max_reviews: u32,

    #[clap(long, default_value = "0.25")]
    add_to_cart_chance: f64,
    #[clap(long, default_value = "0.25")]
    purchase_chance: f64,
    #[clap(long, default_value = "0.25")]
    rating_chance: f64,

    #[clap(long, default_value = "1")]
    min_searches: u32,
    #[clap(long, default_value = "20")]
    max_searches: u32,

    #[clap(long, short = 'j')]
    agents: Option<usize>,

    #[clap(long, short = 'n')]
    shoppers: Option<usize>,
}

fn main() {
    match dotenv::dotenv() {
        Ok(_) => {}
        Err(dotenv::Error::Io(err)) if err.kind() == ErrorKind::NotFound => {}
        Err(other) => panic!("Error with .env file: {}", other),
    }
    let tera = Arc::new(tera::Tera::new("benches/commerce/templates/**/*").unwrap());
    let options = Options::parse();
    let benchmark_name = options.benchmark_name.unwrap_or_default();

    let is_testing = !options.run_from_cargo_bench;

    if options.suite || is_testing {
        run_standard_benchmarks(
            tera.clone(),
            &benchmark_name,
            options.shoppers,
            options.run_from_cargo_bench,
        );
    }

    if !options.suite || is_testing {
        execute::Benchmark {
            label: options.label,
            seed: options.seed,
            agents: options.agents,
            shoppers: options.shoppers,
            data_config: &InitialDataSetConfig {
                number_of_customers: options.min_customers..=options.max_customers,
                number_of_products: options.min_products..=options.max_products,
                number_of_categories: options.min_categories..=options.max_categories,
                number_of_orders: options.min_orders..=options.max_orders,
                number_of_reviews: options.min_reviews..=options.max_reviews,
            },
            shopper_config: &ShopperPlanConfig {
                chance_of_adding_product_to_cart: options.add_to_cart_chance,
                chance_of_purchasing: options.purchase_chance,
                chance_of_rating: options.rating_chance,
                product_search_attempts: options.min_searches..=options.max_searches,
            },
        }
        .execute(&benchmark_name, "commerce-bench", tera);
    }
}

fn run_standard_benchmarks(
    tera: Arc<Tera>,
    name_filter: &str,
    shoppers: Option<usize>,
    run_full: bool,
) {
    let shoppers = shoppers.unwrap_or(100);

    let mut initial_datasets = vec![(
        "small",
        InitialDataSetConfig {
            number_of_customers: 100..=100,
            number_of_products: 100..=100,
            number_of_categories: 10..=10,
            number_of_orders: 125..=125,
            number_of_reviews: 50..=50,
        },
    )];
    if run_full {
        initial_datasets.push((
            "medium",
            InitialDataSetConfig {
                number_of_customers: 1_000..=1_000,
                number_of_products: 1_000..=1_000,
                number_of_categories: 50..=50,
                number_of_orders: 1500..=1500,
                number_of_reviews: 500..=500,
            },
        ));
        initial_datasets.push((
            "large",
            InitialDataSetConfig {
                number_of_customers: 5_000..=5_000,
                number_of_products: 5_000..=5_000,
                number_of_categories: 100..=100,
                number_of_orders: 5_000..=5_000,
                number_of_reviews: 1_000..=1_000,
            },
        ));
    }

    let mut shopper_plans = vec![(
        "balanced",
        ShopperPlanConfig {
            chance_of_adding_product_to_cart: 0.25,
            chance_of_purchasing: 0.25,
            chance_of_rating: 0.25,
            product_search_attempts: 1..=10,
        },
    )];
    if run_full {
        shopper_plans.push((
            "readheavy",
            ShopperPlanConfig {
                chance_of_adding_product_to_cart: 0.1,
                chance_of_purchasing: 0.1,
                chance_of_rating: 0.1,
                product_search_attempts: 1..=10,
            },
        ));
        shopper_plans.push((
            "writeheavy",
            ShopperPlanConfig {
                chance_of_adding_product_to_cart: 0.9,
                chance_of_purchasing: 0.9,
                chance_of_rating: 0.9,
                product_search_attempts: 1..=10,
            },
        ));
    }

    let mut number_of_agents = vec![1];
    let num_cpus = num_cpus::get();
    if num_cpus > 1 {
        number_of_agents.push(num_cpus);
    }
    number_of_agents.push(num_cpus * 2);

    let mut datasets = Vec::new();
    let mut summaries = BTreeMap::<usize, Vec<BTreeMap<&'static str, Duration>>>::new();
    for (dataset_label, data_config) in &initial_datasets {
        for (plan_label, shopper_config) in &shopper_plans {
            println!(
                "Running standard benchmark {}-{}",
                dataset_label, plan_label
            );
            for &concurrency in &number_of_agents {
                let summaries = summaries.entry(concurrency).or_default();
                let measurements = execute::Benchmark {
                    label: format!(
                        "{}, {}, {} agent(s)",
                        dataset_label, plan_label, concurrency
                    ),
                    seed: Some(0),
                    agents: Some(concurrency),
                    shoppers: Some(shoppers),
                    data_config,
                    shopper_config,
                }
                .execute(
                    name_filter,
                    format!(
                        "./commerce-bench/{}-{}/{}/",
                        dataset_label, plan_label, concurrency
                    ),
                    tera.clone(),
                );
                datasets.push(DataSet {
                    size: dataset_label.to_string(),
                    pattern: plan_label.to_string(),
                    concurrency: concurrency.to_string(),
                    path: format!(
                        "{}-{}/{}/index.html",
                        dataset_label, plan_label, concurrency
                    ),
                    results: measurements
                        .iter()
                        .map(|(k, v)| (k.to_string(), format_nanoseconds(v.as_nanos() as f64)))
                        .collect(),
                });
                summaries.push(measurements);
            }
        }
    }

    plot::overview_graph(summaries, "./commerce-bench");

    std::fs::write(
        "./commerce-bench/index.html",
        tera.render(
            "overview.html",
            &Context::from_serialize(&Overview {
                datasets,
                timestamp: current_timestamp_string(),
                revision: local_git_rev(),
            })
            .unwrap(),
        )
        .unwrap()
        .as_bytes(),
    )
    .unwrap();
}

#[derive(Debug, Serialize, Deserialize)]
struct Overview {
    datasets: Vec<DataSet>,
    timestamp: String,
    revision: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct DataSet {
    size: String,
    pattern: String,
    concurrency: String,
    path: String,
    results: BTreeMap<String, String>,
}
