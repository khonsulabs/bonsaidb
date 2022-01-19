use std::{
    collections::{BTreeMap, HashMap},
    ops::RangeInclusive,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use bonsaidb::core::async_trait::async_trait;
use cli_table::{Cell, Table};
use futures::{stream::FuturesUnordered, StreamExt};
use hdrhistogram::Histogram;
use plotters::{
    coord::ranged1d::{NoDefaultFormatting, ValueFormatter},
    element::{BackendCoordOnly, CoordMapper, Drawable, PointCollection},
    prelude::*,
};
use plotters_backend::DrawingErrorKind;
use rand::{rngs::SmallRng, SeedableRng};
use serde::{Deserialize, Serialize};
use tera::Tera;
use tokio::runtime::Runtime;

use crate::{
    bonsai::{Bonsai, BonsaiBackend},
    model::{InitialDataSet, InitialDataSetConfig},
    plan::{
        AddProductToCart, Checkout, CreateCart, FindProduct, Load, LookupProduct, Operation,
        OperationResult, Plan, ReviewProduct, ShopperPlanConfig,
    },
    plot::{BACKGROUND_COLOR, TEXT_COLOR},
    utils::{current_timestamp_string, local_git_rev},
};

pub fn execute_plans_for_all_backends(
    name_filter: &str,
    plans: &[Arc<Plan>],
    initial_data: &Arc<InitialDataSet>,
    number_of_agents: usize,
    measurements: &Measurements,
) {
    if name_filter.is_empty()
        || name_filter == "bonsaidb"
        || name_filter.starts_with("bonsaidb-local")
    {
        println!("Executing bonsaidb-local");
        BonsaiBackend::execute_async(
            Bonsai::Local,
            plans,
            initial_data,
            number_of_agents,
            measurements,
        );
    }
    if name_filter.is_empty()
        || name_filter == "bonsaidb"
        || name_filter.starts_with("bonsaidb-quic")
    {
        println!("Executing bonsaidb-quic");
        BonsaiBackend::execute_async(
            Bonsai::Quic,
            plans,
            initial_data,
            number_of_agents,
            measurements,
        );
    }
    if name_filter.is_empty() || name_filter == "bonsaidb" || name_filter.starts_with("bonsaidb-ws")
    {
        println!("Executing bonsaidb-ws");
        BonsaiBackend::execute_async(
            Bonsai::WebSockets,
            plans,
            initial_data,
            number_of_agents,
            measurements,
        );
    }
    #[cfg(feature = "postgresql")]
    if name_filter.is_empty() || name_filter.starts_with("postgresql") {
        if let Ok(url) = std::env::var("COMMERCE_POSTGRESQL_URL") {
            println!("Executing postgresql");
            crate::postgres::Postgres::execute_async(
                url,
                plans,
                initial_data,
                number_of_agents,
                measurements,
            );
        } else {
            eprintln!("postgresql feature is enabled, but environment variable COMMERCE_POSTGRESQL_URL is missing.");
        }
    }
}

#[async_trait]
pub trait Backend: Sized + Send + Sync + 'static {
    type Operator: BackendOperator;
    type Config: Send + Sync;

    async fn new(config: Self::Config) -> Self;

    fn label(&self) -> &'static str;

    fn execute_async(
        config: Self::Config,
        plans: &[Arc<Plan>],
        initial_data: &Arc<InitialDataSet>,
        concurrent_agents: usize,
        measurements: &Measurements,
    ) {
        let (plan_sender, plan_receiver) = flume::bounded(concurrent_agents * 2);
        let runtime = Runtime::new().unwrap();
        let backend = runtime.block_on(Self::new(config));
        // Load the initial data
        println!("Loading data");
        runtime.block_on(async {
            let _ = backend
                .new_operator_async()
                .await
                .operate(
                    &Load {
                        initial_data: initial_data.clone(),
                    },
                    &[],
                    measurements,
                )
                .await;
        });
        println!("Executing plans");
        let agent_handles = FuturesUnordered::new();
        for _ in 0..concurrent_agents {
            let operator = runtime.block_on(backend.new_operator_async());
            agent_handles.push(runtime.spawn(agent::<Self>(
                operator,
                plan_receiver.clone(),
                measurements.clone(),
            )));
        }
        runtime.block_on(async {
            // Send the plans to the channel that the agents are waiting for
            // them on.
            for plan in plans {
                plan_sender.send_async(plan.clone()).await.unwrap();
            }
            // Disconnect the receivers, allowing the agents to exit once there
            // are no more plans in queue.
            drop(plan_sender);
            // Wait for each of the agents to return.
            for result in agent_handles.collect::<Vec<_>>().await {
                result.unwrap();
            }
        })
    }

    async fn new_operator_async(&self) -> Self::Operator;
}

#[async_trait]
pub trait Operator<T> {
    async fn operate(
        &mut self,
        operation: &T,
        results: &[OperationResult],
        measurements: &Measurements,
    ) -> OperationResult;
}

async fn agent<B: Backend>(
    mut operator: B::Operator,
    plan_receiver: flume::Receiver<Arc<Plan>>,
    measurements: Measurements,
) {
    while let Ok(plan) = plan_receiver.recv_async().await {
        let mut results = Vec::with_capacity(plan.operations.len());
        for step in &plan.operations {
            results.push(operator.operate(step, &results, &measurements).await)
        }
    }
}

pub trait BackendOperator:
    Operator<Load>
    + Operator<LookupProduct>
    + Operator<FindProduct>
    + Operator<CreateCart>
    + Operator<AddProductToCart>
    + Operator<ReviewProduct>
    + Operator<Checkout>
    + Send
    + Sync
{
}

#[async_trait]
impl<T> Operator<Operation> for T
where
    T: BackendOperator,
{
    async fn operate(
        &mut self,
        operation: &Operation,
        results: &[OperationResult],
        measurements: &Measurements,
    ) -> OperationResult {
        match operation {
            Operation::FindProduct(op) => self.operate(op, results, measurements).await,
            Operation::LookupProduct(op) => self.operate(op, results, measurements).await,
            Operation::CreateCart(op) => self.operate(op, results, measurements).await,
            Operation::AddProductToCart(op) => self.operate(op, results, measurements).await,
            Operation::RateProduct(op) => self.operate(op, results, measurements).await,
            Operation::Checkout(op) => self.operate(op, results, measurements).await,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BenchmarkSummary {
    label: String,
    timestamp: String,
    revision: String,
    product_count: usize,
    category_count: usize,
    customer_count: usize,
    order_count: usize,
    summaries: Vec<BackendSummary>,
    operations: Vec<MetricSummary>,
}

impl BenchmarkSummary {
    pub fn render_to(&self, location: &Path, tera: &Tera) {
        std::fs::write(
            location,
            tera.render("run.html", &tera::Context::from_serialize(self).unwrap())
                .unwrap()
                .as_bytes(),
        )
        .unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BackendSummary {
    backend: String,
    transport: String,
    total_time: String,
    wall_time: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MetricSummary {
    metric: Metric,
    description: String,
    summaries: Vec<OperationSummary>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OperationSummary {
    backend: String,
    avg: String,
    min: String,
    max: String,
    stddev: String,
}

pub struct Benchmark<'a> {
    pub label: String,
    pub seed: Option<u64>,
    pub agents: Option<usize>,
    pub shoppers: Option<usize>,
    pub data_config: &'a InitialDataSetConfig,
    pub shopper_config: &'a ShopperPlanConfig,
}

impl<'a> Benchmark<'a> {
    pub fn execute(
        self,
        name_filter: &str,
        plot_dir: impl AsRef<Path>,
        tera: Arc<Tera>,
    ) -> BTreeMap<&'static str, Duration> {
        let plot_dir = plot_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&plot_dir).unwrap();

        let mut rng = if let Some(seed) = self.seed {
            SmallRng::seed_from_u64(seed)
        } else {
            SmallRng::from_entropy()
        };
        let initial_data = Arc::new(self.data_config.fake(&mut rng));
        let number_of_agents = self.agents.unwrap_or_else(num_cpus::get);
        let shoppers = self.shoppers.unwrap_or(number_of_agents * 100);

        println!(
            "Running {} plans across {} agents",
            shoppers, number_of_agents
        );
        // Generate plans to execute.
        let mut plans = Vec::with_capacity(shoppers as usize);
        for _ in 0..shoppers {
            plans.push(Arc::new(
                self.shopper_config.random_plan(&mut rng, &initial_data),
            ));
        }
        // Set up our statistics gathering thread
        let (metric_sender, metric_receiver) = flume::unbounded();
        let measurements = Measurements {
            sender: metric_sender,
        };
        let thread_initial_data = initial_data.clone();
        let stats_thread = std::thread::spawn(move || {
            stats_thread(
                self.label,
                metric_receiver,
                number_of_agents,
                &thread_initial_data,
                &plot_dir,
                &tera,
            )
        });
        // Perform all benchmarks
        execute_plans_for_all_backends(
            name_filter,
            &plans,
            &initial_data,
            number_of_agents,
            &measurements,
        );
        // Drop the measurements instance to allow the stats thread to know
        // there are no more metrics coming.
        drop(measurements);
        // Wait for the statistics thread to report all the results.
        stats_thread.join().unwrap()
    }
}

#[derive(Clone)]
pub struct Measurements {
    sender: flume::Sender<(&'static str, Metric, Duration)>,
}

impl Measurements {
    pub fn begin(&self, label: &'static str, metric: Metric) -> Measurement<'_> {
        Measurement {
            target: &self.sender,
            label,
            metric,
            start: Instant::now(),
        }
    }
}

pub struct Measurement<'a> {
    target: &'a flume::Sender<(&'static str, Metric, Duration)>,
    label: &'static str,
    metric: Metric,
    start: Instant,
}

impl<'a> Measurement<'a> {
    pub fn finish(self) {
        let duration = Instant::now()
            .checked_duration_since(self.start)
            .expect("time went backwards. Restart benchmarks.");
        self.target
            .send((self.label, self.metric, duration))
            .unwrap();
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Hash, Eq, PartialEq, Debug, Ord, PartialOrd)]
pub enum Metric {
    Load,
    LookupProduct,
    FindProduct,
    CreateCart,
    AddProductToCart,
    Checkout,
    RateProduct,
}

impl Metric {
    pub fn description(&self) -> &'static str {
        match self {
            Metric::Load => "Measures the time spent loading the initial data set and performing any pre-cache operations that most database administrators would perform on their databases periodically to ensure good performance.",
            Metric::LookupProduct => "Meaures the time spent looking up a product by its id. This operation is meant to simulate the basic needs of the database to provide a product details page after a user clicked a direct link that contians the product's unique id, including the product's current rating.",
            Metric::FindProduct => "Measures the time spent looking up a product by its name (exact match, indexed). This operation is meant to simulate the basic needs of the database to provide a product details after finding a product by its name, including the product's current rating.",
            Metric::CreateCart => "Measures the time spent creating a shopping cart.",
            Metric::AddProductToCart => "Measures the time spent adding a product to a shopping cart.",
            Metric::RateProduct => "Measures the time spent adding or updating a review of a product by a customer. Each customer can only have one review per product. When this operation is complete, all subsequent calls to LookupProduct and FindProduct should reflect the new rating. This simulates an 'upsert' (insert or update) operation using a unique index.",
            Metric::Checkout => "Measures the time spent converting a shopping cart into an order for a customer."
        }
    }
}

fn format_nanoseconds(nanoseconds: f64) -> String {
    if nanoseconds <= f64::EPSILON {
        String::from("0s")
    } else if nanoseconds < 1_000. {
        format_float(nanoseconds, "ns")
    } else if nanoseconds < 1_000_000. {
        format_float(nanoseconds / 1_000., "us")
    } else if nanoseconds < 1_000_000_000. {
        format_float(nanoseconds / 1_000_000., "ms")
    } else if nanoseconds < 1_000_000_000_000. {
        format_float(nanoseconds / 1_000_000_000., "s")
    } else {
        // this hopefully is unreachable...
        format_float(nanoseconds / 1_000_000_000. / 60., "m")
    }
}

fn format_float(value: f64, suffix: &str) -> String {
    if value < 10. {
        format!("{:.3}{}", value, suffix)
    } else if value < 100. {
        format!("{:.2}{}", value, suffix)
    } else {
        format!("{:.1}{}", value, suffix)
    }
}

#[derive(Clone, Debug)]
struct NanosRange(RangeInclusive<Nanos>);
#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
struct Nanos(u64);

impl ValueFormatter<Nanos> for NanosRange {
    fn format(value: &Nanos) -> String {
        format_nanoseconds(value.0 as f64)
    }
}

impl Ranged for NanosRange {
    type ValueType = Nanos;
    type FormatOption = NoDefaultFormatting;

    fn map(&self, value: &Self::ValueType, limit: (i32, i32)) -> i32 {
        let limited_size = limit.1 - limit.0;
        let full_size = self.0.end().0 + 1 - self.0.start().0;
        let normalized_offset = value.0.saturating_sub(self.0.start().0) as f64 / full_size as f64;
        limit.0 + (normalized_offset * limited_size as f64) as i32
    }

    fn key_points<Hint: plotters::coord::ranged1d::KeyPointHint>(
        &self,
        hint: Hint,
    ) -> Vec<Self::ValueType> {
        let total_range = self.0.end().0 - self.0.start().0;
        let num_points = hint.max_num_points();
        let mut important_points = Vec::with_capacity(num_points);
        important_points.push(*self.0.start());
        if num_points > 2 {
            let steps = num_points - 2;
            let step_size = total_range as f64 / steps as f64;
            important_points.extend(
                (1..num_points - 1)
                    .map(|step| Nanos(self.0.start().0 + (step as f64 * step_size) as u64)),
            );
        }
        important_points.push(*self.0.end());

        important_points
    }

    fn range(&self) -> std::ops::Range<Self::ValueType> {
        Nanos(self.0.start().0)..Nanos(self.0.end().0 + 1)
    }
}

impl DiscreteRanged for NanosRange {
    fn size(&self) -> usize {
        (self.0.end().0 - self.0.start().0) as usize
    }

    fn index_of(&self, value: &Self::ValueType) -> Option<usize> {
        if value.0 <= self.0.end().0 {
            if let Some(index) = value.0.checked_sub(self.0.start().0) {
                return Some(index as usize);
            }
        }
        None
    }

    fn from_index(&self, index: usize) -> Option<Self::ValueType> {
        Some(Nanos(self.0.start().0 + index as u64))
    }
}

fn label_to_color(label: &str) -> RGBColor {
    match label {
        "bonsaidb-local" => COLORS[0],
        "bonsaidb-quic" => COLORS[1],
        "bonsaidb-ws" => COLORS[2],
        "postgresql" => COLORS[3],
        "sqlite" => COLORS[4],
        _ => panic!("Unknown label: {}", label),
    }
}

// https://coolors.co/dc0ab4-50e991-00bfa0-3355ff-9b19f5-ffa300-e60049-0bb4ff-e6d800
const COLORS: [RGBColor; 9] = [
    RGBColor(220, 10, 180),
    RGBColor(80, 233, 145),
    RGBColor(0, 191, 160),
    RGBColor(51, 85, 255),
    RGBColor(155, 25, 245),
    RGBColor(255, 163, 0),
    RGBColor(230, 0, 73),
    RGBColor(11, 180, 255),
    RGBColor(230, 216, 0),
];

fn stats_thread(
    label: String,
    metric_receiver: flume::Receiver<(&'static str, Metric, Duration)>,
    number_of_agents: usize,
    initial_data: &InitialDataSet,
    plot_dir: &Path,
    tera: &Tera,
) -> BTreeMap<&'static str, Duration> {
    let mut all_results: BTreeMap<Metric, BTreeMap<&'static str, Vec<u64>>> = BTreeMap::new();
    let mut accumulated_label_stats: BTreeMap<&'static str, Duration> = BTreeMap::new();
    let mut longest_by_metric = HashMap::new();
    while let Ok((label, metric, duration)) = metric_receiver.recv() {
        let metric_results = all_results.entry(metric).or_default();
        let label_results = metric_results.entry(label).or_default();
        let nanos = u64::try_from(duration.as_nanos()).unwrap();
        label_results.push(nanos);
        let label_duration = accumulated_label_stats.entry(label).or_default();
        longest_by_metric
            .entry(metric)
            .and_modify(|existing: &mut Duration| {
                *existing = (*existing).max(duration);
            })
            .or_insert(duration);
        *label_duration += duration;
    }

    let mut operations = BTreeMap::new();
    for (metric, label_metrics) in all_results {
        let label_histograms = label_metrics
            .iter()
            .map(|(label, stats)| {
                let mut histogram = Histogram::<u64>::new(3).unwrap();
                for &nanos in stats {
                    histogram.record(nanos).unwrap();
                }
                (label, histogram)
            })
            .collect::<BTreeMap<_, _>>();
        println!(
            "{:?}: {} operations",
            metric,
            label_metrics.values().next().unwrap().len()
        );
        cli_table::print_stdout(
            label_histograms
                .iter()
                .map(|(label, stats)| {
                    vec![
                        label.cell(),
                        format_nanoseconds(stats.mean()).cell(),
                        format_nanoseconds(stats.min() as f64).cell(),
                        format_nanoseconds(stats.max() as f64).cell(),
                        format_nanoseconds(stats.stdev()).cell(),
                    ]
                })
                .table()
                .title(vec![
                    "Backend".cell(),
                    "Avg".cell(),
                    "Min".cell(),
                    "Max".cell(),
                    "StdDev".cell(),
                ]),
        )
        .unwrap();

        let longest_measurement = longest_by_metric.get(&metric).unwrap();
        let group_width = longest_measurement.as_nanos() as u64 / 64;
        let mut label_chart_data = BTreeMap::new();
        for (label, metrics) in label_histograms.iter() {
            let report = operations.entry(metric).or_insert_with(|| MetricSummary {
                metric,
                description: metric.description().to_string(),
                summaries: Vec::new(),
            });
            report.summaries.push(OperationSummary {
                backend: label.to_string(),
                avg: format_nanoseconds(metrics.mean()),
                min: format_nanoseconds(metrics.min() as f64),
                max: format_nanoseconds(metrics.max() as f64),
                stddev: format_nanoseconds(metrics.stdev()),
            });
            let chart_data = HistogramBars {
                bars: metrics
                    .iter_linear(group_width)
                    .map(|entry| {
                        let count = entry.count_since_last_iteration();

                        HistogramBar::new(
                            (Nanos(entry.value_iterated_to()), count),
                            group_width,
                            label_to_color(label),
                        )
                    })
                    .collect::<Vec<_>>(),
            };
            label_chart_data.insert(label, chart_data);
        }
        let highest_count = Iterator::max(
            label_chart_data
                .values()
                .flat_map(|chart_data| chart_data.bars.iter().map(|bar| bar.upper_left.1)),
        )
        .unwrap();
        for (label, chart_data) in label_chart_data {
            println!("Plotting {}: {:?}", label, metric);
            let chart_path = plot_dir.join(format!("{}-{:?}.png", label, metric));
            let chart_root = BitMapBackend::new(&chart_path, (800, 240)).into_drawing_area();
            chart_root.fill(&BACKGROUND_COLOR).unwrap();
            let mut chart = ChartBuilder::on(&chart_root)
                .caption(
                    format!("{}: {:?}", label, metric),
                    ("sans-serif", 30., &TEXT_COLOR),
                )
                .margin_left(10)
                .margin_right(50)
                .margin_bottom(10)
                .x_label_area_size(50)
                .y_label_area_size(80)
                .build_cartesian_2d(
                    NanosRange(Nanos(0)..=Nanos(longest_measurement.as_nanos() as u64)),
                    0..highest_count + 1,
                )
                .unwrap();

            chart
                .configure_mesh()
                .disable_x_mesh()
                .y_desc("Count")
                .x_desc("Execution Time")
                .axis_desc_style(("sans-serif", 15, &TEXT_COLOR))
                .x_label_style(&TEXT_COLOR)
                .y_label_style(&TEXT_COLOR)
                .light_line_style(&TEXT_COLOR.mix(0.1))
                .bold_line_style(&TEXT_COLOR.mix(0.3))
                .draw()
                .unwrap();

            chart.draw_series(chart_data).unwrap();
            chart_root.present().unwrap();
        }
        let label_lines = label_metrics
            .iter()
            .map(|(label, stats)| {
                let mut running_data = Vec::new();
                let mut elapsed = 0;
                for (index, &nanos) in stats.iter().enumerate() {
                    elapsed += nanos;
                    running_data.push((index, Nanos(elapsed)));
                }
                (label, running_data)
            })
            .collect::<BTreeMap<_, _>>();
        let metric_chart_path = plot_dir.join(format!("{:?}.png", metric));
        let metric_chart_root =
            BitMapBackend::new(&metric_chart_path, (800, 480)).into_drawing_area();
        metric_chart_root.fill(&BACKGROUND_COLOR).unwrap();
        let mut metric_chart = ChartBuilder::on(&metric_chart_root)
            .caption(format!("{:?}", metric), ("sans-serif", 30., &TEXT_COLOR))
            .margin_left(10)
            .margin_right(50)
            .margin_bottom(10)
            .x_label_area_size(50)
            .y_label_area_size(80)
            .build_cartesian_2d(
                0..label_lines
                    .iter()
                    .map(|(_, data)| data.len())
                    .max()
                    .unwrap(),
                NanosRange(
                    Nanos(0)
                        ..=label_lines
                            .iter()
                            .map(|(_, stats)| stats.last().unwrap().1)
                            .max()
                            .unwrap(),
                ),
            )
            .unwrap();

        metric_chart
            .configure_mesh()
            .disable_x_mesh()
            .x_desc("Invocations")
            .y_desc("Accumulated Execution Time")
            .axis_desc_style(("sans-serif", 15, &TEXT_COLOR))
            .x_label_style(&TEXT_COLOR)
            .y_label_style(&TEXT_COLOR)
            .light_line_style(&TEXT_COLOR.mix(0.1))
            .bold_line_style(&TEXT_COLOR.mix(0.3))
            .draw()
            .unwrap();

        for (label, data) in label_lines {
            metric_chart
                .draw_series(LineSeries::new(data.into_iter(), &label_to_color(label)))
                .unwrap()
                .label(label.to_string())
                .legend(|(x, y)| {
                    PathElement::new(vec![(x, y), (x + 20, y)], &label_to_color(label))
                });
        }
        metric_chart
            .configure_series_labels()
            .border_style(&TEXT_COLOR)
            .background_style(&BACKGROUND_COLOR)
            .label_font(&TEXT_COLOR)
            .position(SeriesLabelPosition::UpperLeft)
            .draw()
            .unwrap();
        metric_chart_root.present().unwrap();
    }
    cli_table::print_stdout(
        accumulated_label_stats
            .iter()
            .map(|(label, duration)| {
                vec![
                    label.cell(),
                    format_nanoseconds(duration.as_nanos() as f64).cell(),
                ]
            })
            .table()
            .title(vec![
                "Backend".cell(),
                format!("Total Execution Time across {} agents", number_of_agents).cell(),
            ]),
    )
    .unwrap();
    BenchmarkSummary {
        label,
        timestamp: current_timestamp_string(),
        revision: local_git_rev(),
        product_count: initial_data.products.len(),
        category_count: initial_data.categories.len(),
        customer_count: initial_data.customers.len(),
        order_count: initial_data.orders.len(),
        summaries: accumulated_label_stats
            .iter()
            .map(|(&backend, duration)| BackendSummary {
                backend: backend.to_string(),
                transport: match backend {
                    "bonsaidb-local" => String::from("None"),
                    "bonsaidb-quic" => String::from("UDP with TLS"),
                    _ => String::from("TCP"),
                },
                total_time: format_nanoseconds(duration.as_nanos() as f64),
                wall_time: format_nanoseconds(duration.as_nanos() as f64 / number_of_agents as f64),
            })
            .collect(),
        operations: operations.into_iter().map(|(_k, v)| v).collect(),
    }
    .render_to(&plot_dir.join("index.html"), tera);
    accumulated_label_stats
}

struct HistogramBars {
    bars: Vec<HistogramBar>,
}

struct HistogramBar {
    upper_left: (Nanos, u64),
    lower_right: (Nanos, u64),

    color: RGBColor,
}

impl HistogramBar {
    pub fn new(coord: (Nanos, u64), width: u64, color: RGBColor) -> Self {
        Self {
            upper_left: (Nanos(coord.0 .0 - width / 2), coord.1),
            lower_right: (Nanos(coord.0 .0 + width / 2), 0),
            color,
        }
    }
}

impl<'a> Drawable<BitMapBackend<'a>> for HistogramBar {
    fn draw<I: Iterator<Item = <BackendCoordOnly as CoordMapper>::Output>>(
        &self,
        mut pos: I,
        backend: &mut BitMapBackend,
        _parent_dim: (u32, u32),
    ) -> Result<(), DrawingErrorKind<<BitMapBackend as DrawingBackend>::ErrorType>> {
        let upper_left = pos.next().unwrap();
        let lower_right = pos.next().unwrap();
        backend.draw_rect(upper_left, lower_right, &self.color, true)?;

        Ok(())
    }
}

impl<'a> PointCollection<'a, (Nanos, u64)> for &'a HistogramBar {
    type Point = &'a (Nanos, u64);

    type IntoIter = HistogramBarIter<'a>;

    fn point_iter(self) -> Self::IntoIter {
        HistogramBarIter::UpperLeft(self)
    }
}

enum HistogramBarIter<'a> {
    UpperLeft(&'a HistogramBar),
    LowerRight(&'a HistogramBar),
    Done,
}

impl<'a> Iterator for HistogramBarIter<'a> {
    type Item = &'a (Nanos, u64);

    fn next(&mut self) -> Option<Self::Item> {
        let (next, result) = match self {
            HistogramBarIter::UpperLeft(bar) => {
                (HistogramBarIter::LowerRight(bar), Some(&bar.upper_left))
            }
            HistogramBarIter::LowerRight(bar) => (HistogramBarIter::Done, Some(&bar.lower_right)),
            HistogramBarIter::Done => (HistogramBarIter::Done, None),
        };
        *self = next;
        result
    }
}

impl IntoIterator for HistogramBars {
    type Item = HistogramBar;

    type IntoIter = std::vec::IntoIter<HistogramBar>;

    fn into_iter(self) -> Self::IntoIter {
        self.bars.into_iter()
    }
}
