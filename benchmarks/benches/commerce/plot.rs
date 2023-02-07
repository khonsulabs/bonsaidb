use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::path::Path;
use std::time::Duration;

use plotters::coord::ranged1d::{NoDefaultFormatting, ValueFormatter};
use plotters::prelude::*;

use crate::utils::format_nanoseconds;
pub fn overview_graph(
    summaries: BTreeMap<usize, Vec<BTreeMap<&'static str, Duration>>>,
    plot_dir: impl AsRef<Path>,
) {
    let chart_path = plot_dir.as_ref().join("Overview.png");
    let chart_root = BitMapBackend::new(&chart_path, (800, 480)).into_drawing_area();
    chart_root.fill(&BACKGROUND_COLOR).unwrap();

    let mut results_by_backend = BTreeMap::new();
    let highest_concurrency = *summaries.keys().max().unwrap();
    for (concurrency, reports) in summaries {
        for report in reports {
            for (backend, wall_time) in report {
                let results = results_by_backend
                    .entry(backend)
                    .or_insert_with(BTreeMap::new);
                results
                    .entry(concurrency)
                    .and_modify(|total: &mut Nanos| total.0 += wall_time.as_nanos() as u64)
                    .or_insert_with(|| Nanos(wall_time.as_nanos() as u64));
            }
        }
    }
    let longest_measurement = Iterator::max(
        results_by_backend
            .values()
            .flat_map(|results| results.values().copied()),
    )
    .unwrap();
    let mut chart = ChartBuilder::on(&chart_root)
        .caption(
            "Commerce Benchmark Suite Overview",
            ("sans-serif", 30., &TEXT_COLOR),
        )
        .margin_left(10)
        .margin_right(50)
        .margin_bottom(10)
        .x_label_area_size(50)
        .y_label_area_size(80)
        .build_cartesian_2d(
            1..highest_concurrency,
            NanosRange(Nanos(0)..=longest_measurement),
        )
        .unwrap();

    chart
        .configure_mesh()
        .disable_x_mesh()
        .x_desc("Number of Agents")
        .y_desc("Total Wall Time")
        .axis_desc_style(("sans-serif", 15, &TEXT_COLOR))
        .x_label_style(&TEXT_COLOR)
        .y_label_style(&TEXT_COLOR)
        .light_line_style(TEXT_COLOR.mix(0.1))
        .bold_line_style(TEXT_COLOR.mix(0.3))
        .draw()
        .unwrap();

    for (backend, points) in results_by_backend {
        chart
            .draw_series(LineSeries::new(points.into_iter(), label_to_color(backend)))
            .unwrap()
            .label(backend.to_string())
            .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], label_to_color(backend)));
    }
    chart
        .configure_series_labels()
        .border_style(TEXT_COLOR)
        .background_style(BACKGROUND_COLOR)
        .label_font(&TEXT_COLOR)
        .position(SeriesLabelPosition::UpperLeft)
        .draw()
        .unwrap();
    chart_root.present().unwrap();
}

#[derive(Clone, Debug)]
pub struct NanosRange(RangeInclusive<Nanos>);
#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub struct Nanos(u64);

impl ValueFormatter<Nanos> for NanosRange {
    fn format(value: &Nanos) -> String {
        format_nanoseconds(value.0 as f64)
    }
}

impl Ranged for NanosRange {
    type FormatOption = NoDefaultFormatting;
    type ValueType = Nanos;

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

pub fn label_to_color(label: &str) -> RGBColor {
    match label {
        "bonsaidb-local" => COLORS[0],
        "bonsaidb-local+lz4" => COLORS[6],
        "bonsaidb-quic" => COLORS[1],
        "bonsaidb-ws" => COLORS[2],
        "postgresql" => COLORS[3],
        "sqlite" => COLORS[4],
        "mongodb" => COLORS[5],
        _ => panic!("Unknown label: {label}"),
    }
}

// https://coolors.co/dc0ab4-50e991-00bfa0-3355ff-9b19f5-ffa300-e60049-0bb4ff-e6d800
pub const COLORS: [RGBColor; 9] = [
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

pub const BACKGROUND_COLOR: RGBColor = RGBColor(0, 0, 0);
pub const TEXT_COLOR: RGBColor = RGBColor(200, 200, 200);
