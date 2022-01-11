use std::ops::RangeInclusive;

use rand::{distributions::uniform::SampleUniform, Rng};

pub fn gen_range<S: Rng, T>(rng: &mut S, range: RangeInclusive<T>) -> T
where
    T: SampleUniform + PartialEq + PartialOrd + Clone,
{
    if range.start() == range.end() {
        range.start().clone()
    } else {
        rng.gen_range(range)
    }
}

pub fn format_nanoseconds(nanoseconds: f64) -> String {
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
