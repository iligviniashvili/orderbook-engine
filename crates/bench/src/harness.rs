//! A deliberately small measurement harness.
//!
//! It reports throughput and the amortised cost per operation, and nothing
//! else. That is a decision, not a gap: the operations here run in hundreds of
//! nanoseconds, and wrapping each one in a pair of `Instant::now()` calls to
//! collect a latency distribution would spend a meaningful fraction of the
//! measurement on measuring. A number that includes its own instrument is
//! worse than an honest average.
//!
//! What it does do is run a warm-up pass first (so the figure is not a report
//! on cold branch predictors and an empty allocator), repeat the timed pass
//! and keep the best one (so a scheduler hiccup does not become the published
//! number), and pass every input and result through `black_box` so the
//! optimiser cannot delete the work.

use std::fmt;
use std::hint::black_box;
use std::time::{Duration, Instant};

/// Timed passes per scenario. The best is reported: the slow ones are the
/// machine's noise, not the code's.
const PASSES: usize = 3;

#[derive(Debug, Clone)]
pub struct Measurement {
    pub name: &'static str,
    pub unit: &'static str,
    pub operations: u64,
    pub elapsed: Duration,
}

impl Measurement {
    pub fn per_second(&self) -> f64 {
        if self.elapsed.is_zero() {
            return f64::INFINITY;
        }
        self.operations as f64 / self.elapsed.as_secs_f64()
    }

    /// Wall time divided by operations. An average, not a percentile.
    pub fn nanos_per_operation(&self) -> f64 {
        if self.operations == 0 {
            return f64::NAN;
        }
        self.elapsed.as_nanos() as f64 / self.operations as f64
    }

    pub fn as_json(&self) -> String {
        format!(
            r#"{{"name":"{}","unit":"{}","operations":{},"elapsed_ms":{:.3},"per_second":{:.0},"ns_per_operation":{:.1}}}"#,
            self.name,
            self.unit,
            self.operations,
            self.elapsed.as_secs_f64() * 1_000.0,
            self.per_second(),
            self.nanos_per_operation(),
        )
    }
}

impl fmt::Display for Measurement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:<34} {:>12} {:>10.1} ms {:>14.0} {}/s {:>9.0} ns/op",
            self.name,
            self.operations,
            self.elapsed.as_secs_f64() * 1_000.0,
            self.per_second(),
            self.unit,
            self.nanos_per_operation(),
        )
    }
}

/// Runs `body` once to warm up, then `PASSES` times, keeping the fastest.
///
/// `body` takes the pass number so it can rebuild whatever state it consumes,
/// and returns how many operations it performed — which is not always the
/// number of inputs, since a fan-out delivers one message to many receivers.
pub fn measure(
    name: &'static str,
    unit: &'static str,
    mut body: impl FnMut(usize) -> u64,
) -> Measurement {
    black_box(body(usize::MAX));

    let mut best: Option<(u64, Duration)> = None;
    for pass in 0..PASSES {
        let started = Instant::now();
        let operations = black_box(body(pass));
        let elapsed = started.elapsed();

        if best.is_none_or(|(_, previous)| elapsed < previous) {
            best = Some((operations, elapsed));
        }
    }

    let (operations, elapsed) = best.expect("at least one pass runs");
    Measurement {
        name,
        unit,
        operations,
        elapsed,
    }
}

/// Async counterpart of [`measure`].
pub async fn measure_async<F, Fut>(
    name: &'static str,
    unit: &'static str,
    mut body: F,
) -> Measurement
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = u64>,
{
    black_box(body(usize::MAX).await);

    let mut best: Option<(u64, Duration)> = None;
    for pass in 0..PASSES {
        let started = Instant::now();
        let operations = black_box(body(pass).await);
        let elapsed = started.elapsed();

        if best.is_none_or(|(_, previous)| elapsed < previous) {
            best = Some((operations, elapsed));
        }
    }

    let (operations, elapsed) = best.expect("at least one pass runs");
    Measurement {
        name,
        unit,
        operations,
        elapsed,
    }
}

/// Xorshift64*, so a run is reproducible and two runs compare.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A value in `0..bound`.
    pub fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
}
