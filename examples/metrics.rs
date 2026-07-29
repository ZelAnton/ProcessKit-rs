//! Install a tiny in-process recorder, run two self-exec children, and print
//! the counters and histogram summaries emitted by processkit.
//!
//! Run with: `cargo run --example metrics --features metrics`

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use metrics::{
    Counter, CounterFn, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use processkit::Command;

#[derive(Debug, Default)]
struct Snapshot {
    counters: Mutex<BTreeMap<String, u64>>,
    histograms: Mutex<BTreeMap<String, (u64, f64)>>,
}

impl Snapshot {
    fn print(&self) {
        let counters = self.counters.lock().expect("counter snapshot poisoned");
        for (series, value) in counters.iter() {
            println!("counter   {series} = {value}");
        }
        drop(counters);

        let histograms = self.histograms.lock().expect("histogram snapshot poisoned");
        for (series, (count, sum)) in histograms.iter() {
            println!("histogram {series}: count={count}, sum={sum:.6}");
        }
    }
}

#[derive(Debug)]
struct CounterHandle {
    state: Arc<Snapshot>,
    series: String,
}

impl CounterFn for CounterHandle {
    fn increment(&self, value: u64) {
        *self
            .state
            .counters
            .lock()
            .expect("counter snapshot poisoned")
            .entry(self.series.clone())
            .or_default() += value;
    }

    fn absolute(&self, value: u64) {
        let mut counters = self
            .state
            .counters
            .lock()
            .expect("counter snapshot poisoned");
        let current = counters.entry(self.series.clone()).or_default();
        *current = (*current).max(value);
    }
}

#[derive(Debug)]
struct HistogramHandle {
    state: Arc<Snapshot>,
    series: String,
}

impl HistogramFn for HistogramHandle {
    fn record(&self, value: f64) {
        let mut histograms = self
            .state
            .histograms
            .lock()
            .expect("histogram snapshot poisoned");
        let (count, sum) = histograms.entry(self.series.clone()).or_default();
        *count += 1;
        *sum += value;
    }
}

#[derive(Debug)]
struct SnapshotRecorder {
    state: Arc<Snapshot>,
}

impl Recorder for SnapshotRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(CounterHandle {
            state: self.state.clone(),
            series: series(key),
        }))
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(HistogramHandle {
            state: self.state.clone(),
            series: series(key),
        }))
    }
}

fn series(key: &Key) -> String {
    let mut labels: Vec<_> = key
        .labels()
        .map(|label| format!("{}={}", label.key(), label.value()))
        .collect();
    labels.sort();
    if labels.is_empty() {
        key.name().to_owned()
    } else {
        format!("{}{{{}}}", key.name(), labels.join(","))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|arg| arg == "--child") {
        println!("child completed");
        return Ok(());
    }

    let snapshot = Arc::new(Snapshot::default());
    metrics::set_global_recorder(SnapshotRecorder {
        state: snapshot.clone(),
    })?;

    let executable = std::env::current_exe()?;
    for _ in 0..2 {
        let result = Command::new(&executable)
            .arg("--child")
            .output_string()
            .await?;
        assert!(result.is_success());
    }

    snapshot.print();
    Ok(())
}
