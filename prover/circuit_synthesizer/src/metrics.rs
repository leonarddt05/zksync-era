use std::time::Duration;
use vise::{Histogram, LabeledFamily, Metrics};

#[derive(Debug, Metrics)]
#[metrics(prefix = "server_circuit_synthesizer")]
pub(crate) struct CircuitSynthesizerMetrics {
    #[metrics(buckets = Buckets::LATENCIES, labels = ["blob_size_in_gb"])]
    pub blob_sending_time: LabeledFamily<&'static str, Histogram<Duration>>,
    #[metrics(buckets = Buckets::LATENCIES, labels = ["circuit_type"])]
    pub synthesize: LabeledFamily<&'static str, Histogram<Duration>>,
}

#[vise::register]
pub(crate) static CIRCUIT_SYNTHESIZER_METRICS: vise::Global<CircuitSynthesizerMetrics> =
    vise::Global::new();
