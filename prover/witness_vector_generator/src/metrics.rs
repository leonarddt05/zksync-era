use std::time::Duration;
use vise::{Buckets, Histogram, LabeledFamily, Metrics};

#[derive(Debug, Metrics)]
#[metrics(prefix = "prover_fri_witness_vector_generator")]
pub(crate) struct WitnessVectorGeneratorMetrics {
    #[metrics(buckets = Buckets::LATENCIES, labels = ["circuit_type"])]
    pub gpu_witness_vector_generation_time: LabeledFamily<&'static str, Histogram<Duration>>,
    #[metrics(buckets = Buckets::LATENCIES, labels = ["circuit_type"])]
    pub blob_sending_time: LabeledFamily<&'static str, Histogram<Duration>>,
    #[metrics(buckets = Buckets::LATENCIES, labels = ["circuit_type"])]
    pub prover_waiting_time: LabeledFamily<&'static str, Histogram<Duration>>,
    #[metrics(buckets = Buckets::exponential(1.0..64.0, 2.0), labels = ["circuit_type"])]
    pub prover_attempts_count: LabeledFamily<&'static str, Histogram<usize>>,
}

#[vise::register]
pub(crate) static WITNESS_VECTOR_GENERATOR_METRICS: vise::Global<WitnessVectorGeneratorMetrics> =
    vise::Global::new();
