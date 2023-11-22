use vise::{Buckets, Histogram, Metrics};

#[derive(Debug, Metrics)]
#[metrics(prefix = "call_tracer")]
pub struct CallMetrics {
    /// Maximum call stack depth during the execution of the transaction.
    #[metrics(buckets = Buckets::linear(1.0..=64.0, 1.0))]
    pub call_stack_depth: Histogram<usize>,
    /// Maximum number of near calls during the execution of the transaction.
    #[metrics(buckets = Buckets::linear(1.0..=64.0, 1.0))]
    pub max_near_calls: Histogram<usize>,
}

#[vise::register]
pub static CALL_METRICS: vise::Global<CallMetrics> = vise::Global::new();
