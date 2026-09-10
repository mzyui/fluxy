use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// Report judge preflight results at validator startup.
///
/// Returned by [`ProxyValidator::judge_health`](crate::validator::ProxyValidator::judge_health);
/// `candidates` counts unique URLs while `healthy` counts judges that passed preflight.
#[derive(Debug, Clone, Default)]
pub struct JudgeHealthReport {
    /// Number of unique judge URL candidates probed at startup.
    pub candidates: usize,
    /// Number of judges that passed preflight and joined the pool.
    pub healthy: usize,
    /// `(url, reason)` pairs for judges dropped during preflight.
    pub failed: Vec<(String, String)>,
}

impl JudgeHealthReport {
    pub(crate) fn merge(&mut self, other: &JudgeHealthReport) {
        self.candidates += other.candidates;
        self.healthy += other.healthy;
        self.failed.extend(other.failed.iter().cloned());
    }
}

/// Track validation progress counters.
///
/// Clone shares the underlying atomics, so a clone taken early still observes
/// live totals; group probes count once per group rather than per slot.
#[derive(Debug, Clone, Default)]
pub struct ValidationProgress {
    pub(crate) total: Arc<AtomicUsize>,
    pub(crate) done: Arc<AtomicUsize>,
    pub(crate) passed: Arc<AtomicUsize>,
}

impl ValidationProgress {
    /// Total probe jobs scheduled so far.
    pub fn total(&self) -> usize {
        self.total.load(Ordering::Relaxed)
    }

    /// Probe jobs finished so far, including failures and gate skips.
    pub fn done(&self) -> usize {
        self.done.load(Ordering::Relaxed)
    }

    /// Probe jobs (or groups) that passed so far.
    pub fn passed(&self) -> usize {
        self.passed.load(Ordering::Relaxed)
    }

    /// Scheduled jobs not yet finished; saturates at zero.
    pub fn remaining(&self) -> usize {
        self.total().saturating_sub(self.done())
    }

    /// Fraction of scheduled jobs finished; `0.0` when nothing is scheduled yet.
    pub fn fraction(&self) -> f64 {
        let total = self.total();
        if total == 0 {
            0.0
        } else {
            self.done() as f64 / total as f64
        }
    }
}
