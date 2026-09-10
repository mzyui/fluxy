use std::sync::Arc;
use std::time::Duration;

use crate::Protocol;

/// Default plain-HTTP online judges used when [`Config::http_judge_urls`] is untouched.
pub const DEFAULT_HTTP_JUDGE_URLS: &[&str] = &[
    "http://azenv.net/",
    "http://wfuchs.de/azenv.php",
    "http://proxyjudge.us/",
    "http://shinh.org/env.cgi",
];

/// Default TLS judges used for tunnel checks when [`Config::https_judge_urls`] is untouched.
pub const DEFAULT_HTTPS_JUDGE_URLS: &[&str] = &[
    "https://aranguren.org/azenv.php",
    "https://wfuchs.de/azenv.php",
];

/// Decides whether a requested protocol may still be probed.
///
/// CLI quota runs share one gate over the output enforcer: protocols whose
/// family quotas are all filled return false so workers skip those probes.
pub type ProbeGate = Arc<dyn Fn(Protocol) -> bool + Send + Sync>;

/// Validation inputs for [`ProxyValidator::validate`](crate::validator::ProxyValidator::validate).
///
/// At least one of `types` or `groups` must be non-empty; [`Config::default`]
/// fills public judge pools, a single attempt, and no failure reporting.
pub struct Config {
    /// Maximum concurrent validation probes (must be greater than zero).
    pub concurrency_limit: usize,
    /// Per-probe timeout in seconds (must be greater than zero).
    pub request_timeout: u64,
    /// Requested protocols; each matching advertised type spawns one probe job.
    pub types: Vec<Protocol>,
    /// AND-groups; a proxy passes only when every slot of a group passes.
    pub groups: Vec<Vec<Protocol>>,
    /// Probe attempts per candidate before giving up (must be greater than zero).
    pub max_attempts: usize,
    /// Plain-HTTP judge URLs used for `HTTP` probes.
    pub http_judge_urls: Vec<String>,
    /// Judge URLs used for tunnel probes (`HTTPS`, `SOCKS4`, `SOCKS5`, `CONNECT`).
    pub https_judge_urls: Vec<String>,
    /// Accept invalid TLS certificates on judge connections.
    pub insecure: bool,
    /// Probe requested types missing from advertised set.
    pub probe_missed_types: bool,
    /// Require judge to echo request cookie header.
    pub support_cookies: bool,
    /// Require judge to echo request referer header.
    pub support_referer: bool,
    /// Delay retries of the same proxy.
    /// Slept between attempts of one probe; zero disables the delay.
    pub retry_delay: Duration,
    /// Emit machine-readable report for every failed probe.
    /// Consume via `take_failures` before draining the stream.
    pub report_failures: bool,
    /// Skip probes for requested protocols the gate closes (`None` probes all).
    pub probe_gate: Option<ProbeGate>,
}

/// Default worker count used by [`Config::default`].
pub const DEFAULT_CONCURRENCY_LIMIT: usize = 500;

impl Default for Config {
    fn default() -> Self {
        Self {
            concurrency_limit: DEFAULT_CONCURRENCY_LIMIT,
            request_timeout: 3,
            types: Vec::new(),
            groups: Vec::new(),
            max_attempts: 1,
            http_judge_urls: DEFAULT_HTTP_JUDGE_URLS
                .iter()
                .map(|u| u.to_string())
                .collect(),
            https_judge_urls: DEFAULT_HTTPS_JUDGE_URLS
                .iter()
                .map(|u| u.to_string())
                .collect(),
            insecure: false,
            probe_missed_types: false,
            support_cookies: false,
            support_referer: false,
            retry_delay: Duration::ZERO,
            report_failures: false,
            probe_gate: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Config;
    use super::{DEFAULT_HTTPS_JUDGE_URLS, DEFAULT_HTTP_JUDGE_URLS};
    use std::time::Duration;

    #[test]
    fn defaults_use_public_judge_pools() {
        let config = Config::default();

        assert_eq!(config.http_judge_urls, DEFAULT_HTTP_JUDGE_URLS.to_vec());
        assert_eq!(config.https_judge_urls, DEFAULT_HTTPS_JUDGE_URLS.to_vec());
    }

    #[test]
    fn default_max_attempts_is_positive() {
        assert!(Config::default().max_attempts > 0);
    }

    #[test]
    fn default_groups_are_empty() {
        assert!(Config::default().groups.is_empty());
    }

    #[test]
    fn default_probe_missed_types_is_disabled() {
        assert!(!Config::default().probe_missed_types);
    }

    #[test]
    fn default_cookie_and_referer_support_are_disabled() {
        let config = Config::default();

        assert!(!config.support_cookies);
        assert!(!config.support_referer);
    }

    #[test]
    fn retry_delay_is_zero_by_default() {
        assert_eq!(Config::default().retry_delay, Duration::ZERO);
    }

    #[test]
    fn failure_reporting_is_disabled_by_default() {
        assert!(!Config::default().report_failures);
    }
}
