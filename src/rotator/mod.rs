//! Serve validated proxies through a local rotating endpoint.

mod pool;
mod server;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::sync::mpsc;
use tokio::{net::TcpListener, time};

pub use pool::RotatorPool;

pub const DEFAULT_BIND: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
pub const DEFAULT_PORT: u16 = 8080;
/// Cap pooled proxies; feeder refills up to it.
pub const MAX_POOL_SIZE: usize = 25;
/// Bound queued serve events; excess events drop instead of blocking relays.
pub const EVENT_CHANNEL_CAPACITY: usize = 1024;
pub const DEFAULT_POOL_SIZE: usize = MAX_POOL_SIZE;
/// Gate serving until this many proxies are ready.
pub const DEFAULT_MIN_READY: usize = 1;
pub const DEFAULT_REFRESH_SECS: u64 = 300;
/// Bound each connection end-to-end without per-phase splits.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

const MAX_CONSECUTIVE_FAILURES: u32 = 3;
const COOLDOWN: Duration = Duration::from_secs(60);
const REQUEST_HEAD_TIMEOUT: Duration = Duration::from_secs(10);
const READY_WAIT_TIMEOUT: Duration = Duration::from_secs(120);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    RoundRobin,
    Random,
}

impl Strategy {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "round-robin" => Some(Self::RoundRobin),
            "random" => Some(Self::Random),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RoundRobin => "round-robin",
            Self::Random => "random",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServeOptions {
    pub bind: IpAddr,
    pub port: u16,
    pub strategy: Strategy,
    pub pool_size: usize,
    pub min_ready: usize,
    pub refresh_secs: u64,
    /// Require Basic proxy auth from clients.
    pub auth: Option<(String, String)>,
    pub request_timeout: Duration,
    /// Opt-in sink for per-connection events; `None` disables reporting.
    pub event_tx: Option<mpsc::Sender<ServeEvent>>,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            bind: DEFAULT_BIND,
            port: DEFAULT_PORT,
            strategy: Strategy::RoundRobin,
            pool_size: DEFAULT_POOL_SIZE,
            min_ready: DEFAULT_MIN_READY,
            refresh_secs: DEFAULT_REFRESH_SECS,
            auth: None,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            event_tx: None,
        }
    }
}

/// One client connection seen by the rotating endpoint.
///
/// Targets are `host:port` authorities only, never paths or queries.
#[derive(Debug, Clone)]
pub enum ServeEvent {
    /// A parseable request head arrived from a client.
    Incoming {
        client: Option<SocketAddr>,
        method: String,
        target: String,
    },
    /// The connection reached an upstream outcome.
    Completed {
        client: Option<SocketAddr>,
        method: String,
        target: String,
        upstream: Option<String>,
        ok: bool,
        reason: Option<String>,
        elapsed: Duration,
    },
}

impl std::fmt::Display for ServeEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incoming {
                client,
                method,
                target,
            } => {
                let client = client.map_or("-".to_owned(), |addr| addr.to_string());
                write!(f, "→ {method} {target} from {client}")
            }
            Self::Completed {
                client: _,
                method,
                target,
                upstream,
                ok,
                reason,
                elapsed,
            } => {
                let upstream = upstream.as_deref().unwrap_or("-");
                if *ok {
                    write!(f, "✓ {method} {target} via {upstream} {elapsed:?}")
                } else {
                    let reason = reason.as_deref().unwrap_or("failed");
                    write!(
                        f,
                        "✗ {method} {target} via {upstream} FAIL {reason} {elapsed:?}"
                    )
                }
            }
        }
    }
}

/// Fill pools and serve connections until shutdown.
///
/// Created from [`ServeOptions`] via [`Rotator::new`]; feed it with
/// [`Rotator::pool`] then drive with [`Rotator::run`].
pub struct Rotator {
    pool: Arc<RotatorPool>,
    options: Arc<ServeOptions>,
    ready_bypass: AtomicBool,
}

impl Rotator {
    /// Creates a rotator backed by a fresh pool.
    ///
    /// # Arguments
    ///
    /// * `options` - Bind address, strategy, pool size, and timeouts.
    pub fn new(options: ServeOptions) -> Self {
        let strategy = options.strategy;
        Self {
            pool: Arc::new(RotatorPool::new(strategy)),
            options: Arc::new(options),
            ready_bypass: AtomicBool::new(false),
        }
    }

    /// Shares the pool fed by the validation pipeline.
    pub fn pool(&self) -> Arc<RotatorPool> {
        Arc::clone(&self.pool)
    }

    /// Bypass the readiness gate for exhausted feeds.
    pub fn force_ready(&self) {
        self.ready_bypass.store(true, Ordering::Relaxed);
    }

    /// Binds, waits for `min_ready` proxies, then serves until cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error when the bind address cannot be claimed.
    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        let address = SocketAddr::new(self.options.bind, self.options.port);
        let listener = TcpListener::bind(address)
            .await
            .with_context(|| format!("failed to bind the rotating endpoint on {address}"))?;

        wait_for_ready(
            &self.pool,
            &self.ready_bypass,
            self.options.min_ready.min(MAX_POOL_SIZE),
        )
        .await;

        server::accept_loop(listener, Arc::clone(&self.pool), Arc::clone(&self.options)).await;
        Ok(())
    }
}

/// Poll pool readiness until bypass or timeout.
async fn wait_for_ready(pool: &RotatorPool, bypass: &AtomicBool, min_ready: usize) {
    let ready = async {
        while !bypass.load(Ordering::Relaxed) && pool.ready() < min_ready {
            time::sleep(READY_POLL_INTERVAL).await;
        }
    };
    tokio::select! {
        _ = ready => {}
        _ = time::sleep(READY_WAIT_TIMEOUT) => {}
    }
}

use anyhow::Context as _;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn single_ready_proxy_goes_live_immediately() {
        let pool = RotatorPool::new(Strategy::RoundRobin);
        assert!(pool.add(crate::Proxy::new(std::net::Ipv4Addr::LOCALHOST, 8081)));
        let bypass = AtomicBool::new(false);
        tokio::select! {
            _ = wait_for_ready(&pool, &bypass, DEFAULT_MIN_READY) => {}
            _ = time::sleep(Duration::from_secs(5)) => {
                panic!("gate did not open for one ready proxy");
            }
        }
    }

    #[tokio::test]
    async fn force_ready_opens_the_gate_with_an_empty_pool() {
        let pool = RotatorPool::new(Strategy::RoundRobin);
        let bypass = AtomicBool::new(true);
        tokio::select! {
            _ = wait_for_ready(&pool, &bypass, 10) => {}
            _ = time::sleep(Duration::from_secs(5)) => {
                panic!("bypass flag did not open the gate");
            }
        }
    }

    #[test]
    fn pool_is_capped_at_twenty_five_by_default() {
        assert_eq!(MAX_POOL_SIZE, 25);
        assert_eq!(DEFAULT_POOL_SIZE, MAX_POOL_SIZE);
        assert_eq!(
            ServeOptions::default().pool_size,
            DEFAULT_POOL_SIZE,
            "the serve facade must default to the capped pool"
        );
    }

    #[test]
    fn serve_events_render_authorities_without_paths() {
        let client: SocketAddr = "127.0.0.1:54321".parse().unwrap();
        let incoming = ServeEvent::Incoming {
            client: Some(client),
            method: "CONNECT".to_owned(),
            target: "example.com:443".to_owned(),
        };
        assert_eq!(
            incoming.to_string(),
            "→ CONNECT example.com:443 from 127.0.0.1:54321"
        );

        let completed = ServeEvent::Completed {
            client: Some(client),
            method: "CONNECT".to_owned(),
            target: "example.com:443".to_owned(),
            upstream: Some("192.0.2.1:8080".to_owned()),
            ok: true,
            reason: None,
            elapsed: Duration::from_millis(12),
        };
        let rendered = completed.to_string();
        assert!(
            rendered.starts_with("✓ CONNECT example.com:443 via 192.0.2.1:8080"),
            "{rendered}"
        );
        assert!(!rendered.contains('/'), "authorities must never leak paths");

        let failed = ServeEvent::Completed {
            client: None,
            method: "GET".to_owned(),
            target: "-".to_owned(),
            upstream: None,
            ok: false,
            reason: Some("bad request".to_owned()),
            elapsed: Duration::from_millis(3),
        };
        let rendered = failed.to_string();
        assert!(
            rendered.starts_with("✗ GET - via - FAIL bad request"),
            "{rendered}"
        );
    }
}
