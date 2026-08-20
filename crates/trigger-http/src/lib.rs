//! Implementation for the Spin HTTP engine.

mod headers;
mod instrument;
mod middleware;
mod outbound_http;
mod server;
mod spin;
mod tls;
mod wagi;
mod wasi;
mod wasip3;

use std::{
    error::Error,
    fmt::Display,
    net::{Ipv4Addr, SocketAddr, ToSocketAddrs},
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, bail};
use clap::Args;
use rand::{
    distr::uniform::{SampleRange, SampleUniform},
    rand_core::Rng,
};
use serde::Deserialize;
use spin_app::App;
use spin_factor_outbound_http::intercept::OutboundHttpInterceptor;
use spin_factors::RuntimeFactors;
use spin_trigger::Trigger;
use wasmtime_wasi_http::p2::bindings::http::types::ErrorCode;

pub use server::HttpServer;

pub use tls::TlsConfig;

pub(crate) use wasmtime_wasi_http::p2::body::HyperIncomingBody as Body;

/// An opaque ID copied from an HTTP request to its store-completion observation.
///
/// Insert this value into [`http::Request::extensions_mut`] before calling
/// [`HttpServer::handle`]. WASIp2 requests have one store per request. WASIp3
/// reports the ID only for single-use stores because a reused store has no
/// exact request-level completion. A request that fails before creating a
/// store produces no store-completion observation.
#[derive(Clone, Debug)]
pub struct RequestCompletionId {
    id: u64,
    started: Arc<AtomicBool>,
}

impl RequestCompletionId {
    /// Creates a request-completion ID.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            started: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns the opaque ID value.
    pub fn get(&self) -> u64 {
        self.id
    }

    /// Returns whether this request reached a request-owned store.
    pub fn started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }

    pub(crate) fn mark_started(&self) {
        self.started.store(true, Ordering::Release);
    }
}

const DEFAULT_WASIP3_MAX_INSTANCE_REUSE_COUNT: usize = 128;
const DEFAULT_WASIP3_MAX_INSTANCE_CONCURRENT_REUSE_COUNT: usize = 16;
const DEFAULT_REQUEST_TIMEOUT: Option<Range<Duration>> = None;
const DEFAULT_IDLE_INSTANCE_TIMEOUT: Range<Duration> = Range::Value(Duration::from_secs(1));

/// The format in which to print startup route information.
#[derive(clap::ValueEnum, Clone, Copy, Debug, Default)]
pub enum OutputFormat {
    /// Human-readable plain text output (the default).
    #[default]
    Plain,
    /// Machine-readable JSON output.
    Json,
}

/// Controls validation performed when constructing an HTTP trigger.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HttpApplicationValidation {
    /// Preserve the validation behavior of existing constructors.
    #[default]
    Compatible,
    /// Reject applications that cannot be safely published.
    Strict,
}

/// A [`spin_trigger::TriggerApp`] for the HTTP trigger.
pub(crate) type TriggerApp<F> = spin_trigger::TriggerApp<HttpTrigger, F>;

/// A [`spin_trigger::TriggerInstanceBuilder`] for the HTTP trigger.
pub(crate) type TriggerInstanceBuilder<'a, F> =
    spin_trigger::TriggerInstanceBuilder<'a, HttpTrigger, F>;

#[derive(Args)]
pub struct CliArgs {
    /// IP address and port to listen on
    #[clap(long = "listen", env = "SPIN_HTTP_LISTEN_ADDR", default_value = "127.0.0.1:3000", value_parser = parse_listen_addr)]
    pub address: SocketAddr,

    /// The path to the certificate to use for https, if this is not set, normal http will be used. The cert should be in PEM format
    #[clap(long, env = "SPIN_TLS_CERT", requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,

    /// The path to the certificate key to use for https, if this is not set, normal http will be used. The key should be in PKCS#8 format
    #[clap(long, env = "SPIN_TLS_KEY", requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,

    /// Sets the maximum buffer size (in bytes) for the HTTP connection. The minimum value allowed is 8192.
    #[clap(long, env = "SPIN_HTTP1_MAX_BUF_SIZE")]
    pub http1_max_buf_size: Option<usize>,

    #[clap(long = "find-free-port")]
    pub find_free_port: bool,

    #[clap(value_enum, long = "format", default_value_t = OutputFormat::default())]
    pub format: OutputFormat,

    /// Maximum number of requests to send to a single component instance before
    /// dropping it.
    ///
    /// This defaults to 1 for WASIp2 components and 128 for WASIp3 components.
    /// As of this writing, setting it to more than 1 will have no effect for
    /// WASIp2 components, but that may change in the future.
    ///
    /// This may be specified either as an integer value or as a range,
    /// e.g. 1..8.  If it's a range, a number will be selected from that range
    /// at random for each new instance.
    #[clap(long, value_parser = parse_usize_range)]
    pub max_instance_reuse_count: Option<Range<usize>>,

    /// Maximum number of concurrent requests to send to a single component
    /// instance.
    ///
    /// This defaults to 1 for WASIp2 components and 16 for WASIp3 components.
    /// Note that setting it to more than 1 will have no effect for WASIp2
    /// components since they cannot be called concurrently.
    ///
    /// This may be specified either as an integer value or as a range,
    /// e.g. 1..8.  If it's a range, a number will be selected from that range
    /// at random for each new instance.
    #[clap(long, value_parser = parse_usize_range)]
    pub max_instance_concurrent_reuse_count: Option<Range<usize>>,

    /// Request timeout to enforce.
    ///
    /// As of this writing, this only affects WASIp3 components.
    ///
    /// A number with no suffix or with an `s` suffix is interpreted as seconds;
    /// other accepted suffixes include `ms` (milliseconds), `us` or `μs`
    /// (microseconds), and `ns` (nanoseconds).
    ///
    /// This may be specified either as a single time value or as a range,
    /// e.g. 1..8s.  If it's a range, a value will be selected from that range
    /// at random for each new instance.
    #[clap(long, value_parser = parse_duration_range)]
    pub request_timeout: Option<Range<Duration>>,

    /// Time to hold an idle component instance for possible reuse before
    /// dropping it.
    ///
    /// A number with no suffix or with an `s` suffix is interpreted as seconds;
    /// other accepted suffixes include `ms` (milliseconds), `us` or `μs`
    /// (microseconds), and `ns` (nanoseconds).
    ///
    /// This may be specified either as a single time value or as a range,
    /// e.g. 1..8s.  If it's a range, a value will be selected from that range
    /// at random for each new instance.
    #[clap(long, default_value = "1s", value_parser = parse_duration_range)]
    pub idle_instance_timeout: Range<Duration>,
}

impl CliArgs {
    fn into_tls_config(self) -> Option<TlsConfig> {
        match (self.tls_cert, self.tls_key) {
            (Some(cert_path), Some(key_path)) => Some(TlsConfig {
                cert_path,
                key_path,
            }),
            (None, None) => None,
            _ => unreachable!(),
        }
    }
}

#[derive(Copy, Clone)]
pub enum Range<T> {
    Value(T),
    Bounds(T, T),
}

impl<T> Range<T> {
    fn map<V>(self, fun: impl Fn(T) -> V) -> Range<V> {
        match self {
            Self::Value(v) => Range::Value(fun(v)),
            Self::Bounds(a, b) => Range::Bounds(fun(a), fun(b)),
        }
    }
}

impl<T: SampleUniform + PartialOrd> SampleRange<T> for Range<T> {
    fn sample_single<R: Rng + ?Sized>(self, rng: &mut R) -> Result<T, rand::distr::uniform::Error> {
        match self {
            Self::Value(v) => Ok(v),
            Self::Bounds(a, b) => (a..b).sample_single(rng),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Value(_) => false,
            Self::Bounds(a, b) => (a..b).is_empty(),
        }
    }
}

fn parse_range<T: FromStr>(s: &str) -> Result<Range<T>, String>
where
    T::Err: Display,
{
    let error = |e| format!("expected integer or range; got {s:?}; {e}");
    if let Some((start, end)) = s.split_once("..") {
        Ok(Range::Bounds(
            start.parse().map_err(error)?,
            end.parse().map_err(error)?,
        ))
    } else {
        Ok(Range::Value(s.parse().map_err(error)?))
    }
}

fn parse_usize_range(s: &str) -> Result<Range<usize>, String> {
    parse_range(s)
}

struct ParsedDuration(Duration);

impl FromStr for ParsedDuration {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let error = |e| {
            format!("expected integer suffixed by `s`, `ms`, `us`, `μs`, or `ns`; got {s:?}; {e}")
        };
        Ok(Self(match s.parse() {
            Ok(val) => Duration::from_secs(val),
            Err(err) => {
                if let Some(num) = s.strip_suffix("ms") {
                    Duration::from_millis(num.parse().map_err(error)?)
                } else if let Some(num) = s.strip_suffix("us").or(s.strip_suffix("μs")) {
                    Duration::from_micros(num.parse().map_err(error)?)
                } else if let Some(num) = s.strip_suffix("ns") {
                    Duration::from_nanos(num.parse().map_err(error)?)
                } else if let Some(num) = s.strip_suffix("s") {
                    Duration::from_secs(num.parse().map_err(error)?)
                } else {
                    return Err(error(err));
                }
            }
        }))
    }
}

fn parse_duration_range(s: &str) -> Result<Range<Duration>, String> {
    parse_range::<ParsedDuration>(s).map(|v| v.map(|v| v.0))
}

#[derive(Clone, Copy)]
pub struct InstanceReuseConfig {
    max_instance_reuse_count: Range<usize>,
    max_instance_concurrent_reuse_count: Range<usize>,
    request_timeout: Option<Range<Duration>>,
    request_deadline: Option<Duration>,
    idle_instance_timeout: Range<Duration>,
}

impl Default for InstanceReuseConfig {
    fn default() -> Self {
        Self {
            max_instance_reuse_count: Range::Value(DEFAULT_WASIP3_MAX_INSTANCE_REUSE_COUNT),
            max_instance_concurrent_reuse_count: Range::Value(
                DEFAULT_WASIP3_MAX_INSTANCE_CONCURRENT_REUSE_COUNT,
            ),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            request_deadline: None,
            idle_instance_timeout: DEFAULT_IDLE_INSTANCE_TIMEOUT,
        }
    }
}

impl InstanceReuseConfig {
    /// Creates a single-use instance reuse configuration with a Wasmtime request deadline.
    ///
    /// The deadline is enforced by the Wasmtime epoch interruption mechanism in the
    /// underlying Spin store. It is a rough deadline: the guest may run somewhat longer
    /// depending on the engine epoch tick interval, host thread scheduling, and how often
    /// the compiled guest code checks the epoch. Instance reuse is disabled so every request
    /// receives a fresh store with a request-specific deadline.
    pub fn single_use_with_request_deadline(timeout: Duration) -> Self {
        Self {
            max_instance_reuse_count: Range::Value(1),
            max_instance_concurrent_reuse_count: Range::Value(1),
            request_timeout: Some(Range::Value(timeout)),
            request_deadline: Some(timeout),
            idle_instance_timeout: DEFAULT_IDLE_INSTANCE_TIMEOUT,
        }
    }
}

/// The Spin HTTP trigger.
pub struct HttpTrigger {
    /// The address the server should listen on.
    ///
    /// Note that this might not be the actual socket address that ends up being bound to.
    /// If the port is set to 0, the actual address will be determined by the OS.
    listen_addr: SocketAddr,
    tls_config: Option<TlsConfig>,
    find_free_port: bool,
    http1_max_buf_size: Option<usize>,
    reuse_config: InstanceReuseConfig,
    output_format: OutputFormat,
    embedder_outbound_http_interceptor: Option<Arc<dyn OutboundHttpInterceptor>>,
}

impl<F: RuntimeFactors> Trigger<F> for HttpTrigger {
    const TYPE: &'static str = "http";

    type CliArgs = CliArgs;
    type InstanceState = ();

    fn new(cli_args: Self::CliArgs, app: &spin_app::App) -> anyhow::Result<Self> {
        let find_free_port = cli_args.find_free_port;
        let http1_max_buf_size = cli_args.http1_max_buf_size;
        let output_format = cli_args.format;
        let reuse_config = InstanceReuseConfig {
            max_instance_reuse_count: cli_args
                .max_instance_reuse_count
                .unwrap_or(Range::Value(DEFAULT_WASIP3_MAX_INSTANCE_REUSE_COUNT)),
            max_instance_concurrent_reuse_count: cli_args
                .max_instance_concurrent_reuse_count
                .unwrap_or(Range::Value(
                    DEFAULT_WASIP3_MAX_INSTANCE_CONCURRENT_REUSE_COUNT,
                )),
            request_timeout: cli_args.request_timeout,
            request_deadline: None,
            idle_instance_timeout: cli_args.idle_instance_timeout,
        };

        Self::new(
            app,
            cli_args.address,
            cli_args.into_tls_config(),
            find_free_port,
            http1_max_buf_size,
            reuse_config,
            output_format,
        )
    }

    async fn run(self, trigger_app: TriggerApp<F>) -> anyhow::Result<()> {
        let server = self.into_server(trigger_app)?;

        server.serve().await?;

        Ok(())
    }

    fn trigger_dependencies_composer() -> impl spin_factors_executor::TriggerDependenciesComposer {
        middleware::HttpMiddlewareComposer
    }

    fn supported_host_requirements() -> Vec<&'static str> {
        vec![
            spin_app::locked::SERVICE_CHAINING_KEY,
            spin_app::locked::MIDDLEWARE_KEY,
        ]
    }

    fn display_name() -> String {
        "HTTP".to_string()
    }
}

impl HttpTrigger {
    /// Create a new `HttpTrigger`.
    pub fn new(
        app: &spin_app::App,
        listen_addr: SocketAddr,
        tls_config: Option<TlsConfig>,
        find_free_port: bool,
        http1_max_buf_size: Option<usize>,
        reuse_config: InstanceReuseConfig,
        output_format: OutputFormat,
    ) -> anyhow::Result<Self> {
        Self::new_with_application_validation(
            app,
            listen_addr,
            tls_config,
            find_free_port,
            http1_max_buf_size,
            reuse_config,
            output_format,
            HttpApplicationValidation::Compatible,
        )
    }

    /// Create a new `HttpTrigger` with the requested application validation.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_application_validation(
        app: &spin_app::App,
        listen_addr: SocketAddr,
        tls_config: Option<TlsConfig>,
        find_free_port: bool,
        http1_max_buf_size: Option<usize>,
        reuse_config: InstanceReuseConfig,
        output_format: OutputFormat,
        validation: HttpApplicationValidation,
    ) -> anyhow::Result<Self> {
        Self::validate_app(app, validation)?;

        Ok(Self {
            listen_addr,
            tls_config,
            find_free_port,
            http1_max_buf_size,
            reuse_config,
            output_format,
            embedder_outbound_http_interceptor: None,
        })
    }

    /// Installs an embedder-provided outbound HTTP interceptor.
    ///
    /// Spin service chaining runs before this interceptor, and ordinary network
    /// handling runs after it returns `Continue`.
    pub fn with_embedder_outbound_http_interceptor(
        mut self,
        interceptor: Arc<dyn OutboundHttpInterceptor>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            self.embedder_outbound_http_interceptor.is_none(),
            "embedder outbound HTTP interceptor is already set"
        );
        self.embedder_outbound_http_interceptor = Some(interceptor);
        Ok(self)
    }

    /// Turn this [`HttpTrigger`] into an [`HttpServer`].
    pub fn into_server<F: RuntimeFactors>(
        self,
        trigger_app: TriggerApp<F>,
    ) -> anyhow::Result<Arc<HttpServer<F>>> {
        let Self {
            listen_addr,
            tls_config,
            find_free_port,
            http1_max_buf_size,
            reuse_config,
            output_format,
            embedder_outbound_http_interceptor,
        } = self;
        let server = Arc::new(
            HttpServer::new(
                listen_addr,
                tls_config,
                find_free_port,
                trigger_app,
                http1_max_buf_size,
                reuse_config,
                output_format,
            )?
            .with_embedder_outbound_http_interceptor(embedder_outbound_http_interceptor),
        );
        Ok(server)
    }

    fn validate_app(app: &App, validation: HttpApplicationValidation) -> anyhow::Result<()> {
        use spin_http::{
            config::{HttpExecutorType, HttpTriggerConfig},
            routes::{HttpTriggerRouteConfig, Router},
        };

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct TriggerMetadata {
            base: Option<String>,
        }
        if let Some(TriggerMetadata { base: Some(base) }) = app.get_trigger_metadata("http")? {
            if base == "/" {
                tracing::warn!(
                    "This application has the deprecated trigger 'base' set to the default value '/'. This may be an error in the future!"
                );
            } else {
                bail!(
                    "This application is using the deprecated trigger 'base' field. The base must be prepended to each [[trigger.http]]'s 'route'."
                )
            }
        }

        let mut explain_wagi_deprecation = false;
        for trigger in app.triggers_with_type("http") {
            if let Ok(config) = trigger.typed_config::<HttpTriggerConfig>()
                && let Some(executor) = config.executor
                && let HttpExecutorType::Wagi(_) = executor
            {
                let description = match config.route {
                    HttpTriggerRouteConfig::Route(r) => format!("route {r}"),
                    HttpTriggerRouteConfig::Private(_) => format!(
                        "private endpoint for {}",
                        config.component.unwrap_or_else(|| "<unknown>".into())
                    ),
                };
                terminal::warn!("HTTP {description} uses the WAGI executor.");
                explain_wagi_deprecation = true;
            }
        }
        if explain_wagi_deprecation {
            terminal::warn!("WAGI will be deprecated in a future version of Spin.");
            eprintln!(
                "To provide feedback, please visit https://github.com/spinframework/spin/issues/3520.\n"
            );
        }

        if validation == HttpApplicationValidation::Strict {
            let trigger_configs = app
                .trigger_configs::<HttpTriggerConfig>("http")?
                .into_iter()
                .map(|(trigger_id, config)| {
                    if let Some(static_response) = &config.static_response {
                        static_response.validate()?;
                    }
                    Ok((config.lookup_key(trigger_id)?, config))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let routes = trigger_configs
                .iter()
                .map(|(key, config)| (key, &config.route));
            let mut duplicate_routes = Vec::new();
            let router = Router::build("/", routes, Some(&mut duplicate_routes))?;

            anyhow::ensure!(
                router.routes().next().is_some(),
                "HTTP application must define at least one public route"
            );
            anyhow::ensure!(
                duplicate_routes.is_empty(),
                "HTTP application contains duplicate routes: {}",
                duplicate_routes
                    .iter()
                    .map(|route| route.route())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            anyhow::ensure!(
                !router.contains_reserved_route(),
                "HTTP application routes must not use the reserved {} prefix",
                spin_http::WELL_KNOWN_PREFIX
            );
        }

        Ok(())
    }
}

fn parse_listen_addr(addr: &str) -> anyhow::Result<SocketAddr> {
    let addrs: Vec<SocketAddr> = addr.to_socket_addrs()?.collect();
    // Prefer 127.0.0.1 over e.g. [::1] because CHANGE IS HARD
    if let Some(addr) = addrs
        .iter()
        .find(|addr| addr.is_ipv4() && addr.ip() == Ipv4Addr::LOCALHOST)
    {
        return Ok(*addr);
    }
    // Otherwise, take the first addr (OS preference)
    addrs.into_iter().next().context("couldn't resolve address")
}

#[derive(Debug, PartialEq)]
enum NotFoundRouteKind {
    Normal(String),
    WellKnown,
}

/// Translate a [`hyper::Error`] to a wasi-http `ErrorCode` in the context of a request.
pub fn hyper_request_error(err: hyper::Error) -> ErrorCode {
    // If there's a source, we might be able to extract a wasi-http error from it.
    if let Some(cause) = err.source()
        && let Some(err) = cause.downcast_ref::<ErrorCode>()
    {
        return err.clone();
    }

    tracing::warn!("hyper request error: {err:?}");

    ErrorCode::HttpProtocolError
}

pub fn dns_error(rcode: String, info_code: u16) -> ErrorCode {
    ErrorCode::DnsError(
        wasmtime_wasi_http::p2::bindings::http::types::DnsErrorPayload {
            rcode: Some(rcode),
            info_code: Some(info_code),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use spin_app::locked::{LockedApp, LockedTrigger};

    fn app_with_http_triggers(configs: impl IntoIterator<Item = serde_json::Value>) -> App {
        let triggers = configs
            .into_iter()
            .enumerate()
            .map(|(index, trigger_config)| LockedTrigger {
                id: format!("trigger-{index}"),
                trigger_type: "http".into(),
                trigger_config,
                trigger_dependencies: Default::default(),
            })
            .collect();
        App::new(
            "test-app",
            LockedApp {
                spin_lock_version: Default::default(),
                must_understand: Default::default(),
                metadata: spin_app::values::ValuesMapBuilder::try_from(json!({
                    "triggers": { "http": {} }
                }))
                .unwrap()
                .build(),
                host_requirements: Default::default(),
                variables: Default::default(),
                triggers,
                components: Default::default(),
            },
        )
    }

    fn validate_http_app(app: &App, validation: HttpApplicationValidation) -> anyhow::Result<()> {
        HttpTrigger::new_with_application_validation(
            app,
            "127.0.0.1:0".parse().unwrap(),
            None,
            false,
            None,
            InstanceReuseConfig::default(),
            OutputFormat::default(),
            validation,
        )
        .map(|_| ())
    }

    #[test]
    fn parse_listen_addr_prefers_ipv4() {
        let addr = parse_listen_addr("localhost:12345").unwrap();
        assert_eq!(addr.ip(), Ipv4Addr::LOCALHOST);
        assert_eq!(addr.port(), 12345);
    }

    #[test]
    fn request_deadline_config_is_single_use() {
        let timeout = Duration::from_millis(500);
        let config = InstanceReuseConfig::single_use_with_request_deadline(timeout);

        assert!(matches!(config.max_instance_reuse_count, Range::Value(1)));
        assert!(matches!(
            config.max_instance_concurrent_reuse_count,
            Range::Value(1)
        ));
        assert!(matches!(config.request_timeout, Some(Range::Value(value)) if value == timeout));
        assert_eq!(config.request_deadline, Some(timeout));
    }

    #[test]
    fn strict_validation_requires_a_public_route() {
        let no_routes = app_with_http_triggers([]);
        validate_http_app(&no_routes, HttpApplicationValidation::Compatible).unwrap();
        assert!(validate_http_app(&no_routes, HttpApplicationValidation::Strict).is_err());

        let private_only = app_with_http_triggers([json!({
            "component": "private-component",
            "route": { "private": true }
        })]);
        assert!(validate_http_app(&private_only, HttpApplicationValidation::Strict).is_err());
    }

    #[test]
    fn strict_validation_rejects_duplicate_and_reserved_routes() {
        let duplicate = app_with_http_triggers([
            json!({ "route": "/same", "static_response": {} }),
            json!({ "route": "/same", "static_response": {} }),
        ]);
        assert!(validate_http_app(&duplicate, HttpApplicationValidation::Strict).is_err());

        let reserved = app_with_http_triggers([json!({
            "route": "/.well-known/spin/status",
            "static_response": {}
        })]);
        assert!(validate_http_app(&reserved, HttpApplicationValidation::Strict).is_err());
    }

    #[test]
    fn strict_validation_rejects_malformed_static_responses() {
        for static_response in [
            json!({ "status_code": 99 }),
            json!({ "headers": { "bad header": "value" } }),
            json!({ "headers": { "valid": "bad\nvalue" } }),
        ] {
            let app = app_with_http_triggers([json!({
                "route": "/",
                "static_response": static_response
            })]);
            assert!(validate_http_app(&app, HttpApplicationValidation::Strict).is_err());
        }
    }

    #[test]
    fn strict_validation_accepts_a_publishable_application() {
        let app = app_with_http_triggers([json!({
            "route": "/",
            "static_response": {
                "status_code": 204,
                "headers": { "x-valid": "yes" }
            }
        })]);
        validate_http_app(&app, HttpApplicationValidation::Strict).unwrap();
    }
}
