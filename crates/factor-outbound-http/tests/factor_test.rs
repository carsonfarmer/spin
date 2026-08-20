use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, bail};
use bytes::Bytes;
use http::{Request, Uri};
use http_body_util::{BodyExt, Empty, combinators::UnsyncBoxBody};
use spin_common::{assert_matches, assert_not_matches};
use spin_factor_outbound_http::{
    ErrorCode, HostFutureIncomingResponse, OutboundHttpFactor, SelfRequestOrigin,
    intercept::{InterceptOutcome, InterceptRequest, OutboundHttpInterceptor},
};
use spin_factor_outbound_networking::OutboundNetworkingFactor;
use spin_factor_variables::VariablesFactor;
use spin_factors::{App, RuntimeFactors, anyhow};
use spin_factors_test::{TestEnvironment, toml};
use spin_world::{
    async_trait,
    v1::{
        http as legacy_http,
        http_types::{HttpError, Method, Request as LegacyRequest},
    },
};
use tracing::{
    Subscriber,
    field::{Field, Visit},
    span::{self, Record},
};
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
};
use wasmtime_wasi::p2::Pollable;
use wasmtime_wasi_http::p2::types::OutgoingRequestConfig;
use wasmtime_wasi_http::p3::{RequestOptions, bindings::http::types as p3_types};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

#[derive(RuntimeFactors)]
struct TestFactors {
    variables: VariablesFactor,
    networking: OutboundNetworkingFactor,
    http: OutboundHttpFactor,
}

#[tokio::test(flavor = "multi_thread")]
async fn allowed_host_is_allowed() -> anyhow::Result<()> {
    let mut state = test_instance_state("https://*", true).await?;
    let wasi_http = OutboundHttpFactor::get_wasi_http_impl(&mut state).unwrap();

    // [100::] is the IPv6 "Discard Prefix", which should always fail
    let req = Request::get("https://[100::1]:443").body(Default::default())?;
    let mut future_resp = wasi_http.hooks.send_request(req, test_request_config())?;
    future_resp.ready().await;

    assert_discard_prefix_error(future_resp);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn self_request_smoke_test() -> anyhow::Result<()> {
    let mut state = test_instance_state("http://self", true).await?;
    // [100::] is the IPv6 "Discard Prefix", which should always fail
    let origin = SelfRequestOrigin::from_uri(&Uri::from_static("http://[100::1]"))?;
    state.http.set_self_request_origin(origin);

    let wasi_http = OutboundHttpFactor::get_wasi_http_impl(&mut state).unwrap();
    let req = Request::get("/self-request").body(Default::default())?;
    let mut future_resp = wasi_http.hooks.send_request(req, test_request_config())?;
    future_resp.ready().await;

    assert_discard_prefix_error(future_resp);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn disallowed_host_fails() -> anyhow::Result<()> {
    let mut state = test_instance_state("https://allowed.test", true).await?;
    let wasi_http = OutboundHttpFactor::get_wasi_http_impl(&mut state).unwrap();

    let req = Request::get("https://denied.test").body(Default::default())?;
    let mut future_resp = wasi_http.hooks.send_request(req, test_request_config())?;
    future_resp.ready().await;
    assert_matches!(
        future_resp.unwrap_ready().unwrap(),
        Err(ErrorCode::HttpRequestDenied),
    );
    Ok(())
}

#[ignore = "flaky"]
#[tokio::test(flavor = "multi_thread")]
async fn disallowed_private_ips_fails() -> anyhow::Result<()> {
    async fn run_test(allow_private_ips: bool) -> anyhow::Result<()> {
        let mut state = test_instance_state("http://*", allow_private_ips).await?;
        let wasi_http = OutboundHttpFactor::get_wasi_http_impl(&mut state).unwrap();
        let req = Request::get("http://localhost").body(Default::default())?;
        let mut future_resp = wasi_http.hooks.send_request(req, test_request_config())?;
        future_resp.ready().await;
        match future_resp.unwrap_ready().unwrap() {
            // If we don't allow private IPs, we should not get a response
            Ok(_) if !allow_private_ips => bail!("expected Err, got Ok"),
            // Otherwise, it's fine if the request happens to succeed
            Ok(_) => {}
            // If private IPs are disallowed, we should get an error saying the destination is prohibited
            Err(err) if !allow_private_ips => {
                assert_matches!(err, ErrorCode::DestinationIpProhibited);
            }
            // Otherwise, we should get some non-DestinationIpProhibited error
            Err(err) => {
                assert_not_matches!(err, ErrorCode::DestinationIpProhibited);
            }
        };
        Ok(())
    }

    // Test with private IPs allowed
    run_test(true).await?;
    // Test with private IPs disallowed
    run_test(false).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn override_connect_addr_disallowed_private_ip_fails() -> anyhow::Result<()> {
    let mut state = test_instance_state("http://*", false).await?;
    state.http.set_request_interceptor({
        struct Interceptor;
        #[async_trait]
        impl OutboundHttpInterceptor for Interceptor {
            async fn intercept(
                &self,
                mut request: InterceptRequest,
            ) -> wasmtime_wasi_http::p2::HttpResult<InterceptOutcome> {
                request.override_connect_addr("[::1]:80".parse().unwrap());
                Ok(InterceptOutcome::Continue(request))
            }
        }
        Interceptor
    })?;
    let wasi_http = OutboundHttpFactor::get_wasi_http_impl(&mut state).unwrap();
    let req = Request::get("http://1.1.1.1").body(Default::default())?;
    let mut future_resp = wasi_http.hooks.send_request(req, test_request_config())?;
    future_resp.ready().await;
    assert_matches!(
        future_resp.unwrap_ready().unwrap(),
        Err(ErrorCode::DestinationIpProhibited),
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_http_permit_is_held_while_response_body_is_buffered() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (headers_sent, headers_received) = tokio::sync::oneshot::channel();
    let (release_body, body_released) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await?;
        let _first_response = tokio::spawn(async move {
            let mut request = [0; 1024];
            assert_ne!(first.read(&mut request).await?, 0);
            first
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n")
                .await?;
            let _ = headers_sent.send(());
            let _ = body_released.await;
            first.write_all(b"body").await?;
            anyhow::Ok(())
        });

        let (mut second, _) = listener.accept().await?;
        let mut request = [0; 1024];
        assert_ne!(second.read(&mut request).await?, 0);
        second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await?;
        anyhow::Ok(())
    });

    let (mut first_state, mut second_state) = legacy_http_states(address).await?;

    let uri = format!("http://{address}/slow");
    let first_request = legacy_request(uri.clone());
    let first_response = tokio::spawn(async move {
        legacy_http::Host::send_request(&mut first_state.http, first_request).await
    });
    headers_received.await?;

    let second_response =
        legacy_http::Host::send_request(&mut second_state.http, legacy_request(uri)).await;
    assert!(matches!(second_response, Err(HttpError::TooManyRequests)));

    let _ = release_body.send(());
    let first_response = first_response.await??;
    assert_eq!(first_response.body.as_deref(), Some(b"body".as_slice()));
    server.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_http_permit_is_released_after_body_error() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (partial_sent, partial_received) = tokio::sync::oneshot::channel();
    let (close_first, close_received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await?;
        let first_response = tokio::spawn(async move {
            let mut request = [0; 1024];
            assert_ne!(first.read(&mut request).await?, 0);
            first
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nx")
                .await?;
            let _ = partial_sent.send(());
            let _ = close_received.await;
            anyhow::Ok(())
        });

        let (mut second, _) = listener.accept().await?;
        let mut request = [0; 1024];
        assert_ne!(second.read(&mut request).await?, 0);
        second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await?;
        first_response.await??;
        anyhow::Ok(())
    });
    let (mut first_state, mut second_state) = legacy_http_states(address).await?;
    let uri = format!("http://{address}/body-error");
    let first_uri = uri.clone();
    let first_response = tokio::spawn(async move {
        legacy_http::Host::send_request(&mut first_state.http, legacy_request(first_uri)).await
    });
    partial_received.await?;

    assert!(matches!(
        legacy_http::Host::send_request(&mut second_state.http, legacy_request(uri.clone())).await,
        Err(HttpError::TooManyRequests)
    ));
    let _ = close_first.send(());
    assert!(matches!(
        first_response.await?,
        Err(HttpError::RuntimeError)
    ));
    let response = legacy_http::Host::send_request(
        &mut second_state.http,
        legacy_request(format!("http://{address}/after-error")),
    )
    .await?;
    assert_eq!(response.body.as_deref(), Some(b"ok".as_slice()));
    server.await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_http_permit_is_released_after_body_cancellation() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (headers_sent, headers_received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await?;
        let first_response = tokio::spawn(async move {
            let mut request = [0; 1024];
            assert_ne!(first.read(&mut request).await?, 0);
            first
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n")
                .await?;
            let _ = headers_sent.send(());
            let _ = first.read(&mut request).await?;
            anyhow::Ok(())
        });

        let (mut second, _) = listener.accept().await?;
        let mut request = [0; 1024];
        assert_ne!(second.read(&mut request).await?, 0);
        second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await?;
        first_response.await??;
        anyhow::Ok(())
    });
    let (mut first_state, mut second_state) = legacy_http_states(address).await?;
    let uri = format!("http://{address}/cancel");
    let first_uri = uri.clone();
    let first_response = tokio::spawn(async move {
        legacy_http::Host::send_request(&mut first_state.http, legacy_request(first_uri)).await
    });
    headers_received.await?;

    assert!(matches!(
        legacy_http::Host::send_request(&mut second_state.http, legacy_request(uri)).await,
        Err(HttpError::TooManyRequests)
    ));
    first_response.abort();
    assert!(first_response.await.unwrap_err().is_cancelled());
    let response = legacy_http::Host::send_request(
        &mut second_state.http,
        legacy_request(format!("http://{address}/after-cancel")),
    )
    .await?;
    assert_eq!(response.body.as_deref(), Some(b"ok".as_slice()));
    server.await??;
    Ok(())
}

async fn legacy_http_states(
    address: std::net::SocketAddr,
) -> anyhow::Result<(TestFactorsInstanceState, TestFactorsInstanceState)> {
    let factors = TestFactors {
        variables: VariablesFactor::default(),
        networking: OutboundNetworkingFactor::new(),
        http: OutboundHttpFactor::default(),
    };
    let allowed_host = format!("http://{address}");
    let env = TestEnvironment::new(factors)
        .extend_manifest(toml! {
            [component.test-component]
            source = "does-not-exist.wasm"
            allowed_outbound_hosts = [allowed_host]
        })
        .runtime_config(TestFactorsRuntimeConfig {
            http: Some(spin_factor_outbound_http::runtime_config::RuntimeConfig {
                max_concurrent_connections: Some(1),
                wait_timeout: Some(Duration::from_millis(100)),
                ..Default::default()
            }),
            ..Default::default()
        })?;
    let locked_app = env.build_locked_app().await?;
    let TestEnvironment {
        factors,
        runtime_config,
        ..
    } = env;
    let configured_app = factors.configure_app(App::new("test-app", locked_app), runtime_config)?;
    let component_id = configured_app
        .app()
        .components()
        .last()
        .context("no components")?
        .id()
        .to_owned();
    let builders = factors.prepare(&configured_app, &component_id)?;
    let first_state = factors.build_instance_state(builders)?;
    let builders = factors.prepare(&configured_app, &component_id)?;
    Ok((first_state, factors.build_instance_state(builders)?))
}

fn legacy_request(uri: String) -> LegacyRequest {
    LegacyRequest {
        method: Method::Get,
        uri,
        headers: Vec::new(),
        params: Vec::new(),
        body: None,
    }
}

async fn test_instance_state(
    allowed_outbound_hosts: &str,
    allow_private_ips: bool,
) -> anyhow::Result<TestFactorsInstanceState> {
    let factors = TestFactors {
        variables: VariablesFactor::default(),
        networking: OutboundNetworkingFactor::new(),
        http: OutboundHttpFactor::default(),
    };
    let env = TestEnvironment::new(factors)
        .extend_manifest(toml! {
            [component.test-component]
            source = "does-not-exist.wasm"
            allowed_outbound_hosts = [allowed_outbound_hosts]
        })
        .runtime_config(TestFactorsRuntimeConfig {
            networking: Some(
                spin_factor_outbound_networking::runtime_config::RuntimeConfig {
                    block_private_networks: !allow_private_ips,
                    ..Default::default()
                },
            ),
            ..Default::default()
        })?;
    env.build_instance_state().await
}

fn test_request_config() -> OutgoingRequestConfig {
    OutgoingRequestConfig {
        use_tls: false,
        connect_timeout: Duration::from_millis(10),
        first_byte_timeout: Duration::from_millis(0),
        between_bytes_timeout: Duration::from_millis(0),
    }
}

fn assert_discard_prefix_error(future_resp: HostFutureIncomingResponse) {
    // Different systems handle the discard prefix differently; some will
    // immediately reject it while others will silently let it time out
    assert_matches!(
        future_resp.unwrap_ready().unwrap(),
        Err(ErrorCode::ConnectionRefused
            | ErrorCode::ConnectionTimeout
            | ErrorCode::ConnectionReadTimeout
            | ErrorCode::DnsError(_)),
    );
}

// Regression: deferred `Span::record(...)` calls (e.g. `url.full`) must
// land on the `spin_outbound_http.send_request` span created by
// `#[instrument]`. Uses the current_thread runtime so the thread-local
// subscriber covers all async work.
#[tokio::test(flavor = "current_thread")]
async fn p3_send_request_propagates_span_to_async_work() -> anyhow::Result<()> {
    let layer = CaptureLayer::default();
    let records = Arc::clone(&layer.records);
    let subscriber = tracing_subscriber::registry().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let mut state = test_instance_state("https://*", true).await?;
    let p3_view = OutboundHttpFactor::get_wasi_p3_http_impl(&mut state).unwrap();
    // [100::1] is the IPv6 discard prefix — connection fails fast.
    let req = Request::get("https://[100::1]:443").body(empty_p3_body())?;
    let result_fut = p3_view
        .hooks
        .send_request(req, fast_p3_options(), p3_noop_cleanup_fut());
    let _ = Box::into_pin(result_fut).await;

    let records = records.lock().unwrap();
    assert!(
        records.iter().any(|(span, field)| {
            span == "spin_outbound_http.send_request" && field == "url.full"
        }),
        "`url.full` missing from `spin_outbound_http.send_request` span — \
         async block likely lost its `.in_current_span()` wrapper. \
         Recorded: {records:?}"
    );
    Ok(())
}

type CapturedRecords = Arc<Mutex<Vec<(String, String)>>>;

#[derive(Default)]
struct CaptureLayer {
    records: CapturedRecords,
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_record(&self, id: &span::Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let span_name = ctx
            .span(id)
            .map(|s| s.name().to_string())
            .unwrap_or_default();
        let mut v = CaptureVisitor {
            span_name: &span_name,
            records: &self.records,
        };
        values.record(&mut v);
    }
}

struct CaptureVisitor<'a> {
    span_name: &'a str,
    records: &'a CapturedRecords,
}

impl Visit for CaptureVisitor<'_> {
    fn record_debug(&mut self, f: &Field, _v: &dyn std::fmt::Debug) {
        self.records
            .lock()
            .unwrap()
            .push((self.span_name.to_string(), f.name().to_string()));
    }
}

fn empty_p3_body() -> UnsyncBoxBody<Bytes, p3_types::ErrorCode> {
    Empty::<Bytes>::new()
        .map_err(|never: std::convert::Infallible| match never {})
        .boxed_unsync()
}

fn fast_p3_options() -> Option<RequestOptions> {
    Some(RequestOptions {
        connect_timeout: Some(Duration::from_millis(10)),
        first_byte_timeout: Some(Duration::from_millis(10)),
        between_bytes_timeout: Some(Duration::from_millis(10)),
    })
}

fn p3_noop_cleanup_fut()
-> Box<dyn std::future::Future<Output = Result<(), p3_types::ErrorCode>> + Send> {
    Box::new(async { Ok(()) })
}
