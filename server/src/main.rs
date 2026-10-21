#![deny(clippy::all)]
#![warn(clippy::nursery, clippy::pedantic, clippy::cargo)]
#![allow(clippy::cargo_common_metadata)]

mod domain {
    use anyhow::anyhow;
    use indexmap::IndexMap;
    use prost::bytes::Buf;
    use std::fmt::Debug;
    use std::str::Utf8Error;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct PlayerUuidString([u8; 36]);

    impl PlayerUuidString {
        pub const fn as_str(&self) -> Result<&str, Utf8Error> {
            std::str::from_utf8(&self.0)
        }

        pub fn from_string(str: &String) -> anyhow::Result<Self> {
            if !str.is_ascii() {
                Err(anyhow!("Expected ascii string for UuidString, got {str}"))
            } else if str.len() != 36 {
                Err(anyhow!(
                    "Expect string of length 36 for UuidString, got {str}"
                ))
            } else {
                let mut result: [u8; 36] = [0; 36];
                str.as_bytes().copy_to_slice(result.as_mut_slice());
                Ok(Self(result))
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct Player {
        pub uuid: PlayerUuidString,
    }

    #[derive(Debug, Clone)]
    pub struct PlayerBreakCount {
        pub player: Player,
        pub break_count: u64,
    }

    #[derive(Debug, Clone)]
    pub struct PlayerBuildCount {
        pub player: Player,
        pub build_count: u64,
    }

    #[derive(Debug, Clone)]
    pub struct PlayerPlayTicks {
        pub player: Player,
        pub play_ticks: u64,
    }

    #[derive(Debug, Clone)]
    pub struct PlayerVoteCount {
        pub player: Player,
        pub vote_count: u64,
    }

    #[derive(Debug, Clone, Default)]
    pub struct AggregatedPlayerData {
        pub break_count: u64,
        pub build_count: u64,
        pub play_ticks: u64,
        pub vote_count: u64,
    }

    #[derive(Debug, Clone, Default)]
    pub struct KnownAggregatedPlayerData(pub IndexMap<Player, AggregatedPlayerData>);

    #[async_trait::async_trait]
    pub trait PlayerDataRepository: Debug + Sync + Send + 'static {
        async fn get_all_break_counts(&self) -> anyhow::Result<Vec<PlayerBreakCount>>;
        async fn get_all_build_counts(&self) -> anyhow::Result<Vec<PlayerBuildCount>>;
        async fn get_all_play_ticks(&self) -> anyhow::Result<Vec<PlayerPlayTicks>>;
        async fn get_all_vote_counts(&self) -> anyhow::Result<Vec<PlayerVoteCount>>;
    }
}

mod use_cases {
    use crate::domain::{AggregatedPlayerData, KnownAggregatedPlayerData, PlayerDataRepository};
    use indexmap::IndexMap;
    use std::sync::Arc;

    #[derive(Debug, Clone)]
    pub struct GetAllPlayerDataUseCase {
        pub repository: Arc<dyn PlayerDataRepository>,
    }

    impl GetAllPlayerDataUseCase {
        #[tracing::instrument(skip(self))]
        pub async fn get_all_known_aggregated_player_data(
            &self,
        ) -> anyhow::Result<KnownAggregatedPlayerData> {
            let (break_counts, build_counts, play_ticks, vote_counts) = tokio::try_join!(
                self.repository.get_all_break_counts(),
                self.repository.get_all_build_counts(),
                self.repository.get_all_play_ticks(),
                self.repository.get_all_vote_counts(),
            )?;

            let mut result_map: IndexMap<_, AggregatedPlayerData> =
                IndexMap::with_capacity(break_counts.len());

            for break_count in break_counts {
                let entry = result_map.entry(break_count.player).or_default();
                entry.break_count = break_count.break_count;
            }

            for build_count in build_counts {
                let entry = result_map.entry(build_count.player).or_default();
                entry.build_count = build_count.build_count;
            }

            for tick_count in play_ticks {
                let entry = result_map.entry(tick_count.player).or_default();
                entry.play_ticks = tick_count.play_ticks;
            }

            for vote_count in vote_counts {
                let entry = result_map.entry(vote_count.player).or_default();
                entry.vote_count = vote_count.vote_count;
            }

            Ok(KnownAggregatedPlayerData(result_map))
        }
    }
}

mod infra_axum_handlers {
    use crate::domain::PlayerDataRepository;
    use crate::use_cases::GetAllPlayerDataUseCase;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, MethodRouter};
    use std::sync::Arc;

    #[derive(Clone, Debug)]
    pub struct SharedAppState {
        pub repository: Arc<dyn PlayerDataRepository>,
    }

    mod presenter {
        use crate::domain::{KnownAggregatedPlayerData, Player};
        use std::fmt::Write;

        fn estimate_presented_string_size(data: &KnownAggregatedPlayerData) -> usize {
            // Each Prometheus record takes about 85 characters and 4 records are generated per
            // aggregated player data, hence length * 340. The constant term is from the help string.
            100 + data.0.len() * 340
        }

        fn write_record(
            target: &mut String,
            player: &Player,
            kind: &'static str,
            value: u64,
        ) -> anyhow::Result<()> {
            Ok(target.write_str(&format!(
                r#"player_data{{uuid="{}",kind="{}"}} {}{}"#,
                player.uuid.as_str()?,
                kind,
                value,
                '\n'
            ))?)
        }

        #[tracing::instrument(skip(data), fields(player_count = data.0.len()))]
        pub fn present_player_data_as_prometheus_metrics(
            data: &KnownAggregatedPlayerData,
        ) -> anyhow::Result<String> {
            let mut result = String::with_capacity(estimate_presented_string_size(data));

            result
                .write_str("# HELP player_data Player metrics, partitioned by uuid and kind\n")?;
            result.write_str("# TYPE player_data gauge\n")?;

            for (player, data) in &data.0 {
                write_record(&mut result, player, "break_count", data.break_count)?;
                write_record(&mut result, player, "build_count", data.build_count)?;
                write_record(&mut result, player, "play_ticks", data.play_ticks)?;
                write_record(&mut result, player, "vote_count", data.vote_count)?;
            }

            Ok(result)
        }
    }

    fn const_error_response() -> (StatusCode, String) {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Encountered internal server error. Please contact the server administrator to resolve the issue.".to_string(),
        )
    }

    /// Handler for the `GET /metrics` endpoint.
    pub fn handle_get_metrics() -> MethodRouter<SharedAppState> {
        // we need a separate handler function to create an error tracing span
        #[tracing::instrument(skip(state))]
        async fn handle_request(state: &SharedAppState) -> Response {
            let use_case = GetAllPlayerDataUseCase {
                repository: state.repository.clone(),
            };

            match use_case
                .get_all_known_aggregated_player_data()
                .await
                .and_then(|known_aggregated_player_data| {
                    presenter::present_player_data_as_prometheus_metrics(
                        &known_aggregated_player_data,
                    )
                }) {
                Ok(metrics_presentation) => (StatusCode::OK, metrics_presentation).into_response(),
                Err(e) => {
                    tracing::error!("{:?}", e);
                    const_error_response().into_response()
                }
            }
        }

        get(|State(s): State<SharedAppState>| async move { handle_request(&s).await })
    }
}

mod infra_repository_impls {
    #[allow(dead_code)]
    #[allow(clippy::nursery, clippy::pedantic, clippy::all)]
    mod buf_generated {
        include!("gen/mod.rs");
    }

    pub mod config {
        #[derive(serde::Deserialize, Debug, Clone)]
        pub struct GrpcClient {
            pub game_data_server_grpc_endpoint_url: String,
        }

        impl GrpcClient {
            pub fn from_env() -> anyhow::Result<Self> {
                Ok(envy::from_env::<Self>()?)
            }
        }
    }

    mod buf_generated_to_domain {
        use super::buf_generated::gigantic_minecraft::seichi_game_data::v1 as generated;
        use crate::domain;
        use crate::domain::PlayerUuidString;

        fn into_domain_player(p: &generated::Player) -> anyhow::Result<domain::Player> {
            Ok(domain::Player {
                uuid: PlayerUuidString::from_string(&p.uuid)?,
            })
        }

        fn extract_domain_player(
            player: Option<generated::Player>,
        ) -> anyhow::Result<domain::Player> {
            let player = player.ok_or_else(|| anyhow::anyhow!("Player field not set"))?;

            into_domain_player(&player)
        }

        pub fn try_into_domain_player_break_count(
            value: generated::PlayerBreakCount,
        ) -> anyhow::Result<domain::PlayerBreakCount> {
            Ok(domain::PlayerBreakCount {
                player: extract_domain_player(value.player)?,
                break_count: value.break_count,
            })
        }

        pub fn try_into_domain_player_build_count(
            value: generated::PlayerBuildCount,
        ) -> anyhow::Result<domain::PlayerBuildCount> {
            Ok(domain::PlayerBuildCount {
                player: extract_domain_player(value.player)?,
                build_count: value.build_count,
            })
        }

        pub fn try_into_domain_player_play_ticks(
            value: generated::PlayerPlayTicks,
        ) -> anyhow::Result<domain::PlayerPlayTicks> {
            Ok(domain::PlayerPlayTicks {
                player: extract_domain_player(value.player)?,
                play_ticks: value.play_ticks,
            })
        }

        pub fn try_into_domain_player_vote_count(
            value: generated::PlayerVoteCount,
        ) -> anyhow::Result<domain::PlayerVoteCount> {
            Ok(domain::PlayerVoteCount {
                player: extract_domain_player(value.player)?,
                vote_count: value.vote_count,
            })
        }
    }

    use buf_generated::gigantic_minecraft::seichi_game_data::v1::read_service_client::ReadServiceClient;
    use tonic_tracing_opentelemetry::middleware::client::{OtelGrpcLayer, OtelGrpcService};
    use tower::Layer;
    type GameDataGrpcClient = ReadServiceClient<OtelGrpcService<tonic::transport::Channel>>;

    /// Upper bound for establishing a TCP/HTTP2 connection to the upstream.
    const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
    /// Upper bound for a single RPC. Must stay below the cache-proxy nginx
    /// `proxy_read_timeout` (nginx default: 60s) so that the proxy receives our
    /// error response instead of timing out on its own.
    const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    #[derive(Debug)]
    pub struct GameDataGrpcRepository {
        client: GameDataGrpcClient,
    }

    impl GameDataGrpcRepository {
        #[tracing::instrument(skip(config), fields(endpoint = %config.game_data_server_grpc_endpoint_url))]
        pub fn initialize_connections_with(config: config::GrpcClient) -> anyhow::Result<Self> {
            // Build the channel manually so we can wrap it with the OTel gRPC
            // layer; the layer injects the W3C trace context into outgoing
            // requests and creates a span per RPC call.
            //
            // The channel connects lazily so that the publisher can start (and
            // keep running) while the upstream game data server is down;
            // requests made in the meantime fail fast with an error response
            // instead of crash-looping the whole process.
            let channel =
                tonic::transport::Channel::from_shared(config.game_data_server_grpc_endpoint_url)?
                    .connect_timeout(CONNECT_TIMEOUT)
                    .timeout(REQUEST_TIMEOUT)
                    .connect_lazy();
            let channel = OtelGrpcLayer.layer(channel);

            Ok(Self {
                client: ReadServiceClient::new(channel),
            })
        }

        pub(crate) fn game_data_client(&self) -> GameDataGrpcClient {
            self.client.clone()
        }
    }

    fn empty_request() -> tonic::Request<pbjson_types::Empty> {
        tonic::Request::new(pbjson_types::Empty::default())
    }

    use crate::domain::{PlayerBreakCount, PlayerBuildCount, PlayerPlayTicks, PlayerVoteCount};
    use crate::telemetry_metrics::record_rpc_client_call;

    #[async_trait::async_trait]
    impl crate::domain::PlayerDataRepository for GameDataGrpcRepository {
        #[tracing::instrument(skip(self))]
        async fn get_all_break_counts(&self) -> anyhow::Result<Vec<PlayerBreakCount>> {
            Ok(record_rpc_client_call(
                "gigantic_minecraft.seichi_game_data.v1.ReadService/BreakCounts",
                self.game_data_client().break_counts(empty_request()),
            )
            .await?
            .into_inner()
            .results
            .into_iter()
            .map(buf_generated_to_domain::try_into_domain_player_break_count)
            .collect::<Result<_, _>>()?)
        }

        #[tracing::instrument(skip(self))]
        async fn get_all_build_counts(&self) -> anyhow::Result<Vec<PlayerBuildCount>> {
            Ok(record_rpc_client_call(
                "gigantic_minecraft.seichi_game_data.v1.ReadService/BuildCounts",
                self.game_data_client().build_counts(empty_request()),
            )
            .await?
            .into_inner()
            .results
            .into_iter()
            .map(buf_generated_to_domain::try_into_domain_player_build_count)
            .collect::<Result<_, _>>()?)
        }

        #[tracing::instrument(skip(self))]
        async fn get_all_play_ticks(&self) -> anyhow::Result<Vec<PlayerPlayTicks>> {
            Ok(record_rpc_client_call(
                "gigantic_minecraft.seichi_game_data.v1.ReadService/PlayTicks",
                self.game_data_client().play_ticks(empty_request()),
            )
            .await?
            .into_inner()
            .results
            .into_iter()
            .map(buf_generated_to_domain::try_into_domain_player_play_ticks)
            .collect::<Result<_, _>>()?)
        }

        #[tracing::instrument(skip(self))]
        async fn get_all_vote_counts(&self) -> anyhow::Result<Vec<PlayerVoteCount>> {
            Ok(record_rpc_client_call(
                "gigantic_minecraft.seichi_game_data.v1.ReadService/VoteCounts",
                self.game_data_client().vote_counts(empty_request()),
            )
            .await?
            .into_inner()
            .results
            .into_iter()
            .map(buf_generated_to_domain::try_into_domain_player_vote_count)
            .collect::<Result<_, _>>()?)
        }
    }
}

// Continuous profiling agent (pyroscope-rs).
//
// Pushes pprof CPU profiles to a Pyroscope server while the returned guard is
// alive. Disabled at runtime when `PYROSCOPE_SERVER_ADDRESS` is not set so the
// binary remains usable in environments without a profiler endpoint.
mod profiler {
    use pyroscope::backend::{pprof_backend, BackendConfig, PprofConfig};
    use pyroscope::pyroscope::PyroscopeAgentBuilder;

    pub struct Guard {
        shutdown: Option<Box<dyn FnOnce() + Send>>,
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            if let Some(f) = self.shutdown.take() {
                f();
            }
        }
    }

    pub fn try_start() -> anyhow::Result<Guard> {
        let Ok(server_address) = std::env::var("PYROSCOPE_SERVER_ADDRESS") else {
            tracing::info!("PYROSCOPE_SERVER_ADDRESS unset; continuous profiler disabled");
            return Ok(Guard { shutdown: None });
        };

        let app_name = std::env::var("PYROSCOPE_APPLICATION_NAME")
            .unwrap_or_else(|_| env!("CARGO_PKG_NAME").to_string());
        let sample_rate: u32 = std::env::var("PYROSCOPE_SAMPLE_RATE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);

        let agent = PyroscopeAgentBuilder::new(
            &server_address,
            &app_name,
            sample_rate,
            "pyroscope-rs",
            env!("CARGO_PKG_VERSION"),
            pprof_backend(PprofConfig::default(), BackendConfig::default()),
        )
        .build()?;
        let running = agent.start()?;
        tracing::info!(
            server = %server_address,
            application = %app_name,
            sample_rate = sample_rate,
            "continuous profiler started"
        );

        Ok(Guard {
            shutdown: Some(Box::new(move || match running.stop() {
                Ok(ready) => ready.shutdown(),
                Err(e) => eprintln!("pyroscope agent stop failed: {e}"),
            })),
        })
    }
}

// Application-level OpenTelemetry metrics.
//
// Recorded through the global meter provider that init-tracing-opentelemetry
// installs, and pushed over OTLP together with the traces. Names, units and
// attributes follow the OpenTelemetry semantic conventions. These are
// unrelated to the `GET /metrics` endpoint, which serves player data.
mod telemetry_metrics {
    use axum::extract::{MatchedPath, Request};
    use axum::http::Method;
    use axum::middleware::Next;
    use axum::response::Response;
    use opentelemetry::metrics::Histogram;
    use opentelemetry::KeyValue;
    use std::future::Future;
    use std::sync::LazyLock;
    use std::time::Instant;

    /// Histogram bucket boundaries in seconds: the ones advised by the
    /// semantic conventions for `http.server.request.duration`, extended with
    /// 30s and 60s. Calls cut off by the upstream `REQUEST_TIMEOUT` (30s) end
    /// slightly after 30s and so fall into the (30, 60] bucket instead of +Inf.
    const DURATION_BUCKETS_SECONDS: [f64; 16] = [
        0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 30.0, 60.0,
    ];

    /// `error.type` for a request or call whose future was dropped before it
    /// completed: an HTTP request abandoned by the client (or cut off by the
    /// shutdown drain timeout), or an RPC abandoned by this process because
    /// `tokio::try_join!` gave up after a sibling call failed. Kept apart from
    /// the status codes so that error-ratio queries can tell these collateral
    /// cancellations from failures of the call itself.
    const ERROR_TYPE_ABANDONED: &str = "abandoned";

    struct Instruments {
        http_server_request_duration: Histogram<f64>,
        rpc_client_call_duration: Histogram<f64>,
    }

    /// Created on first use, which is after `init_subscriber` has installed the
    /// global meter provider. Instruments obtained from the default no-op
    /// provider before that would never be exported.
    static INSTRUMENTS: LazyLock<Instruments> = LazyLock::new(|| {
        let meter = opentelemetry::global::meter(env!("CARGO_PKG_NAME"));
        Instruments {
            http_server_request_duration: meter
                .f64_histogram("http.server.request.duration")
                .with_unit("s")
                .with_description("Duration of HTTP server requests.")
                .with_boundaries(DURATION_BUCKETS_SECONDS.to_vec())
                .build(),
            rpc_client_call_duration: meter
                .f64_histogram("rpc.client.call.duration")
                .with_unit("s")
                .with_description("Duration of outgoing RPC calls.")
                .with_boundaries(DURATION_BUCKETS_SECONDS.to_vec())
                .build(),
        }
    });

    /// `http.request.method` value: the known methods as-is, anything else as
    /// `_OTHER` so that arbitrary client input cannot inflate cardinality.
    const fn http_method_attribute(method: &Method) -> &'static str {
        match *method {
            Method::GET => "GET",
            Method::HEAD => "HEAD",
            Method::POST => "POST",
            Method::PUT => "PUT",
            Method::DELETE => "DELETE",
            Method::CONNECT => "CONNECT",
            Method::OPTIONS => "OPTIONS",
            Method::TRACE => "TRACE",
            Method::PATCH => "PATCH",
            _ => "_OTHER",
        }
    }

    /// Records `http.server.request.duration` when dropped, so that requests
    /// whose handler future is dropped before completion are recorded too.
    struct HttpRequestRecorder {
        start: Instant,
        method: &'static str,
        /// Absent for requests that did not match any route (404), which
        /// keeps unknown paths out of the attribute values.
        route: Option<String>,
        /// `None` while the request is in flight.
        status: Option<axum::http::StatusCode>,
    }

    impl Drop for HttpRequestRecorder {
        fn drop(&mut self) {
            let mut attributes = vec![KeyValue::new("http.request.method", self.method)];
            if let Some(route) = self.route.take() {
                attributes.push(KeyValue::new("http.route", route));
            }
            match self.status {
                Some(status) => {
                    attributes.push(KeyValue::new(
                        "http.response.status_code",
                        i64::from(status.as_u16()),
                    ));
                    if status.is_server_error() {
                        attributes.push(KeyValue::new("error.type", status.as_str().to_owned()));
                    }
                }
                None => attributes.push(KeyValue::new("error.type", ERROR_TYPE_ABANDONED)),
            }
            INSTRUMENTS
                .http_server_request_duration
                .record(self.start.elapsed().as_secs_f64(), &attributes);
        }
    }

    /// axum middleware recording `http.server.request.duration`.
    pub async fn record_http_server_request_duration(request: Request, next: Next) -> Response {
        let mut recorder = HttpRequestRecorder {
            start: Instant::now(),
            method: http_method_attribute(request.method()),
            route: request
                .extensions()
                .get::<MatchedPath>()
                .map(|path| path.as_str().to_owned()),
            status: None,
        };
        let response = next.run(request).await;
        recorder.status = Some(response.status());
        response
    }

    /// Canonical gRPC status code name, as used for `rpc.response.status_code`.
    pub const fn grpc_status_code_name(code: tonic::Code) -> &'static str {
        match code {
            tonic::Code::Ok => "OK",
            tonic::Code::Cancelled => "CANCELLED",
            tonic::Code::Unknown => "UNKNOWN",
            tonic::Code::InvalidArgument => "INVALID_ARGUMENT",
            tonic::Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
            tonic::Code::NotFound => "NOT_FOUND",
            tonic::Code::AlreadyExists => "ALREADY_EXISTS",
            tonic::Code::PermissionDenied => "PERMISSION_DENIED",
            tonic::Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
            tonic::Code::FailedPrecondition => "FAILED_PRECONDITION",
            tonic::Code::Aborted => "ABORTED",
            tonic::Code::OutOfRange => "OUT_OF_RANGE",
            tonic::Code::Unimplemented => "UNIMPLEMENTED",
            tonic::Code::Internal => "INTERNAL",
            tonic::Code::Unavailable => "UNAVAILABLE",
            tonic::Code::DataLoss => "DATA_LOSS",
            tonic::Code::Unauthenticated => "UNAUTHENTICATED",
        }
    }

    /// Status code reported for a failed call.
    ///
    /// tonic turns the expiry of the channel's request timeout into a
    /// `CANCELLED` status (with `TimeoutExpired`'s message), which would look
    /// like a cancellation. Report it as `DEADLINE_EXCEEDED`, which is what the
    /// call actually ran into.
    fn rpc_error_code(status: &tonic::Status) -> tonic::Code {
        if status.code() == tonic::Code::Cancelled
            && status.message() == tonic::TimeoutExpired(()).to_string()
        {
            tonic::Code::DeadlineExceeded
        } else {
            status.code()
        }
    }

    /// Records `rpc.client.call.duration` when dropped, so that calls whose
    /// future is dropped before completion are recorded as well.
    struct RpcCallRecorder {
        method: &'static str,
        start: Instant,
        /// `None` while the call is in flight.
        status_code: Option<tonic::Code>,
    }

    impl Drop for RpcCallRecorder {
        fn drop(&mut self) {
            let mut attributes = vec![
                KeyValue::new("rpc.system.name", "grpc"),
                KeyValue::new("rpc.method", self.method),
            ];
            match self.status_code {
                Some(code) => {
                    let name = grpc_status_code_name(code);
                    attributes.push(KeyValue::new("rpc.response.status_code", name));
                    if code != tonic::Code::Ok {
                        attributes.push(KeyValue::new("error.type", name));
                    }
                }
                None => attributes.push(KeyValue::new("error.type", ERROR_TYPE_ABANDONED)),
            }
            INSTRUMENTS
                .rpc_client_call_duration
                .record(self.start.elapsed().as_secs_f64(), &attributes);
        }
    }

    /// Awaits a unary gRPC call and records `rpc.client.call.duration`.
    ///
    /// `method` is the fully-qualified `package.Service/Method` name. This
    /// wraps the call future rather than the channel because the future of a
    /// unary tonic call resolves only once the response message has been
    /// received and decoded, whereas a layer on the channel would stop the
    /// clock at the response headers, before the (large) body is transferred.
    pub async fn record_rpc_client_call<T>(
        method: &'static str,
        call: impl Future<Output = Result<T, tonic::Status>>,
    ) -> Result<T, tonic::Status> {
        let mut recorder = RpcCallRecorder {
            method,
            start: Instant::now(),
            status_code: None,
        };
        let result = call.await;
        recorder.status_code = Some(match &result {
            Ok(_) => tonic::Code::Ok,
            Err(status) => rpc_error_code(status),
        });
        result
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn grpc_status_code_names_follow_the_canonical_spelling() {
            assert_eq!(grpc_status_code_name(tonic::Code::Ok), "OK");
            assert_eq!(
                grpc_status_code_name(tonic::Code::DeadlineExceeded),
                "DEADLINE_EXCEEDED"
            );
            assert_eq!(
                grpc_status_code_name(tonic::Code::Unavailable),
                "UNAVAILABLE"
            );
        }

        #[test]
        fn request_timeouts_are_reported_as_deadline_exceeded() {
            // Built the same way tonic's channel converts its timeout error.
            let timeout = tonic::Status::from_error(Box::new(tonic::TimeoutExpired(())));
            assert_eq!(timeout.code(), tonic::Code::Cancelled);
            assert_eq!(rpc_error_code(&timeout), tonic::Code::DeadlineExceeded);

            let cancelled = tonic::Status::cancelled("cancelled by the server");
            assert_eq!(rpc_error_code(&cancelled), tonic::Code::Cancelled);
        }

        #[test]
        fn unknown_http_methods_are_reported_as_other() {
            assert_eq!(http_method_attribute(&Method::GET), "GET");
            assert_eq!(
                http_method_attribute(&Method::from_bytes(b"PURGE").unwrap()),
                "_OTHER"
            );
        }
    }
}

mod app {
    use crate::infra_axum_handlers;
    use crate::infra_axum_handlers::SharedAppState;
    use crate::infra_repository_impls;
    use crate::profiler;
    use crate::telemetry_metrics;
    use axum::Router;
    use axum_tracing_opentelemetry::middleware::OtelAxumLayer;
    use init_tracing_opentelemetry::resource::DetectResource;
    use init_tracing_opentelemetry::FmtTraceContext;
    use std::future::IntoFuture;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::signal::unix::{signal, Signal, SignalKind};
    use tokio::sync::Notify;

    pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
        // The binary runs as PID 1 in a distroless image, so SIGTERM has no
        // default action: without a handler the pod is only stopped by the
        // SIGKILL at the end of terminationGracePeriodSeconds, and the
        // telemetry guards below never get a chance to flush. Register the
        // handlers before anything else so that a pod terminated during
        // startup is not left ignoring SIGTERM.
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigint = signal(SignalKind::interrupt())?;

        // OpenTelemetry pipeline: traces + metrics over OTLP, plus JSON logs to
        // stdout (picked up by the cluster's log scraper). Configuration is
        // taken from `OTEL_*` environment variables (OTEL_SERVICE_NAME,
        // OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_PROTOCOL, ...).
        // Without the OTLP vars only the stdout sink is active, so local runs
        // do not require a collector.
        //
        // Logs are deliberately not exported over OTLP: the stdout lines are
        // already ingested by the log scraper, so exporting them as well would
        // store every line twice. Instead each line logged inside a span
        // carries `trace_id` / `span_id` fields for log <-> trace correlation.
        // The `logs` feature is not enabled in Cargo.toml either;
        // `with_logs(false)` keeps the export off even if another dependency
        // turns that feature on through feature unification.
        let otel_guard = init_tracing_opentelemetry::TracingConfig::default()
            .with_json_format()
            .with_stdout()
            .with_fmt_trace_context(FmtTraceContext::TraceIdSpanId)
            .with_log_directives(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
            .with_otel(true)
            .with_resource_config(
                DetectResource::default().with_fallback_service_version(service_version()),
            )
            .with_logs(false)
            .with_metrics(true)
            .init_subscriber()?;

        let profiler_guard = profiler::try_start()?;

        let shared_state = {
            let client_config = infra_repository_impls::config::GrpcClient::from_env()?;
            let repository =
                infra_repository_impls::GameDataGrpcRepository::initialize_connections_with(
                    client_config,
                )?;

            SharedAppState {
                repository: Arc::new(repository),
            }
        };

        let routes: Router = Router::new()
            .route("/metrics", infra_axum_handlers::handle_get_metrics())
            .layer(axum::middleware::from_fn(
                telemetry_metrics::record_http_server_request_duration,
            ))
            .layer(OtelAxumLayer::default())
            .with_state(shared_state.clone());

        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 80));
        tracing::info!("listening on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;

        let shutdown = Arc::new(Notify::new());
        let server = axum::serve(listener, routes)
            .with_graceful_shutdown({
                let shutdown = shutdown.clone();
                async move { shutdown.notified().await }
            })
            .into_future();
        tokio::pin!(server);

        tokio::select! {
            result = &mut server => result?,
            signal = wait_for_shutdown_signal(&mut sigterm, &mut sigint) => {
                tracing::info!(signal, "shutdown signal received");
                // Keep serving for a moment: the endpoint removal triggered by
                // the pod deletion reaches kube-proxy asynchronously, so the
                // cache-proxy may still route new connections here.
                tokio::time::sleep(PRE_DRAIN_DELAY).await;
                tracing::info!("draining in-flight requests");
                // `notify_one` stores a permit, so this is not lost even if the
                // server has not polled the shutdown future yet.
                shutdown.notify_one();
                // Bound the drain so that the telemetry flush below still runs
                // before Kubernetes' SIGKILL (default grace period: 30s).
                if let Ok(result) = tokio::time::timeout(DRAIN_TIMEOUT, &mut server).await {
                    result?;
                } else {
                    tracing::warn!(
                        timeout_secs = DRAIN_TIMEOUT.as_secs(),
                        "in-flight requests did not finish in time; shutting down anyway"
                    );
                }
            }
        }

        // Flush traces/metrics first, then stop the profiler.
        tracing::info!("flushing telemetry");
        drop(otel_guard);
        drop(profiler_guard);
        tracing::info!("shutdown complete");

        Ok(())
    }

    /// Default for the `service.version` resource attribute: the git commit
    /// passed as `GIT_SHA` at build time (CI sets it), or the crate version
    /// for local builds. A `SERVICE_VERSION` env var still takes precedence.
    fn service_version() -> &'static str {
        option_env!("GIT_SHA")
            .filter(|sha| !sha.is_empty())
            .unwrap_or(env!("CARGO_PKG_VERSION"))
    }

    /// How long to keep accepting connections after a shutdown signal, so that
    /// the endpoint removal can propagate before the listener is closed.
    const PRE_DRAIN_DELAY: Duration = Duration::from_secs(5);
    /// Upper bound for draining in-flight requests. `PRE_DRAIN_DELAY` +
    /// `DRAIN_TIMEOUT` is kept well below the default 30s grace period to
    /// leave time for the telemetry flush.
    const DRAIN_TIMEOUT: Duration = Duration::from_secs(15);

    /// Resolves with the signal name when SIGTERM (sent by Kubernetes on pod
    /// termination) or SIGINT (Ctrl+C in local runs) is received.
    async fn wait_for_shutdown_signal(sigterm: &mut Signal, sigint: &mut Signal) -> &'static str {
        tokio::select! {
            _ = sigterm.recv() => "SIGTERM",
            _ = sigint.recv() => "SIGINT",
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    app::main().await
}
