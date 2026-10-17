use std::{fmt::Display, future::Future, io::Cursor};

use anyhow::Context;
use bytes::Bytes;
use futures_util::future::Either;
use http_body_util::Full;
use hyper::{
	body::Incoming, header::SEC_WEBSOCKET_PROTOCOL, server::conn::http1::Builder,
	header::HeaderValue, service::service_fn, upgrade::OnUpgrade, HeaderMap, Request, Response,
	StatusCode,
};
use hyper_util::rt::TokioIo;
use log::{debug, error, trace};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tokio_websockets::{Limits, ServerBuilder};
use wisp_mux::{
	ws::{TokioWebsocketsTransport, TransportExt, WebSocketSplitRead, WebSocketSplitWrite},
	WISPX_WS_PROTOCOL,
};

use crate::{
	config::SocketTransport,
	generate_stats,
	listener::{ServerStream, ServerStreamExt, ServerStreamRead, ServerStreamWrite},
	stream::WebSocketStreamWrapper,
	upgrade::{is_upgrade_request, upgrade},
	util_chain::{chain, Chain},
	util_map_err::MapErr,
	CONFIG,
};

pub type WispStreamRead = Either<
	WebSocketSplitRead<TokioWebsocketsTransport<Chain<Cursor<Bytes>, ServerStream>>>,
	MapErr<FramedRead<ServerStreamRead, LengthDelimitedCodec>>,
>;
pub type WispWsStreamWrite =
	WebSocketSplitWrite<TokioWebsocketsTransport<Chain<Cursor<Bytes>, ServerStream>>>;
pub type WispStreamWrite =
	Either<WispWsStreamWrite, MapErr<FramedWrite<ServerStreamWrite, LengthDelimitedCodec>>>;
pub type WispResult = (WispStreamRead, WispStreamWrite);

/// Which Wisp protocol a client asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WispClientProtocol {
	/// No websocket subprotocol: Wisp v1.
	V1,
	/// Some websocket subprotocol: Wisp v2.
	V2,
	/// The `wispx` websocket subprotocol.
	X,
}

pub enum ServerRouteResult {
	Wisp {
		stream: WispResult,
		protocol: WispClientProtocol,
	},
	Wispnet {
		stream: WispResult,
	},
	WsProxy {
		stream: WebSocketStreamWrapper,
		path: String,
		udp: bool,
	},
}

impl Display for ServerRouteResult {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Wisp { .. } => write!(f, "Wisp"),
			Self::Wispnet { .. } => write!(f, "Wispnet"),
			Self::WsProxy { path, udp, .. } => write!(f, "WsProxy path {path:?} udp {udp:?}"),
		}
	}
}

type Body = Full<Bytes>;
fn non_ws_resp() -> anyhow::Result<Response<Body>> {
	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(Body::new(CONFIG.server.non_ws_response.as_bytes().into()))?)
}

async fn send_stats() -> anyhow::Result<Response<Body>> {
	match generate_stats().await {
		Ok(x) => {
			debug!("sent server stats to http client");
			Ok(Response::builder()
				.status(StatusCode::OK)
				.body(Body::new(x.into()))?)
		}
		Err(x) => {
			error!("failed to send stats to http client: {:?}", x);
			Ok(Response::builder()
				.status(StatusCode::INTERNAL_SERVER_ERROR)
				.body(Body::new(x.to_string().into()))?)
		}
	}
}

fn get_header(headers: &HeaderMap, header: &str) -> Option<String> {
	headers
		.get(header)
		.and_then(|x| x.to_str().ok())
		.map(ToString::to_string)
}

enum HttpUpgradeResult {
	Wisp {
		protocol: WispClientProtocol,
		is_wispnet: bool,
	},
	WsProxy {
		path: String,
		udp: bool,
	},
}

async fn ws_upgrade<F, R>(
	mut req: Request<Incoming>,
	stats_endpoint: Option<String>,
	callback: F,
) -> anyhow::Result<Response<Body>>
where
	F: FnOnce(OnUpgrade, HttpUpgradeResult, Option<String>) -> R + Send + 'static,
	R: Future<Output = anyhow::Result<()>> + Send,
{
	let is_upgrade = is_upgrade_request(&req);

	if !is_upgrade {
		if let Some(stats_endpoint) = stats_endpoint {
			if req.uri().path() == stats_endpoint {
				return send_stats().await;
			}
		}

		debug!("sent non_ws_response to http client");
		return non_ws_resp();
	}

	trace!("recieved request {:?}", req);

	let (resp, fut) = upgrade(&mut req)?;
	// replace body of Empty<Bytes> with Full<Bytes>
	let mut resp = Response::from_parts(resp.into_parts().0, Body::new(Bytes::new()));

	let headers = req.headers();
	let ip_header = if CONFIG.server.use_real_ip_headers {
		get_header(headers, "x-real-ip").or_else(|| get_header(headers, "x-forwarded-for"))
	} else {
		None
	};

	let ws_protocol = headers.get(SEC_WEBSOCKET_PROTOCOL);
	let req_path = req.uri().path().to_string();

	if req_path.ends_with(&(CONFIG.wisp.prefix.clone() + "/")) {
		let is_wispnet =
			CONFIG.wisp.has_wispnet() && req.uri().query().unwrap_or_default() == "net";
		let (protocol, selected) = select_ws_protocol(ws_protocol);
		tokio::spawn(async move {
			if let Err(err) = (callback)(
				fut,
				HttpUpgradeResult::Wisp {
					protocol,
					is_wispnet,
				},
				ip_header,
			)
			.await
			{
				error!("error while serving client: {:?}", err);
			}
		});
		if let Some(selected) = selected {
			resp.headers_mut().append(SEC_WEBSOCKET_PROTOCOL, selected);
		}
	} else if CONFIG.wisp.allow_wsproxy {
		let udp = req.uri().query().unwrap_or_default() == "udp";
		tokio::spawn(async move {
			if let Err(err) = (callback)(
				fut,
				HttpUpgradeResult::WsProxy {
					path: req_path,
					udp,
				},
				ip_header,
			)
			.await
			{
				error!("error while serving client: {:?}", err);
			}
		});
	} else {
		debug!("sent non_ws_response to http client");
		return non_ws_resp();
	}

	Ok(resp)
}

/// Pick the Wisp protocol and the single subprotocol token to answer with.
///
/// RFC 6455 requires the response to name exactly one of the offered tokens.
fn select_ws_protocol(
	header: Option<&HeaderValue>,
) -> (WispClientProtocol, Option<HeaderValue>) {
	let Some(header) = header else {
		return (WispClientProtocol::V1, None);
	};
	let tokens = header
		.to_str()
		.unwrap_or_default()
		.split(',')
		.map(str::trim)
		.filter(|x| !x.is_empty());

	let mut first = None;
	for token in tokens {
		if CONFIG.wisp.wisp_v2 && CONFIG.wisp.wispx && token == WISPX_WS_PROTOCOL {
			return (
				WispClientProtocol::X,
				Some(HeaderValue::from_static(WISPX_WS_PROTOCOL)),
			);
		}
		first.get_or_insert(token);
	}

	let selected = first.and_then(|x| HeaderValue::from_str(x).ok());
	(WispClientProtocol::V2, selected.or_else(|| Some(header.clone())))
}

fn ws_builder() -> ServerBuilder {
	ServerBuilder::new()
		.limits(Limits::default().max_payload_len(Some(CONFIG.server.max_message_size)))
		// the multiplexor flushes once per batch, don't flush every 8 KiB in between
		.config(tokio_websockets::Config::default().flush_threshold(256 * 1024))
}

pub async fn route_stats(stream: ServerStream) -> anyhow::Result<()> {
	let stream = TokioIo::new(stream);
	Builder::new()
		.serve_connection(stream, service_fn(move |_| async { send_stats().await }))
		.await?;
	Ok(())
}

pub async fn route(
	stream: ServerStream,
	stats_endpoint: Option<String>,
	callback: impl FnOnce(ServerRouteResult, Option<String>) + Clone + Send + 'static,
) -> anyhow::Result<()> {
	match CONFIG.server.transport {
		SocketTransport::WebSocket => {
			let stream = TokioIo::new(stream);

			Builder::new()
				.serve_connection(
					stream,
					service_fn(move |req| {
						let callback = callback.clone();

						ws_upgrade(
							req,
							stats_endpoint.clone(),
							|fut, res, maybe_ip| async move {
								let ws = fut.await.context("failed to await upgrade future")?;

								match res {
									HttpUpgradeResult::Wisp {
										protocol,
										is_wispnet,
									} => {
										let ws = ws.downcast::<TokioIo<ServerStream>>().unwrap();
										let ws =
											chain(Cursor::new(ws.read_buf), ws.io.into_inner());

										let ws = ws_builder().serve(ws);
										let (read, write) =
											TokioWebsocketsTransport(ws).split_fast();

										let result = if is_wispnet {
											ServerRouteResult::Wispnet {
												stream: (Either::Left(read), Either::Left(write)),
											}
										} else {
											ServerRouteResult::Wisp {
												stream: (Either::Left(read), Either::Left(write)),
												protocol,
											}
										};

										(callback)(result, maybe_ip);
									}
									HttpUpgradeResult::WsProxy { path, udp } => {
										let ws = ws_builder().serve(TokioIo::new(ws));
										let ws = WebSocketStreamWrapper(ws);
										(callback)(
											ServerRouteResult::WsProxy {
												stream: ws,
												path,
												udp,
											},
											maybe_ip,
										);
									}
								}

								Ok(())
							},
						)
					}),
				)
				.with_upgrades()
				.await?;
		}
		SocketTransport::LengthDelimitedLe => {
			let codec = LengthDelimitedCodec::builder()
				.little_endian()
				.max_frame_length(usize::MAX)
				.new_codec();

			let (read, write) = stream.split();
			let read = MapErr(FramedRead::new(read, codec.clone()));
			let write = MapErr(FramedWrite::new(write, codec));

			(callback)(
				ServerRouteResult::Wisp {
					stream: (Either::Right(read), Either::Right(write)),
					// no negotiation on this transport, so keep the protocol existing clients speak
					protocol: WispClientProtocol::V2,
				},
				None,
			);
		}
	}
	Ok(())
}
