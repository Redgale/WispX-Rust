//! State shared between the multiplexor driver (reader + writer) and stream handles.
//!
//! Lock order is stream `state` -> `streams` -> `out`: nothing takes a stream's state lock
//! while holding the stream map or the writer queue. Wakers may be woken with a stream's state
//! lock held; they only schedule tasks.

use std::{
	collections::VecDeque,
	mem,
	sync::{
		atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
		Arc, Mutex, MutexGuard,
	},
	task::{Context, Poll, Waker},
};

use bytes::{Bytes, BytesMut};
use rustc_hash::FxHashMap;

use crate::{
	frame::{OutFrame, Proto, DATA_HEADROOM},
	packet::{CloseReason, ConnectPacket, StreamType},
	Role, WispError,
};

/// Credit value meaning "not flow controlled".
pub(crate) const UNLIMITED: u64 = u64::MAX;
/// v1/v2 server: once a stream has this much unread data, stop reading the connection until it
/// drains. v1/v2 clients ignore CONTINUE, so this (TCP backpressure) is the only way to bound
/// memory without dropping data, which is what the reference server does.
const V2_PAUSE_BYTES: usize = 4 * 1024 * 1024;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
	// a panic while holding one of these locks leaves the state consistent enough to keep going
	mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Parameters fixed once the handshake completes.
#[derive(Debug, Clone)]
pub(crate) struct MuxParams {
	pub proto: Proto,
	pub role: Role,
	/// Largest DATA payload we may put in one frame.
	pub max_frame_payload: usize,
	/// WispX: cap on coalesced message size.
	pub batch_size: usize,
	/// Initial send credit for flow controlled streams.
	/// WispX: bytes (peer's stream window). v1/v2 client: packets (server's buffer size).
	pub tx_window: u64,
	/// Receive window we grant on flow controlled streams.
	/// WispX: bytes (our initial stream window). v1/v2 server: packets (our buffer size).
	pub rx_window: u64,
	/// WispX: the receive window grows up to this for streams whose reader keeps up.
	pub rx_max_window: u64,
	/// WispX: total window growth allowed across all streams of the connection.
	pub rx_max_growth: u64,
	/// Queued bytes above which data senders wait for the writer.
	pub out_high_watermark: usize,
	/// v1/v2: stream types that are flow controlled, besides TCP.
	pub flow_stream_types: Box<[u8]>,
}

impl MuxParams {
	fn is_flow_controlled(&self, ty: StreamType) -> bool {
		match self.proto {
			Proto::X => ty != StreamType::Udp,
			Proto::V1 | Proto::V2 => {
				ty == StreamType::Tcp || self.flow_stream_types.contains(&u8::from(ty))
			}
		}
	}

	/// Whether we send flow control updates to the peer for streams we receive on.
	fn grants_credit(&self) -> bool {
		self.proto == Proto::X || self.role == Role::Server
	}

	/// Whether we wait for the peer's flow control credit before sending.
	///
	/// Only WispX. In v1/v2, deployed clients don't honor CONTINUE (wisp-mux 6's `AsyncWrite`,
	/// which epoxy uses for every TLS connection, never checks it) and the reference server
	/// relies on that: it queues CONTINUE packets without flushing them, so a client that waits
	/// for them stalls until something else flushes the websocket. A v1/v2 server instead pushes
	/// back by pausing reads, see [`MuxShared::on_data`].
	fn tracks_credit(&self) -> bool {
		self.proto == Proto::X
	}
}

pub(crate) struct StreamState {
	pub rx: VecDeque<Bytes>,
	rx_bytes: usize,
	rx_waker: Option<Waker>,
	/// Peer sent FIN.
	pub rx_eof: bool,
	/// Consumed by the application but not yet credited back (bytes for X, packets for v2).
	rx_unacked: u64,
	/// WispX: how much more the peer may send before violating flow control.
	rx_peer_credit: i64,
	/// WispX: current receive window, see `account_consumed`.
	rx_window: u64,
	/// WispX: bytes consumed since the window last grew.
	rx_since_growth: u64,
	/// WispX: the reader found nothing to read and had to wait, since the window last grew.
	rx_starved: bool,
	/// WispX: how much this stream's window has grown, counted against the connection budget.
	rx_grown: u64,

	pub tx_credit: u64,
	tx_waker: Option<Waker>,
	/// We sent FIN.
	pub tx_fin: bool,

	pub closed: Option<CloseReason>,
	close_wakers: Vec<Waker>,
	/// v1/v2 server: the read loop, waiting for this stream to drain.
	drain_waker: Option<Waker>,
	/// Client: the stream hasn't been announced yet. See [`MuxShared::push_opening`].
	pending_connect: Option<ConnectPacket>,
}

impl StreamState {
	fn fully_finished(&self) -> bool {
		self.tx_fin && self.rx_eof
	}

	fn take_all_wakers(&mut self) -> Vec<Waker> {
		let mut wakers = mem::take(&mut self.close_wakers);
		wakers.extend(self.rx_waker.take());
		wakers.extend(self.tx_waker.take());
		wakers.extend(self.drain_waker.take());
		wakers
	}
}

pub(crate) struct StreamShared {
	/// 0 until a client stream is announced, see [`MuxShared::push_opening`].
	id: AtomicU32,
	pub stream_type: StreamType,
	pub flow: bool,
	/// Number of live user-facing handles. When it hits zero the stream is closed.
	handles: AtomicUsize,
	/// Removed from the stream map. Only changed while holding `state`.
	removed: AtomicBool,
	state: Mutex<StreamState>,
}

impl StreamShared {
	pub fn lock(&self) -> MutexGuard<'_, StreamState> {
		lock(&self.state)
	}

	/// The stream ID, or 0 if this client stream hasn't been announced yet.
	pub fn id(&self) -> u32 {
		self.id.load(Ordering::Acquire)
	}
}

enum OutState {
	Open,
	Closing(Option<CloseReason>),
	Dead,
}

struct OutQueue {
	frames: VecDeque<OutFrame>,
	/// Unannounced client streams whose reader is waiting; the writer announces them.
	announce: Vec<Arc<StreamShared>>,
	bytes: usize,
	writer: Option<Waker>,
	blocked: Vec<Waker>,
	state: OutState,
}

pub(crate) enum WriterEvent {
	/// Frames were moved into the writer's buffer.
	Frames,
	/// The queue is drained and the connection should be closed, optionally with a reason.
	Close(Option<CloseReason>),
	/// The multiplexor is dead.
	Dead,
}

pub(crate) type StreamMap = FxHashMap<u32, Arc<StreamShared>>;
/// Server: where accepted streams are handed to `ServerMux::wait_for_stream`.
pub(crate) type NewStreamSender = flume::Sender<(ConnectPacket, Arc<StreamShared>)>;

pub(crate) struct MuxShared {
	pub params: MuxParams,
	streams: Mutex<StreamMap>,
	out: Mutex<OutQueue>,
	next_stream_id: AtomicU32,
	dead: AtomicBool,
	/// Multiplexor handle plus all stream handles. When it hits zero the connection is closed.
	handles: AtomicUsize,
	/// WispX: window growth handed out to open streams, see `MuxParams::rx_max_growth`.
	rx_growth: AtomicU64,
	/// Frames are waiting in `out`. Lets the read loop check without taking the lock.
	out_pending: AtomicBool,
	new_streams: Mutex<Option<NewStreamSender>>,
}

impl MuxShared {
	pub fn new(params: MuxParams, new_streams: Option<NewStreamSender>) -> Self {
		Self {
			params,
			streams: Mutex::new(StreamMap::default()),
			out: Mutex::new(OutQueue {
				frames: VecDeque::new(),
				announce: Vec::new(),
				bytes: 0,
				writer: None,
				blocked: Vec::new(),
				state: OutState::Open,
			}),
			next_stream_id: AtomicU32::new(1),
			dead: AtomicBool::new(false),
			handles: AtomicUsize::new(1),
			rx_growth: AtomicU64::new(0),
			out_pending: AtomicBool::new(false),
			new_streams: Mutex::new(new_streams),
		}
	}

	pub fn is_dead(&self) -> bool {
		self.dead.load(Ordering::Acquire)
	}

	// ---- handle counting ----

	pub fn acquire_handle(&self) {
		self.handles.fetch_add(1, Ordering::Relaxed);
	}

	pub fn release_handle(&self) {
		if self.handles.fetch_sub(1, Ordering::AcqRel) == 1 {
			self.request_close(None);
		}
	}

	pub fn acquire_stream_handle(&self, stream: &StreamShared) {
		stream.handles.fetch_add(1, Ordering::Relaxed);
		self.acquire_handle();
	}

	pub fn release_stream_handle(&self, stream: &StreamShared) {
		if stream.handles.fetch_sub(1, Ordering::AcqRel) == 1 {
			let needs_close = {
				let state = stream.lock();
				state.closed.is_none() && !state.fully_finished()
			};
			if needs_close {
				let _ = self.close_stream(stream, CloseReason::Voluntary);
			}
		}
		self.release_handle();
	}

	// ---- writer queue ----

	fn push_frames(&self, frames: impl IntoIterator<Item = OutFrame>) -> Result<(), WispError> {
		let waker = {
			let mut out = lock(&self.out);
			if !matches!(out.state, OutState::Open) {
				return Err(WispError::MuxTaskEnded);
			}
			for frame in frames {
				out.bytes += frame.queued_len();
				out.frames.push_back(frame);
			}
			self.out_pending.store(true, Ordering::Relaxed);
			out.writer.take()
		};
		if let Some(waker) = waker {
			waker.wake();
		}
		Ok(())
	}

	pub fn enqueue(&self, frame: OutFrame) -> Result<(), WispError> {
		self.push_frames(std::iter::once(frame))
	}

	/// Wait until the writer queue has room for more data.
	pub fn poll_out_capacity(&self, cx: &mut Context<'_>) -> Poll<Result<(), WispError>> {
		let mut out = lock(&self.out);
		if !matches!(out.state, OutState::Open) {
			return Poll::Ready(Err(WispError::MuxTaskEnded));
		}
		if out.bytes < self.params.out_high_watermark {
			Poll::Ready(Ok(()))
		} else {
			out.blocked.push(cx.waker().clone());
			Poll::Pending
		}
	}

	/// Called by the writer: move every queued frame into `buf`.
	pub fn poll_writer(
		&self,
		cx: &mut Context<'_>,
		buf: &mut VecDeque<OutFrame>,
		blocked: &mut Vec<Waker>,
	) -> Poll<WriterEvent> {
		let mut out = lock(&self.out);
		if !out.announce.is_empty() {
			let announce = mem::take(&mut out.announce);
			drop(out);
			// announcing locks each stream, which can't happen under the queue lock
			for stream in announce {
				let mut state = stream.lock();
				if state.pending_connect.is_some() && state.closed.is_none() {
					let _ = self.push_opening(&stream, &mut state, |_| [None, None]);
				}
			}
			out = lock(&self.out);
		}
		if !out.frames.is_empty() {
			mem::swap(&mut out.frames, buf);
			out.bytes = 0;
			mem::swap(&mut out.blocked, blocked);
			self.out_pending.store(false, Ordering::Relaxed);
			return Poll::Ready(WriterEvent::Frames);
		}
		match out.state {
			OutState::Open => {
				out.writer = Some(cx.waker().clone());
				Poll::Pending
			}
			OutState::Closing(reason) => {
				out.state = OutState::Dead;
				Poll::Ready(WriterEvent::Close(reason))
			}
			OutState::Dead => Poll::Ready(WriterEvent::Dead),
		}
	}

	/// Whether frames are waiting for the writer.
	pub fn has_queued_frames(&self) -> bool {
		self.out_pending.load(Ordering::Relaxed)
	}

	/// Called by the writer: grab frames queued since the last poll without waiting.
	pub fn try_take_frames(&self, buf: &mut VecDeque<OutFrame>, blocked: &mut Vec<Waker>) -> bool {
		let mut out = lock(&self.out);
		if out.frames.is_empty() {
			false
		} else {
			mem::swap(&mut out.frames, buf);
			out.bytes = 0;
			mem::swap(&mut out.blocked, blocked);
			self.out_pending.store(false, Ordering::Relaxed);
			true
		}
	}

	/// Ask the writer to drain the queue and close the connection.
	pub fn request_close(&self, reason: Option<CloseReason>) {
		let waker = {
			let mut out = lock(&self.out);
			if matches!(out.state, OutState::Open) {
				out.state = OutState::Closing(reason);
			}
			out.writer.take()
		};
		if let Some(waker) = waker {
			waker.wake();
		}
	}

	/// Mark everything dead. Called when the driver stops, for whatever reason.
	pub fn shutdown(&self) {
		self.dead.store(true, Ordering::Release);

		let (wakers, unannounced) = {
			let mut out = lock(&self.out);
			out.state = OutState::Dead;
			out.frames.clear();
			out.bytes = 0;
			let mut wakers = mem::take(&mut out.blocked);
			wakers.extend(out.writer.take());
			(wakers, mem::take(&mut out.announce))
		};
		wakers.into_iter().for_each(Waker::wake);
		for stream in unannounced {
			let wakers = {
				let mut state = stream.lock();
				state.closed.get_or_insert(CloseReason::Unknown);
				state.take_all_wakers()
			};
			wakers.into_iter().for_each(Waker::wake);
		}

		lock(&self.new_streams).take();

		let streams: Vec<_> = lock(&self.streams).drain().map(|x| x.1).collect();
		for stream in streams {
			let wakers = {
				let mut state = stream.lock();
				stream.removed.store(true, Ordering::Relaxed);
				if state.closed.is_none() {
					state.closed = Some(CloseReason::Unknown);
				}
				self.release_growth(&mut state);
				state.take_all_wakers()
			};
			wakers.into_iter().for_each(Waker::wake);
		}
	}

	// ---- stream table ----

	fn create_stream(
		&self,
		id: u32,
		stream_type: StreamType,
		pending_connect: Option<ConnectPacket>,
	) -> Arc<StreamShared> {
		let flow = self.params.is_flow_controlled(stream_type);
		let tx_credit = if flow && self.params.tracks_credit() {
			self.params.tx_window
		} else {
			UNLIMITED
		};
		#[expect(clippy::cast_possible_wrap)]
		let rx_peer_credit = self.params.rx_window as i64;
		Arc::new(StreamShared {
			id: AtomicU32::new(id),
			stream_type,
			flow,
			handles: AtomicUsize::new(0),
			removed: AtomicBool::new(false),
			state: Mutex::new(StreamState {
				rx: VecDeque::new(),
				rx_bytes: 0,
				rx_waker: None,
				rx_eof: false,
				rx_unacked: 0,
				rx_peer_credit,
				rx_window: self.params.rx_window,
				rx_since_growth: 0,
				rx_starved: false,
				rx_grown: 0,
				tx_credit,
				tx_waker: None,
				tx_fin: false,
				closed: None,
				close_wakers: Vec::new(),
				drain_waker: None,
				pending_connect,
			}),
		})
	}

	pub fn get_stream(&self, id: u32) -> Option<Arc<StreamShared>> {
		lock(&self.streams).get(&id).cloned()
	}

	fn remove_stream(&self, id: u32) {
		lock(&self.streams).remove(&id);
	}

	/// Client: create a stream. It is announced (ID allocated, CONNECT queued) together with its
	/// first frame, see [`Self::push_opening`].
	pub fn open_stream(&self, connect: ConnectPacket) -> Result<Arc<StreamShared>, WispError> {
		if self.is_dead() {
			return Err(WispError::MuxTaskEnded);
		}
		Ok(self.create_stream(0, connect.stream_type, Some(connect)))
	}

	/// Queue a stream's frames, announcing the stream first if it hasn't been yet.
	///
	/// Client streams are announced lazily: the ID is allocated and the CONNECT queued in the
	/// same writer-queue critical section as the stream's first frame, so the CONNECT always
	/// shares a websocket message with the first data (a TLS ClientHello or HTTP request) instead
	/// of going out alone. Allocating the ID at this point also keeps CONNECTs in ID order, which
	/// WispX requires, however streams are opened. A stream dropped before it sends anything
	/// never touches the wire.
	///
	/// `frames` gets the stream ID. The caller holds the stream's state lock.
	fn push_opening(
		&self,
		stream: &Arc<StreamShared>,
		state: &mut StreamState,
		frames: impl FnOnce(u32) -> [Option<OutFrame>; 2],
	) -> Result<(), WispError> {
		let Some(connect) = state.pending_connect.take() else {
			return self.push_frames(frames(stream.id()).into_iter().flatten());
		};

		let mut streams = lock(&self.streams);
		let waker = {
			let mut out = lock(&self.out);
			if !matches!(out.state, OutState::Open) {
				state.closed.get_or_insert(CloseReason::Unknown);
				return Err(WispError::MuxTaskEnded);
			}
			// 0 marks the ID space as exhausted
			let id = self.next_stream_id.load(Ordering::Relaxed);
			if id == 0 {
				state.closed.get_or_insert(CloseReason::Unknown);
				return Err(WispError::MaxStreamCountReached);
			}
			self.next_stream_id
				.store(id.checked_add(1).unwrap_or(0), Ordering::Relaxed);
			stream.id.store(id, Ordering::Release);
			streams.insert(id, stream.clone());

			for frame in std::iter::once(OutFrame::connect(id, &connect))
				.chain(frames(id).into_iter().flatten())
			{
				out.bytes += frame.queued_len();
				out.frames.push_back(frame);
			}
			self.out_pending.store(true, Ordering::Relaxed);
			out.writer.take()
		};
		drop(streams);
		if let Some(waker) = waker {
			waker.wake();
		}
		Ok(())
	}

	/// Announce the stream if it hasn't been yet, and return its ID.
	pub fn ensure_open(&self, stream: &Arc<StreamShared>) -> u32 {
		let mut state = stream.lock();
		if state.pending_connect.is_some() {
			let _ = self.push_opening(stream, &mut state, |_| [None, None]);
		}
		stream.id()
	}

	/// Server: register a stream the client asked for. Returns false if the ID is already in use.
	pub fn accept_stream(&self, id: u32, connect: ConnectPacket) -> bool {
		let stream = {
			let mut streams = lock(&self.streams);
			if streams.contains_key(&id) {
				return false;
			}
			let stream = self.create_stream(id, connect.stream_type, None);
			streams.insert(id, stream.clone());
			stream
		};

		let sent = lock(&self.new_streams)
			.as_ref()
			.is_some_and(|tx| tx.send((connect, stream.clone())).is_ok());
		if !sent {
			let _ = self.close_stream(&stream, CloseReason::Unknown);
		}
		true
	}

	/// Take `amount` from the connection's window growth budget, if it has that much left.
	fn try_take_growth(&self, amount: u64) -> bool {
		let mut current = self.rx_growth.load(Ordering::Relaxed);
		loop {
			if current + amount > self.params.rx_max_growth {
				return false;
			}
			match self.rx_growth.compare_exchange_weak(
				current,
				current + amount,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => return true,
				Err(x) => current = x,
			}
		}
	}

	/// The stream won't receive any more data: return its window growth to the connection.
	fn release_growth(&self, state: &mut StreamState) {
		if state.rx_grown > 0 {
			self.rx_growth.fetch_sub(state.rx_grown, Ordering::Relaxed);
			state.rx_grown = 0;
		}
	}

	// ---- local stream operations ----

	/// Wait for a queued chunk. Returns `None` at end of stream.
	pub fn poll_recv(
		&self,
		stream: &Arc<StreamShared>,
		cx: &mut Context<'_>,
	) -> Poll<Option<Bytes>> {
		let mut state = stream.lock();
		if state.pending_connect.is_some() {
			// The reader is waiting for the peer, so the stream must be announced, but not
			// necessarily right now: hand it to the writer, which runs after whatever else is
			// ready. Readers usually write right after their first read (hyper polls read, then
			// write), and then the CONNECT goes out with that data.
			if state.closed.is_some() {
				return Poll::Ready(None);
			}
			match &mut state.rx_waker {
				Some(waker) => waker.clone_from(cx.waker()),
				None => state.rx_waker = Some(cx.waker().clone()),
			}
			drop(state);
			let waker = {
				let mut out = lock(&self.out);
				if !matches!(out.state, OutState::Open) {
					return Poll::Ready(None);
				}
				if !out.announce.iter().any(|x| Arc::ptr_eq(x, stream)) {
					out.announce.push(stream.clone());
				}
				out.writer.take()
			};
			if let Some(waker) = waker {
				waker.wake();
			}
			return Poll::Pending;
		}
		if let Some(chunk) = state.rx.pop_front() {
			state.rx_bytes -= chunk.len();
			let credit = self.account_consumed(stream, &mut state, chunk.len());
			let drained = if state.rx_bytes < V2_PAUSE_BYTES {
				state.drain_waker.take()
			} else {
				None
			};
			drop(state);
			if let Some(frame) = credit {
				let _ = self.enqueue(frame);
			}
			if let Some(waker) = drained {
				waker.wake();
			}
			Poll::Ready(Some(chunk))
		} else if state.rx_eof || state.closed.is_some() {
			Poll::Ready(None)
		} else {
			state.rx_starved = true;
			match &mut state.rx_waker {
				Some(waker) => waker.clone_from(cx.waker()),
				None => state.rx_waker = Some(cx.waker().clone()),
			}
			Poll::Pending
		}
	}

	/// Work out whether consuming `len` bytes means we should send the peer more credit.
	fn account_consumed(
		&self,
		stream: &StreamShared,
		state: &mut StreamState,
		len: usize,
	) -> Option<OutFrame> {
		if !stream.flow || !self.params.grants_credit() || state.closed.is_some() || state.rx_eof
		{
			return None;
		}
		match self.params.proto {
			Proto::X => {
				state.rx_unacked += len as u64;
				state.rx_since_growth += len as u64;
				// Grant in quarter-window steps. Smaller steps cost real throughput: every grant wakes
				// the sending task, which then sends only a little before blocking again.
				if state.rx_unacked >= (state.rx_window / 4).max(1) {
					let mut increment = state.rx_unacked;
					state.rx_unacked = 0;
					// Window auto-tuning, like TCP slow start: each time the reader consumes a full
					// window and had to wait for data along the way, double the window (up to the
					// max). Waiting means delivery, possibly the window, is the bottleneck; a
					// reader that always finds data is the bottleneck itself, and a bigger window
					// would only pile up unread data. Streams being read on fast, high-latency
					// links reach a large window within a few round trips; a stream nobody reads
					// never grows. The connection budget limits growth across all streams.
					if state.rx_since_growth >= state.rx_window
						&& state.rx_starved
						&& state.rx_window < self.params.rx_max_window
					{
						let extra = state.rx_window.min(self.params.rx_max_window - state.rx_window);
						if self.try_take_growth(extra) {
							state.rx_window += extra;
							state.rx_grown += extra;
							increment += extra;
						}
						state.rx_since_growth = 0;
						state.rx_starved = false;
					}
					#[expect(clippy::cast_possible_wrap)]
					{
						state.rx_peer_credit += increment as i64;
					}
					#[expect(clippy::cast_possible_truncation)]
					Some(OutFrame::credit(stream.id(), increment as u32))
				} else {
					None
				}
			}
			Proto::V1 | Proto::V2 => {
				let window = self.params.rx_window;
				state.rx_unacked += 1;
				if state.rx_unacked >= (window / 4).max(1) {
					state.rx_unacked = 0;
					let remaining = window.saturating_sub(state.rx.len() as u64);
					#[expect(clippy::cast_possible_truncation)]
					Some(OutFrame::credit(stream.id(), remaining as u32))
				} else {
					None
				}
			}
		}
	}

	/// Wait until the stream can send at least some data.
	pub fn poll_send_ready(
		&self,
		stream: &StreamShared,
		cx: &mut Context<'_>,
	) -> Poll<Result<(), WispError>> {
		{
			let mut state = stream.lock();
			if state.closed.is_some() || state.tx_fin {
				return Poll::Ready(Err(WispError::StreamAlreadyClosed));
			}
			if state.tx_credit == 0 {
				match &mut state.tx_waker {
					Some(waker) => waker.clone_from(cx.waker()),
					None => state.tx_waker = Some(cx.waker().clone()),
				}
				return Poll::Pending;
			}
		}
		self.poll_out_capacity(cx)
	}

	/// Largest payload `send_data` would accept right now. Only meaningful after
	/// `poll_send_ready` returned ready.
	pub fn send_limit(&self, stream: &StreamShared) -> usize {
		if stream.stream_type == StreamType::Udp {
			return usize::MAX;
		}
		let max = self.params.max_frame_payload;
		let state = stream.lock();
		if self.params.proto == Proto::X && state.tx_credit != UNLIMITED {
			max.min(usize::try_from(state.tx_credit).unwrap_or(usize::MAX))
		} else {
			max
		}
	}

	/// Check the stream can send and take credit for up to `len` payload bytes. Returns how many
	/// bytes may be sent now (0 if there's no credit).
	fn take_credit(
		&self,
		stream: &StreamShared,
		state: &mut StreamState,
		len: usize,
	) -> Result<usize, WispError> {
		if state.closed.is_some() || state.tx_fin {
			return Err(WispError::StreamAlreadyClosed);
		}
		if state.tx_credit == 0 {
			return Ok(0);
		}
		if stream.stream_type == StreamType::Udp {
			// datagrams can't be split
			return Ok(len);
		}
		let mut len = len.min(self.params.max_frame_payload);
		if state.tx_credit != UNLIMITED {
			if self.params.proto == Proto::X {
				len = len.min(usize::try_from(state.tx_credit).unwrap_or(usize::MAX));
				state.tx_credit -= len as u64;
			} else {
				state.tx_credit -= 1;
			}
		}
		Ok(len)
	}

	/// Queue as much of `data` as credit allows. Returns the part that was not sent.
	pub fn send_data(
		&self,
		stream: &Arc<StreamShared>,
		mut data: Bytes,
	) -> Result<Bytes, WispError> {
		if data.is_empty() {
			return Ok(data);
		}
		let mut state = stream.lock();
		let len = self.take_credit(stream, &mut state, data.len())?;
		if len == 0 {
			return Ok(data);
		}
		let chunk = data.split_to(len);
		self.push_opening(stream, &mut state, |id| {
			[Some(OutFrame::data(id, chunk)), None]
		})?;
		Ok(data)
	}

	/// Like [`Self::send_data`] for a buffer that starts with [`DATA_HEADROOM`] scratch bytes,
	/// which lets the writer send it without copying. Returns the part that was not sent.
	pub fn send_data_with_headroom(
		&self,
		stream: &Arc<StreamShared>,
		mut data: BytesMut,
	) -> Result<Bytes, WispError> {
		let payload = data.len() - DATA_HEADROOM;
		if payload == 0 {
			return Ok(Bytes::new());
		}
		let mut state = stream.lock();
		let len = self.take_credit(stream, &mut state, payload)?;
		if len == 0 {
			return Ok(data.split_off(DATA_HEADROOM).freeze());
		}
		let rest = data.split_off(DATA_HEADROOM + len).freeze();
		self.push_opening(stream, &mut state, |id| {
			[Some(OutFrame::data_with_headroom(id, data)), None]
		})?;
		Ok(rest)
	}

	/// Send `data` (with [`DATA_HEADROOM`]) and finish the stream (FIN, or CLOSE where there is
	/// no half-close) in one go, so both leave in the same write. Returns the data unsent if
	/// credit doesn't cover all of it; the caller then sends it normally and finishes after.
	pub fn send_last_with_headroom(
		&self,
		stream: &Arc<StreamShared>,
		data: BytesMut,
	) -> Result<Option<BytesMut>, WispError> {
		let half_close = self.params.proto == Proto::X && stream.stream_type != StreamType::Udp;
		let payload = data.len() - DATA_HEADROOM;
		let (wakers, remove) = {
			let mut state = stream.lock();
			if state.closed.is_some() || state.tx_fin {
				return Err(WispError::StreamAlreadyClosed);
			}
			let fits = stream.stream_type == StreamType::Udp
				|| (payload <= self.params.max_frame_payload
					&& (state.tx_credit == UNLIMITED
						|| (self.params.proto == Proto::X && state.tx_credit >= payload as u64)
						|| (self.params.proto != Proto::X && state.tx_credit >= 1)));
			if !fits {
				return Ok(Some(data));
			}
			if state.tx_credit != UNLIMITED {
				state.tx_credit -= if self.params.proto == Proto::X {
					payload as u64
				} else {
					1
				};
			}
			let last = if half_close {
				state.tx_fin = true;
				None
			} else {
				state.closed = Some(CloseReason::Voluntary);
				state.rx.clear();
				state.rx_bytes = 0;
				self.release_growth(&mut state);
				Some(CloseReason::Voluntary)
			};
			let data = (payload > 0).then_some(data);
			self.push_opening(stream, &mut state, |id| {
				[
					data.map(|x| OutFrame::data_with_headroom(id, x)),
					Some(match last {
						None => OutFrame::fin(id),
						Some(reason) => OutFrame::close(id, reason),
					}),
				]
			})?;
			let done = last.is_some() || state.rx_eof;
			let remove = done && !stream.removed.swap(true, Ordering::Relaxed);
			let wakers = if last.is_some() {
				state.take_all_wakers()
			} else {
				Vec::new()
			};
			(wakers, remove)
		};
		wakers.into_iter().for_each(Waker::wake);
		if remove {
			self.remove_stream(stream.id());
		}
		Ok(None)
	}

	/// Send a raw extension packet on this stream's ID.
	pub fn send_raw(
		&self,
		stream: &Arc<StreamShared>,
		ty: u8,
		data: Bytes,
	) -> Result<(), WispError> {
		let mut state = stream.lock();
		if state.closed.is_some() {
			return Err(WispError::StreamAlreadyClosed);
		}
		self.push_opening(stream, &mut state, |id| [Some(OutFrame::raw(ty, id, data)), None])
	}

	/// Abort the stream in both directions.
	pub fn close_stream(&self, stream: &StreamShared, reason: CloseReason) -> Result<(), WispError> {
		let (wakers, was_removed) = {
			let mut state = stream.lock();
			if state.closed.is_some() || state.fully_finished() {
				return Err(WispError::StreamAlreadyClosed);
			}
			if state.pending_connect.take().is_some() {
				// never announced, so there's nothing to tell the peer
				state.closed = Some(reason);
				let wakers = state.take_all_wakers();
				drop(state);
				wakers.into_iter().for_each(Waker::wake);
				return Ok(());
			}
			state.closed = Some(reason);
			state.rx.clear();
			state.rx_bytes = 0;
			self.release_growth(&mut state);
			let was_removed = stream.removed.swap(true, Ordering::Relaxed);
			(state.take_all_wakers(), was_removed)
		};
		wakers.into_iter().for_each(Waker::wake);
		if !was_removed {
			self.remove_stream(stream.id());
		}
		self.enqueue(OutFrame::close(stream.id(), reason))
	}

	/// Half-close: we will send no more data. Falls back to a full close where the protocol has
	/// no half-close (v1/v2, and UDP streams).
	pub fn finish_stream(&self, stream: &Arc<StreamShared>) -> Result<(), WispError> {
		if self.params.proto != Proto::X || stream.stream_type == StreamType::Udp {
			return match self.close_stream(stream, CloseReason::Voluntary) {
				Err(WispError::StreamAlreadyClosed) => Ok(()),
				x => x,
			};
		}
		let done = {
			let mut state = stream.lock();
			if state.closed.is_some() {
				return Err(WispError::StreamAlreadyClosed);
			}
			if state.tx_fin {
				return Ok(());
			}
			state.tx_fin = true;
			self.push_opening(stream, &mut state, |id| [Some(OutFrame::fin(id)), None])?;
			let done = state.rx_eof && !stream.removed.load(Ordering::Relaxed);
			if done {
				stream.removed.store(true, Ordering::Relaxed);
			}
			done
		};
		if done {
			self.remove_stream(stream.id());
		}
		Ok(())
	}

	pub fn close_reason(stream: &StreamShared) -> Option<CloseReason> {
		let state = stream.lock();
		state
			.closed
			.or_else(|| state.fully_finished().then_some(CloseReason::Voluntary))
	}

	/// Resolves once the stream is aborted (CLOSE sent or received, or the multiplexor died).
	pub fn poll_closed(stream: &StreamShared, cx: &mut Context<'_>) -> Poll<CloseReason> {
		let mut state = stream.lock();
		if let Some(reason) = state.closed {
			Poll::Ready(reason)
		} else {
			if !state.close_wakers.iter().any(|x| x.will_wake(cx.waker())) {
				state.close_wakers.push(cx.waker().clone());
			}
			Poll::Pending
		}
	}

	// ---- events from the peer ----

	/// DATA arrived.
	///
	/// A WispX peer that overruns its credit gets the stream closed with
	/// [`CloseReason::ProtocolViolation`]. For v1/v2, returns the stream if the read loop should
	/// wait for it to drain (see [`V2_PAUSE_BYTES`]). Data is never dropped from an open stream.
	///
	/// `cache` holds the stream of the previous DATA frame: consecutive frames usually belong to
	/// the same stream, and this skips the stream map lock.
	pub fn on_data(
		&self,
		id: u32,
		data: Bytes,
		cache: &mut Option<Arc<StreamShared>>,
	) -> Option<Arc<StreamShared>> {
		let stream = match cache {
			Some(stream) if stream.id() == id && !stream.removed.load(Ordering::Relaxed) => {
				stream.clone()
			}
			_ => {
				let stream = self.get_stream(id)?;
				*cache = Some(stream.clone());
				stream
			}
		};
		let (waker, pause) = {
			let mut state = stream.lock();
			if state.closed.is_some() || state.rx_eof {
				return None;
			}
			if stream.flow && self.params.grants_credit() && self.params.proto == Proto::X {
				state.rx_peer_credit -= i64::try_from(data.len()).unwrap_or(i64::MAX);
				if state.rx_peer_credit < 0 {
					drop(state);
					let _ = self.close_stream(&stream, CloseReason::ProtocolViolation);
					return None;
				}
			}
			state.rx_bytes += data.len();
			state.rx.push_back(data);
			let pause = self.params.proto != Proto::X
				&& self.params.role == Role::Server
				&& state.rx_bytes >= V2_PAUSE_BYTES;
			(state.rx_waker.take(), pause)
		};
		if let Some(waker) = waker {
			waker.wake();
		}
		pause.then_some(stream)
	}

	/// Resolves once `stream` has drained below [`V2_PAUSE_BYTES`] or closed.
	pub fn poll_drained(stream: &StreamShared, cx: &mut Context<'_>) -> Poll<()> {
		let mut state = stream.lock();
		if state.rx_bytes < V2_PAUSE_BYTES || state.closed.is_some() {
			Poll::Ready(())
		} else {
			state.drain_waker = Some(cx.waker().clone());
			Poll::Pending
		}
	}

	/// CREDIT (WispX, increment) or CONTINUE (v1/v2, absolute) arrived.
	pub fn on_credit(&self, id: u32, value: u32) {
		let Some(stream) = self.get_stream(id) else {
			return;
		};
		let waker = {
			let mut state = stream.lock();
			if state.tx_credit == UNLIMITED || !self.params.tracks_credit() {
				return;
			}
			match self.params.proto {
				Proto::X => state.tx_credit = state.tx_credit.saturating_add(value.into()),
				Proto::V1 | Proto::V2 => state.tx_credit = value.into(),
			}
			if state.tx_credit > 0 {
				state.tx_waker.take()
			} else {
				None
			}
		};
		if let Some(waker) = waker {
			waker.wake();
		}
	}

	pub fn on_close(&self, id: u32, reason: CloseReason) {
		let Some(stream) = lock(&self.streams).remove(&id) else {
			return;
		};
		let wakers = {
			let mut state = stream.lock();
			stream.removed.store(true, Ordering::Relaxed);
			if state.closed.is_some() {
				return;
			}
			// keep already-received data readable
			state.closed = Some(reason);
			self.release_growth(&mut state);
			state.take_all_wakers()
		};
		wakers.into_iter().for_each(Waker::wake);
	}

	pub fn on_fin(&self, id: u32) {
		let Some(stream) = self.get_stream(id) else {
			return;
		};
		let (waker, done) = {
			let mut state = stream.lock();
			if state.closed.is_some() || state.rx_eof {
				return;
			}
			state.rx_eof = true;
			self.release_growth(&mut state);
			let done = state.tx_fin && !stream.removed.load(Ordering::Relaxed);
			if done {
				stream.removed.store(true, Ordering::Relaxed);
			}
			(state.rx_waker.take(), done)
		};
		if done {
			self.remove_stream(id);
		}
		if let Some(waker) = waker {
			waker.wake();
		}
	}
}
