//! State shared between the multiplexor driver (reader + writer) and stream handles.
//!
//! Lock order is `streams` -> stream `state` -> `out`. No lock is held while waking a task.

use std::{
	collections::VecDeque,
	mem,
	sync::{
		atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
		Arc, Mutex, MutexGuard,
	},
	task::{Context, Poll, Waker},
};

use bytes::Bytes;
use rustc_hash::FxHashMap;

use crate::{
	frame::{OutFrame, Proto},
	packet::{CloseReason, ConnectPacket, StreamType},
	Role, WispError,
};

/// Credit value meaning "not flow controlled".
pub(crate) const UNLIMITED: u64 = u64::MAX;

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
	/// WispX: bytes (our stream window). v1/v2 server: packets (our buffer size).
	pub rx_window: u64,
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

	/// Whether the peer gives us flow control credit for streams we send on.
	fn tracks_credit(&self) -> bool {
		self.proto == Proto::X || self.role == Role::Client
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

	pub tx_credit: u64,
	tx_waker: Option<Waker>,
	/// We sent FIN.
	pub tx_fin: bool,

	pub closed: Option<CloseReason>,
	close_wakers: Vec<Waker>,
	/// Removed from the stream map.
	removed: bool,
}

impl StreamState {
	fn fully_finished(&self) -> bool {
		self.tx_fin && self.rx_eof
	}

	fn take_all_wakers(&mut self) -> Vec<Waker> {
		let mut wakers = mem::take(&mut self.close_wakers);
		wakers.extend(self.rx_waker.take());
		wakers.extend(self.tx_waker.take());
		wakers
	}
}

pub(crate) struct StreamShared {
	pub id: u32,
	pub stream_type: StreamType,
	pub flow: bool,
	/// Number of live user-facing handles. When it hits zero the stream is closed.
	handles: AtomicUsize,
	state: Mutex<StreamState>,
}

impl StreamShared {
	pub fn lock(&self) -> MutexGuard<'_, StreamState> {
		lock(&self.state)
	}
}

enum OutState {
	Open,
	Closing(Option<CloseReason>),
	Dead,
}

struct OutQueue {
	frames: VecDeque<OutFrame>,
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

pub(crate) struct MuxShared {
	pub params: MuxParams,
	streams: Mutex<StreamMap>,
	out: Mutex<OutQueue>,
	next_stream_id: AtomicU32,
	dead: AtomicBool,
	/// Multiplexor handle plus all stream handles. When it hits zero the connection is closed.
	handles: AtomicUsize,
	new_streams: Mutex<Option<flume::Sender<(ConnectPacket, Arc<StreamShared>)>>>,
}

impl MuxShared {
	pub fn new(
		params: MuxParams,
		new_streams: Option<flume::Sender<(ConnectPacket, Arc<StreamShared>)>>,
	) -> Self {
		Self {
			params,
			streams: Mutex::new(StreamMap::default()),
			out: Mutex::new(OutQueue {
				frames: VecDeque::new(),
				bytes: 0,
				writer: None,
				blocked: Vec::new(),
				state: OutState::Open,
			}),
			next_stream_id: AtomicU32::new(1),
			dead: AtomicBool::new(false),
			handles: AtomicUsize::new(1),
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
		if !out.frames.is_empty() {
			mem::swap(&mut out.frames, buf);
			out.bytes = 0;
			mem::swap(&mut out.blocked, blocked);
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

	/// Called by the writer: grab frames queued since the last poll without waiting.
	pub fn try_take_frames(&self, buf: &mut VecDeque<OutFrame>, blocked: &mut Vec<Waker>) -> bool {
		let mut out = lock(&self.out);
		if out.frames.is_empty() {
			false
		} else {
			mem::swap(&mut out.frames, buf);
			out.bytes = 0;
			mem::swap(&mut out.blocked, blocked);
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

		let wakers = {
			let mut out = lock(&self.out);
			out.state = OutState::Dead;
			out.frames.clear();
			out.bytes = 0;
			let mut wakers = mem::take(&mut out.blocked);
			wakers.extend(out.writer.take());
			wakers
		};
		wakers.into_iter().for_each(Waker::wake);

		lock(&self.new_streams).take();

		let streams: Vec<_> = lock(&self.streams).drain().map(|x| x.1).collect();
		for stream in streams {
			let wakers = {
				let mut state = stream.lock();
				state.removed = true;
				if state.closed.is_none() {
					state.closed = Some(CloseReason::Unknown);
				}
				state.take_all_wakers()
			};
			wakers.into_iter().for_each(Waker::wake);
		}
	}

	// ---- stream table ----

	fn create_stream(&self, id: u32, stream_type: StreamType) -> Arc<StreamShared> {
		let flow = self.params.is_flow_controlled(stream_type);
		let tx_credit = if flow && self.params.tracks_credit() {
			self.params.tx_window
		} else {
			UNLIMITED
		};
		#[expect(clippy::cast_possible_wrap)]
		let rx_peer_credit = self.params.rx_window as i64;
		Arc::new(StreamShared {
			id,
			stream_type,
			flow,
			handles: AtomicUsize::new(0),
			state: Mutex::new(StreamState {
				rx: VecDeque::new(),
				rx_bytes: 0,
				rx_waker: None,
				rx_eof: false,
				rx_unacked: 0,
				rx_peer_credit,
				tx_credit,
				tx_waker: None,
				tx_fin: false,
				closed: None,
				close_wakers: Vec::new(),
				removed: false,
			}),
		})
	}

	pub fn get_stream(&self, id: u32) -> Option<Arc<StreamShared>> {
		lock(&self.streams).get(&id).cloned()
	}

	fn remove_stream(&self, id: u32) {
		lock(&self.streams).remove(&id);
	}

	/// Client: allocate an ID, register a stream and queue its CONNECT.
	pub fn open_stream(&self, connect: &ConnectPacket) -> Result<Arc<StreamShared>, WispError> {
		if self.is_dead() {
			return Err(WispError::MuxTaskEnded);
		}
		let id = self
			.next_stream_id
			.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| x.checked_add(1))
			.map_err(|_| WispError::MaxStreamCountReached)?;

		let stream = self.create_stream(id, connect.stream_type);
		lock(&self.streams).insert(id, stream.clone());
		if let Err(err) = self.enqueue(OutFrame::connect(id, connect)) {
			self.remove_stream(id);
			return Err(err);
		}
		Ok(stream)
	}

	/// Server: register a stream the client asked for. Returns false if the ID is already in use.
	pub fn accept_stream(&self, id: u32, connect: ConnectPacket) -> bool {
		let stream = {
			let mut streams = lock(&self.streams);
			if streams.contains_key(&id) {
				return false;
			}
			let stream = self.create_stream(id, connect.stream_type);
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

	// ---- local stream operations ----

	/// Wait for a queued chunk. Returns `None` at end of stream.
	pub fn poll_recv(&self, stream: &StreamShared, cx: &mut Context<'_>) -> Poll<Option<Bytes>> {
		let mut state = stream.lock();
		if let Some(chunk) = state.rx.pop_front() {
			state.rx_bytes -= chunk.len();
			let credit = self.account_consumed(stream, &mut state, chunk.len());
			drop(state);
			if let Some(frame) = credit {
				let _ = self.enqueue(frame);
			}
			Poll::Ready(Some(chunk))
		} else if state.rx_eof || state.closed.is_some() {
			Poll::Ready(None)
		} else {
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
		let window = self.params.rx_window;
		match self.params.proto {
			Proto::X => {
				state.rx_unacked += len as u64;
				if state.rx_unacked >= (window / 4).max(1) {
					let increment = state.rx_unacked;
					state.rx_unacked = 0;
					#[expect(clippy::cast_possible_wrap)]
					{
						state.rx_peer_credit += increment as i64;
					}
					#[expect(clippy::cast_possible_truncation)]
					Some(OutFrame::credit(stream.id, increment as u32))
				} else {
					None
				}
			}
			Proto::V1 | Proto::V2 => {
				state.rx_unacked += 1;
				if state.rx_unacked >= (window / 4).max(1) {
					state.rx_unacked = 0;
					let remaining = window.saturating_sub(state.rx.len() as u64);
					#[expect(clippy::cast_possible_truncation)]
					Some(OutFrame::credit(stream.id, remaining as u32))
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

	/// Queue as much of `data` as credit allows. Returns the part that was not sent.
	pub fn send_data(&self, stream: &StreamShared, mut data: Bytes) -> Result<Bytes, WispError> {
		if data.is_empty() {
			return Ok(data);
		}
		let frame = {
			let mut state = stream.lock();
			if state.closed.is_some() || state.tx_fin {
				return Err(WispError::StreamAlreadyClosed);
			}
			if state.tx_credit == 0 {
				return Ok(data);
			}
			if stream.stream_type == StreamType::Udp {
				// datagrams can't be split
				mem::take(&mut data)
			} else {
				let mut len = data.len().min(self.params.max_frame_payload);
				if state.tx_credit != UNLIMITED {
					if self.params.proto == Proto::X {
						len = len.min(usize::try_from(state.tx_credit).unwrap_or(usize::MAX));
						state.tx_credit -= len as u64;
					} else {
						state.tx_credit -= 1;
					}
				}
				data.split_to(len)
			}
		};
		self.enqueue(OutFrame::data(stream.id, frame))?;
		Ok(data)
	}

	/// Send a raw extension packet on this stream's ID.
	pub fn send_raw(&self, stream: &StreamShared, ty: u8, data: Bytes) -> Result<(), WispError> {
		if stream.lock().closed.is_some() {
			return Err(WispError::StreamAlreadyClosed);
		}
		self.enqueue(OutFrame::raw(ty, stream.id, data))
	}

	/// Abort the stream in both directions.
	pub fn close_stream(&self, stream: &StreamShared, reason: CloseReason) -> Result<(), WispError> {
		let (wakers, was_removed) = {
			let mut state = stream.lock();
			if state.closed.is_some() || state.fully_finished() {
				return Err(WispError::StreamAlreadyClosed);
			}
			state.closed = Some(reason);
			state.rx.clear();
			state.rx_bytes = 0;
			let was_removed = mem::replace(&mut state.removed, true);
			(state.take_all_wakers(), was_removed)
		};
		wakers.into_iter().for_each(Waker::wake);
		if !was_removed {
			self.remove_stream(stream.id);
		}
		self.enqueue(OutFrame::close(stream.id, reason))
	}

	/// Half-close: we will send no more data. Falls back to a full close where the protocol has
	/// no half-close (v1/v2, and UDP streams).
	pub fn finish_stream(&self, stream: &StreamShared) -> Result<(), WispError> {
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
			let done = state.rx_eof && !state.removed;
			if done {
				state.removed = true;
			}
			done
		};
		if done {
			self.remove_stream(stream.id);
		}
		self.enqueue(OutFrame::fin(stream.id))
	}

	pub fn close_reason(&self, stream: &StreamShared) -> Option<CloseReason> {
		let state = stream.lock();
		state
			.closed
			.or_else(|| state.fully_finished().then_some(CloseReason::Voluntary))
	}

	/// Resolves once the stream is aborted (CLOSE sent or received, or the multiplexor died).
	pub fn poll_closed(&self, stream: &StreamShared, cx: &mut Context<'_>) -> Poll<CloseReason> {
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

	/// DATA arrived. A peer that overruns its flow control credit gets the stream closed with
	/// [`CloseReason::ProtocolViolation`]; data is never silently dropped from an open stream.
	pub fn on_data(&self, id: u32, data: Bytes) {
		let Some(stream) = self.get_stream(id) else {
			return;
		};
		let waker = {
			let mut state = stream.lock();
			if state.closed.is_some() || state.rx_eof {
				return;
			}
			let violation = stream.flow
				&& self.params.grants_credit()
				&& match self.params.proto {
					Proto::X => {
						state.rx_peer_credit -= data.len() as i64;
						state.rx_peer_credit < 0
					}
					// v1/v2 CONTINUE values are absolute and race with packets in flight, so
					// clients get generous slack before this counts as abuse
					Proto::V1 | Proto::V2 => {
						state.rx.len() as u64 >= (self.params.rx_window * 4).max(1024)
					}
				};
			if violation {
				None
			} else {
				state.rx_bytes += data.len();
				state.rx.push_back(data);
				Some(state.rx_waker.take())
			}
		};
		match waker {
			Some(Some(waker)) => waker.wake(),
			Some(None) => {}
			None => {
				let _ = self.close_stream(&stream, CloseReason::ProtocolViolation);
			}
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
			state.removed = true;
			if state.closed.is_some() {
				return;
			}
			// keep already-received data readable
			state.closed = Some(reason);
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
			let done = state.tx_fin && !state.removed;
			if done {
				state.removed = true;
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
