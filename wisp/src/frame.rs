//! Wire encoding shared by the multiplexor core.
//!
//! Wisp v1/v2 put exactly one packet in each websocket message (5 byte header). WispX puts one
//! or more length-prefixed frames in each message (9 byte header) so queued frames can be
//! coalesced into a single message.

use std::collections::VecDeque;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
	packet::{CloseReason, ConnectPacket},
	WispError,
};

pub(crate) const TY_CONNECT: u8 = 0x01;
pub(crate) const TY_DATA: u8 = 0x02;
/// CONTINUE in v1/v2, CREDIT in WispX.
pub(crate) const TY_CONTINUE: u8 = 0x03;
pub(crate) const TY_CLOSE: u8 = 0x04;
/// INFO in v2, HELLO in WispX.
pub(crate) const TY_INFO: u8 = 0x05;
/// WispX only.
pub(crate) const TY_FIN: u8 = 0x06;

pub(crate) const V2_HEADER_LEN: usize = 5;
pub(crate) const X_HEADER_LEN: usize = 9;

/// Every WispX peer must accept messages of at least this size.
pub const WISPX_MIN_MESSAGE_SIZE: u32 = 64 * 1024;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Proto {
	V1,
	V2,
	X,
}

impl Proto {
	pub fn header_len(self) -> usize {
		match self {
			Self::V1 | Self::V2 => V2_HEADER_LEN,
			Self::X => X_HEADER_LEN,
		}
	}
}

#[derive(Debug)]
pub(crate) enum OutPayload {
	Bytes(Bytes),
	U32(u32),
	U8(u8),
	Empty,
}

/// A frame queued for the writer.
#[derive(Debug)]
pub(crate) struct OutFrame {
	pub ty: u8,
	pub id: u32,
	pub payload: OutPayload,
}

impl OutFrame {
	pub fn data(id: u32, data: Bytes) -> Self {
		Self {
			ty: TY_DATA,
			id,
			payload: OutPayload::Bytes(data),
		}
	}

	/// CONTINUE (absolute packet count) in v1/v2, CREDIT (byte increment) in WispX.
	pub fn credit(id: u32, value: u32) -> Self {
		Self {
			ty: TY_CONTINUE,
			id,
			payload: OutPayload::U32(value),
		}
	}

	pub fn close(id: u32, reason: CloseReason) -> Self {
		Self {
			ty: TY_CLOSE,
			id,
			payload: OutPayload::U8(reason.into()),
		}
	}

	pub fn fin(id: u32) -> Self {
		Self {
			ty: TY_FIN,
			id,
			payload: OutPayload::Empty,
		}
	}

	pub fn connect(id: u32, packet: &ConnectPacket) -> Self {
		let mut buf = BytesMut::with_capacity(3 + packet.host.len());
		buf.put_u8(packet.stream_type.into());
		buf.put_u16_le(packet.port);
		buf.extend_from_slice(packet.host.as_bytes());
		Self {
			ty: TY_CONNECT,
			id,
			payload: OutPayload::Bytes(buf.freeze()),
		}
	}

	pub fn raw(ty: u8, id: u32, payload: Bytes) -> Self {
		Self {
			ty,
			id,
			payload: OutPayload::Bytes(payload),
		}
	}

	pub fn payload_len(&self) -> usize {
		match &self.payload {
			OutPayload::Bytes(x) => x.len(),
			OutPayload::U32(_) => 4,
			OutPayload::U8(_) => 1,
			OutPayload::Empty => 0,
		}
	}

	/// Bytes this frame takes up in the writer queue, used for backpressure.
	pub fn queued_len(&self) -> usize {
		X_HEADER_LEN + self.payload_len()
	}

	fn encode_into(&self, proto: Proto, buf: &mut BytesMut) {
		buf.put_u8(self.ty);
		buf.put_u32_le(self.id);
		if proto == Proto::X {
			#[expect(clippy::cast_possible_truncation)]
			buf.put_u32_le(self.payload_len() as u32);
		}
		match &self.payload {
			OutPayload::Bytes(x) => buf.extend_from_slice(x),
			OutPayload::U32(x) => buf.put_u32_le(*x),
			OutPayload::U8(x) => buf.put_u8(*x),
			OutPayload::Empty => {}
		}
	}

	/// Encode this frame as a standalone websocket message.
	pub fn encode(&self, proto: Proto) -> Bytes {
		let mut buf = BytesMut::with_capacity(proto.header_len() + self.payload_len());
		self.encode_into(proto, &mut buf);
		buf.freeze()
	}
}

/// Encode queued frames into websocket messages, appending them to `out`.
///
/// WispX coalesces frames into messages of up to `batch_size` bytes. A single frame larger than
/// `batch_size` is sent alone. v1/v2 produce one message per frame, but every message is split
/// out of a single allocation.
pub(crate) fn encode_batch(
	proto: Proto,
	frames: &mut VecDeque<OutFrame>,
	batch_size: usize,
	out: &mut Vec<Bytes>,
) {
	let header = proto.header_len();
	let total: usize = frames.iter().map(|x| header + x.payload_len()).sum();
	let mut buf = BytesMut::with_capacity(total);

	match proto {
		Proto::V1 | Proto::V2 => {
			for frame in frames.drain(..) {
				frame.encode_into(proto, &mut buf);
				out.push(buf.split().freeze());
			}
		}
		Proto::X => {
			for frame in frames.drain(..) {
				let len = header + frame.payload_len();
				if !buf.is_empty() && buf.len() + len > batch_size {
					out.push(buf.split().freeze());
				}
				frame.encode_into(proto, &mut buf);
			}
			if !buf.is_empty() {
				out.push(buf.freeze());
			}
		}
	}
}

/// A decoded incoming frame. The payload is a zero-copy slice of the websocket message.
#[derive(Debug)]
pub(crate) struct InFrame {
	pub ty: u8,
	pub id: u32,
	pub payload: Bytes,
}

/// Decode one websocket message into frames.
pub(crate) fn decode_message(
	proto: Proto,
	mut msg: Bytes,
	out: &mut Vec<InFrame>,
) -> Result<(), WispError> {
	match proto {
		Proto::V1 | Proto::V2 => {
			if msg.len() < V2_HEADER_LEN {
				return Err(WispError::PacketTooSmall);
			}
			let ty = msg.get_u8();
			let id = msg.get_u32_le();
			out.push(InFrame {
				ty,
				id,
				payload: msg,
			});
		}
		Proto::X => {
			while !msg.is_empty() {
				if msg.len() < X_HEADER_LEN {
					return Err(WispError::PacketTooSmall);
				}
				let ty = msg.get_u8();
				let id = msg.get_u32_le();
				let len = msg.get_u32_le() as usize;
				if msg.len() < len {
					return Err(WispError::PacketTooSmall);
				}
				out.push(InFrame {
					ty,
					id,
					payload: msg.split_to(len),
				});
			}
		}
	}
	Ok(())
}

/// What the first message from a server says about its protocol version.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ServerFirstMessage {
	V1,
	V2,
	X,
}

/// Classify the first message a server sends, so a WispX client can fall back to v1/v2.
pub(crate) fn classify_server_first_message(msg: &[u8]) -> ServerFirstMessage {
	// WispX HELLO: [0x05][id 0][len = rest][major 3]..., and it is alone in its message.
	if msg.len() >= X_HEADER_LEN + 10
		&& msg[0] == TY_INFO
		&& u32::from_le_bytes([msg[5], msg[6], msg[7], msg[8]]) as usize == msg.len() - X_HEADER_LEN
		&& msg[X_HEADER_LEN] == 3
	{
		ServerFirstMessage::X
	} else if msg.first() == Some(&TY_INFO) {
		ServerFirstMessage::V2
	} else {
		ServerFirstMessage::V1
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn x_batch_roundtrip() {
		let mut frames: VecDeque<_> = vec![
			OutFrame::data(1, Bytes::from_static(b"hello")),
			OutFrame::credit(2, 1234),
			OutFrame::close(3, CloseReason::Voluntary),
			OutFrame::fin(4),
			OutFrame::data(5, Bytes::new()),
		]
		.into();
		let mut out = Vec::new();
		encode_batch(Proto::X, &mut frames, 64 * 1024, &mut out);
		assert_eq!(out.len(), 1);

		let mut decoded = Vec::new();
		decode_message(Proto::X, out.pop().unwrap(), &mut decoded).unwrap();
		assert_eq!(decoded.len(), 5);
		assert_eq!(decoded[0].ty, TY_DATA);
		assert_eq!(&decoded[0].payload[..], b"hello");
		assert_eq!(decoded[1].payload.clone().get_u32_le(), 1234);
		assert_eq!(decoded[2].payload[0], 0x02);
		assert_eq!(decoded[3].ty, TY_FIN);
		assert!(decoded[3].payload.is_empty());
		assert_eq!(decoded[4].id, 5);
	}

	#[test]
	fn x_batch_respects_cap() {
		let mut frames: VecDeque<_> = (0..10)
			.map(|i| OutFrame::data(i, Bytes::from(vec![0u8; 100])))
			.collect();
		let mut out = Vec::new();
		encode_batch(Proto::X, &mut frames, 3 * 109, &mut out);
		assert_eq!(out.len(), 4);
		assert!(out.iter().all(|x| x.len() <= 3 * 109));
		let mut decoded = Vec::new();
		for msg in out {
			decode_message(Proto::X, msg, &mut decoded).unwrap();
		}
		assert_eq!(decoded.len(), 10);
		assert!(decoded.iter().enumerate().all(|(i, x)| x.id == i as u32));
	}

	#[test]
	fn v2_one_message_per_frame() {
		let mut frames: VecDeque<_> = vec![
			OutFrame::data(1, Bytes::from_static(b"abc")),
			OutFrame::credit(1, 7),
		]
		.into();
		let mut out = Vec::new();
		encode_batch(Proto::V2, &mut frames, 64 * 1024, &mut out);
		assert_eq!(out.len(), 2);
		assert_eq!(&out[0][..], &[0x02, 1, 0, 0, 0, b'a', b'b', b'c']);
		assert_eq!(&out[1][..], &[0x03, 1, 0, 0, 0, 7, 0, 0, 0]);
	}

	#[test]
	fn x_truncated_is_error() {
		let mut out = Vec::new();
		let msg = Bytes::from_static(&[0x02, 1, 0, 0, 0, 10, 0, 0, 0, 1, 2]);
		assert!(decode_message(Proto::X, msg, &mut out).is_err());
		let msg = Bytes::from_static(&[0x02, 1, 0]);
		assert!(decode_message(Proto::X, msg, &mut out).is_err());
	}

	#[test]
	fn classify() {
		// v1 CONTINUE
		assert_eq!(
			classify_server_first_message(&[0x03, 0, 0, 0, 0, 128, 0, 0, 0]),
			ServerFirstMessage::V1
		);
		// v2 INFO with udp extension
		assert_eq!(
			classify_server_first_message(&[0x05, 0, 0, 0, 0, 2, 0, 0x01, 0, 0, 0, 0]),
			ServerFirstMessage::V2
		);
		// WispX HELLO, no extensions
		let mut hello = vec![0x05, 0, 0, 0, 0, 10, 0, 0, 0, 3, 0];
		hello.extend_from_slice(&65536u32.to_le_bytes());
		hello.extend_from_slice(&65536u32.to_le_bytes());
		assert_eq!(classify_server_first_message(&hello), ServerFirstMessage::X);
	}
}
