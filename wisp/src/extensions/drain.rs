//! "Drain on close" protocol extension (unofficial, Wisp v2 only).
//!
//! The original wisp-mux client throws away data it has received but not yet read when a CLOSE
//! arrives, so servers delay CLOSE after recent data to avoid truncating responses. A client
//! sending this extension delivers everything it received before a CLOSE, so the server can
//! close right away. WispX doesn't need it: it has FIN, and CLOSE keeps received data by spec.
use async_trait::async_trait;
use bytes::Bytes;

use crate::{Role, WispError};

use super::{AnyProtocolExtension, ProtocolExtension, ProtocolExtensionBuilder};

/// "Drain on close" protocol extension.
#[derive(Debug, Clone, Copy)]
pub struct DrainOnCloseProtocolExtension;

impl DrainOnCloseProtocolExtension {
	/// Extension ID.
	pub const ID: u8 = 0xFA;
}

#[async_trait]
impl ProtocolExtension for DrainOnCloseProtocolExtension {
	fn get_id(&self) -> u8 {
		Self::ID
	}

	fn encode(&self) -> Bytes {
		Bytes::new()
	}

	fn box_clone(&self) -> Box<dyn ProtocolExtension + Sync + Send> {
		Box::new(*self)
	}
}

/// "Drain on close" protocol extension builder.
pub struct DrainOnCloseProtocolExtensionBuilder;

impl ProtocolExtensionBuilder for DrainOnCloseProtocolExtensionBuilder {
	fn get_id(&self) -> u8 {
		DrainOnCloseProtocolExtension::ID
	}

	fn build_from_bytes(&mut self, _: Bytes, _: Role) -> Result<AnyProtocolExtension, WispError> {
		Ok(DrainOnCloseProtocolExtension.into())
	}

	fn build_to_extension(&mut self, _: Role) -> Result<AnyProtocolExtension, WispError> {
		Ok(DrainOnCloseProtocolExtension.into())
	}
}
