//! How each backend reads with connection credit.

use bytes::Bytes;
use moq_net::transport::Unheld;

/// Read a plain chunk, whose connection credit the backend returned as it read.
pub(crate) async fn read_unheld<S: web_transport_trait::RecvStream>(
	stream: &mut S,
	max: usize,
) -> Result<Option<(Bytes, Unheld)>, S::Error> {
	let chunk = stream.read_chunk(max).await?;
	Ok(chunk.map(|bytes| {
		let len = bytes.len() as u64;
		(bytes, Unheld(len))
	}))
}

/// Give a backend that returns connection credit as it reads, so never fills its window with
/// held credit, the held interface.
macro_rules! unheld {
	($session:ty, $recv:ty) => {
		impl $crate::transport::HeldRecvStream for $recv {
			type Credit = moq_net::transport::Unheld;

			fn read_chunk_held(
				&mut self,
				max: usize,
			) -> impl Future<Output = Result<Option<(bytes::Bytes, Self::Credit)>, Self::Error>> + Send {
				$crate::transport::held::read_unheld(self, max)
			}
		}

		impl $crate::transport::HeldSession for $session {
			fn poll_credit_exhausted(&self, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
				std::task::Poll::Pending
			}
		}
	};
}
#[cfg(test)]
pub(crate) use unheld;

#[cfg(any(feature = "tcp", feature = "websocket"))]
unheld!(qmux::Session, qmux::RecvStream);
#[cfg(feature = "iroh")]
unheld!(web_transport_iroh::Session, web_transport_iroh::RecvStream);

#[cfg(feature = "quic")]
impl moq_net::transport::Credit for crate::quic::driver::Credit {
	fn len(&self) -> u64 {
		self.len()
	}

	fn split(&mut self, bytes: u64) -> Self {
		self.split(bytes)
	}

	fn merge(&mut self, other: Self) {
		self.merge(other)
	}
}

#[cfg(feature = "quic")]
impl super::HeldRecvStream for crate::quic::web::RecvStream {
	type Credit = crate::quic::driver::Credit;

	fn read_chunk_held(
		&mut self,
		max: usize,
	) -> impl Future<Output = Result<Option<(Bytes, Self::Credit)>, Self::Error>> + Send {
		crate::quic::web::RecvStream::read_chunk_held(self, max)
	}
}

#[cfg(feature = "quic")]
impl super::HeldSession for crate::quic::web::Session {
	fn poll_credit_exhausted(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
		crate::quic::driver::Connection::poll_credit_exhausted(self, cx)
	}
}
