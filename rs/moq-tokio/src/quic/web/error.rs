use std::sync::{Arc, OnceLock};

use thiserror::Error;

use crate::{ConnectError, SettingsError};

/// An error returned when connecting to a WebTransport endpoint.
#[derive(Error, Debug, Clone)]
pub enum ClientError {
    #[error("unexpected end of stream")]
    UnexpectedEnd,

    #[error("connection error: {0}")]
    Connection(#[from] noq::ConnectionError),

    #[error("failed to write: {0}")]
    WriteError(#[from] noq::WriteError),

    #[error("failed to read: {0}")]
    ReadError(#[from] noq::ReadError),

    #[error("failed to exchange h3 settings: {0}")]
    SettingsError(#[from] SettingsError),

    #[error("failed to exchange h3 connect: {0}")]
    HttpError(#[from] ConnectError),

    #[error("quic error: {0}")]
    NoqError(#[from] noq::ConnectError),

    #[error("invalid DNS name: {0}")]
    InvalidDnsName(String),

    #[cfg(any(feature = "aws-lc-rs", feature = "ring"))]
    #[error("rustls error: {0}")]
    Rustls(#[from] rustls::Error),
}

/// An errors returned by [`crate::Session`], split based on if they are underlying QUIC errors or
/// WebTransport errors.
#[derive(Clone, Error, Debug)]
pub enum SessionError {
    #[error("connection error: {0}")]
    ConnectionError(noq::ConnectionError),

    #[error("webtransport error: {0}")]
    WebTransportError(#[from] WebTransportError),

    #[error("send datagram error: {0}")]
    SendDatagramError(#[from] noq::SendDatagramError),
}

impl From<noq::ConnectionError> for SessionError {
    fn from(e: noq::ConnectionError) -> Self {
        match &e {
            noq::ConnectionError::ApplicationClosed(close) => {
                match web_transport_proto::error_from_http3(close.error_code.into_inner()) {
                    Some(code) => WebTransportError::Closed(
                        code,
                        String::from_utf8_lossy(&close.reason).into_owned(),
                    )
                    .into(),
                    None => SessionError::ConnectionError(e),
                }
            }
            _ => SessionError::ConnectionError(e),
        }
    }
}

/// Why a session closed, shared by the session and its streams. The first close wins.
///
/// It also knows whether the session is raw QUIC, which decides how its close and stream
/// codes are encoded.
#[derive(Debug)]
pub(crate) struct CloseReason {
    // Raw QUIC carries the application code as is; HTTP/3 maps it into its own code space.
    raw: bool,
    reason: OnceLock<SessionError>,
}

impl CloseReason {
    pub(crate) fn new(raw: bool) -> Self {
        Self {
            raw,
            reason: OnceLock::new(),
        }
    }

    /// The QUIC code to reset or stop a stream with.
    pub(crate) fn encode_stream_code(&self, code: u32) -> noq::VarInt {
        if self.raw {
            return code.into();
        }
        noq::VarInt::try_from(web_transport_proto::error_to_http3(code)).unwrap()
    }

    /// Decode the peer's QUIC code for a stream reset or stop, or None if it is not one.
    pub(crate) fn decode_stream_code(&self, code: noq::VarInt) -> Option<u32> {
        let code = code.into_inner();
        // An older raw peer maps its codes into the HTTP/3 range too. No u32 reaches that
        // range, so the mapped form cannot be mistaken for a raw code.
        match web_transport_proto::error_from_http3(code) {
            Some(code) => Some(code),
            None if self.raw => u32::try_from(code).ok(),
            None => None,
        }
    }

    /// Record the close reason, returning false if one was already recorded.
    pub(crate) fn set(&self, err: SessionError) -> bool {
        self.reason.set(err).is_ok()
    }

    /// Replace an error caused by the connection closing with the recorded reason, or
    /// decode the peer's raw QUIC close code when none was recorded.
    pub(crate) fn map(&self, err: SessionError) -> SessionError {
        let conn = match &err {
            SessionError::ConnectionError(conn)
            | SessionError::SendDatagramError(noq::SendDatagramError::ConnectionLost(conn)) => {
                Some(conn)
            }
            SessionError::WebTransportError(WebTransportError::Closed(..)) => None,
            _ => return err,
        };

        if let Some(reason) = self.reason.get() {
            return reason.clone();
        }

        match conn {
            Some(noq::ConnectionError::ApplicationClosed(close)) if self.raw => {
                match u32::try_from(close.error_code.into_inner()) {
                    Ok(code) => WebTransportError::Closed(
                        code,
                        String::from_utf8_lossy(&close.reason).into_owned(),
                    )
                    .into(),
                    Err(_) => err,
                }
            }
            _ => err,
        }
    }
}

/// An error that can occur when reading/writing the WebTransport stream header.
#[derive(Clone, Error, Debug)]
pub enum WebTransportError {
    #[error("closed: code={0} reason={1}")]
    Closed(u32, String),

    #[error("unknown session")]
    UnknownSession,

    #[error("read error: {0}")]
    ReadError(#[from] noq::ReadExactError),

    #[error("write error: {0}")]
    WriteError(#[from] noq::WriteError),
}

/// An error when writing to [`crate::SendStream`]. Similar to [`noq::WriteError`].
#[derive(Clone, Error, Debug)]
pub enum WriteError {
    #[error("STOP_SENDING: {0}")]
    Stopped(u32),

    #[error("invalid STOP_SENDING: {0}")]
    InvalidStopped(noq::VarInt),

    #[error("session error: {0}")]
    SessionError(#[from] SessionError),

    #[error("stream closed")]
    ClosedStream,
}

impl From<noq::WriteError> for WriteError {
    fn from(e: noq::WriteError) -> Self {
        match e {
            noq::WriteError::Stopped(code) => {
                match web_transport_proto::error_from_http3(code.into_inner()) {
                    Some(code) => WriteError::Stopped(code),
                    None => WriteError::InvalidStopped(code),
                }
            }
            noq::WriteError::ClosedStream => WriteError::ClosedStream,
            noq::WriteError::ConnectionLost(e) => WriteError::SessionError(e.into()),
            noq::WriteError::ZeroRttRejected => unreachable!("0-RTT not supported"),
        }
    }
}

/// An error when reading from [`crate::RecvStream`]. Similar to [`noq::ReadError`].
#[derive(Clone, Error, Debug)]
pub enum ReadError {
    #[error("session error: {0}")]
    SessionError(#[from] SessionError),

    #[error("RESET_STREAM: {0}")]
    Reset(u32),

    #[error("invalid RESET_STREAM: {0}")]
    InvalidReset(noq::VarInt),

    #[error("stream already closed")]
    ClosedStream,
}

impl From<noq::ReadError> for ReadError {
    fn from(value: noq::ReadError) -> Self {
        match value {
            noq::ReadError::Reset(code) => {
                match web_transport_proto::error_from_http3(code.into_inner()) {
                    Some(code) => ReadError::Reset(code),
                    None => ReadError::InvalidReset(code),
                }
            }
            noq::ReadError::ConnectionLost(e) => ReadError::SessionError(e.into()),
            noq::ReadError::ClosedStream => ReadError::ClosedStream,
            noq::ReadError::ZeroRttRejected => unreachable!("0-RTT not supported"),
        }
    }
}

/// An error returned by [`crate::RecvStream::read_exact`]. Similar to [`noq::ReadExactError`].
#[derive(Clone, Error, Debug)]
pub enum ReadExactError {
    #[error("finished early")]
    FinishedEarly(usize),

    #[error("read error: {0}")]
    ReadError(#[from] ReadError),
}

impl From<noq::ReadExactError> for ReadExactError {
    fn from(e: noq::ReadExactError) -> Self {
        match e {
            noq::ReadExactError::FinishedEarly(size) => ReadExactError::FinishedEarly(size),
            noq::ReadExactError::ReadError(e) => ReadExactError::ReadError(e.into()),
        }
    }
}

/// An error returned by [`crate::RecvStream::read_to_end`]. Similar to [`noq::ReadToEndError`].
#[derive(Clone, Error, Debug)]
pub enum ReadToEndError {
    #[error("too long")]
    TooLong,

    #[error("read error: {0}")]
    ReadError(#[from] ReadError),
}

impl From<noq::ReadToEndError> for ReadToEndError {
    fn from(e: noq::ReadToEndError) -> Self {
        match e {
            noq::ReadToEndError::TooLong => ReadToEndError::TooLong,
            noq::ReadToEndError::Read(e) => ReadToEndError::ReadError(e.into()),
        }
    }
}

/// An error indicating the stream was already closed.
#[derive(Clone, Error, Debug)]
#[error("stream closed")]
pub struct ClosedStream;

impl From<noq::ClosedStream> for ClosedStream {
    fn from(_: noq::ClosedStream) -> Self {
        ClosedStream
    }
}

/// An error returned when receiving a new WebTransport session.
#[derive(Error, Debug, Clone)]
pub enum ServerError {
    #[error("unexpected end of stream")]
    UnexpectedEnd,

    #[error("connection error")]
    Connection(#[from] noq::ConnectionError),

    #[error("failed to write")]
    WriteError(#[from] noq::WriteError),

    #[error("failed to read")]
    ReadError(#[from] noq::ReadError),

    #[error("failed to exchange h3 settings")]
    SettingsError(#[from] SettingsError),

    #[error("failed to exchange h3 connect")]
    ConnectError(#[from] ConnectError),

    #[error("io error: {0}")]
    IoError(Arc<std::io::Error>),

    #[cfg(any(feature = "aws-lc-rs", feature = "ring"))]
    #[error("rustls error: {0}")]
    Rustls(#[from] rustls::Error),
}

// #[derive(Clone, Error, Debug)]
// pub enum SendDatagramError {
//     #[error("Unsupported peer")]
//     UnsupportedPeer,

//     #[error("Datagram support Disabled by peer")]
//     DatagramSupportDisabled,

//     #[error("Datagram Too large")]
//     TooLarge,

//     #[error("Session errorr: {0}")]
//     SessionError(#[from] SessionError),
// }

// impl From<noq::SendDatagramError> for SendDatagramError {
//     fn from(value: noq::SendDatagramError) -> Self {
//          match value {
//              noq::SendDatagramError::UnsupportedByPeer => SendDatagramError::UnsupportedPeer,
//              noq::SendDatagramError::Disabled => SendDatagramError::DatagramSupportDisabled,
//              noq::SendDatagramError::TooLarge => SendDatagramError::TooLarge,
//              noq::SendDatagramError::ConnectionLost(e) =>
// SendDatagramError::SessionError(e.into()),          }
//     }
// }

impl web_transport_trait::Error for SessionError {
    fn session_error(&self) -> Option<(u32, String)> {
        if let SessionError::WebTransportError(WebTransportError::Closed(code, reason)) = self {
            return Some((*code, reason.to_string()));
        }

        None
    }
}

impl web_transport_trait::Error for WriteError {
    fn session_error(&self) -> Option<(u32, String)> {
        if let WriteError::SessionError(e) = self {
            return e.session_error();
        }

        None
    }

    fn stream_error(&self) -> Option<u32> {
        match self {
            WriteError::Stopped(code) => Some(*code),
            _ => None,
        }
    }
}

impl web_transport_trait::Error for ReadError {
    fn session_error(&self) -> Option<(u32, String)> {
        if let ReadError::SessionError(e) = self {
            return e.session_error();
        }

        None
    }

    fn stream_error(&self) -> Option<u32> {
        match self {
            ReadError::Reset(code) => Some(*code),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// WebTransport keeps the HTTP/3 mapping in both directions, and a code outside its
    /// range is not a WebTransport code.
    #[test]
    fn http3_stream_codes_stay_mapped() {
        let close = CloseReason::new(false);
        let mapped = close.encode_stream_code(5);
        assert_eq!(mapped.into_inner(), web_transport_proto::error_to_http3(5));
        assert_eq!(close.decode_stream_code(mapped), Some(5));
        assert_eq!(close.decode_stream_code(5u32.into()), None);
    }

    /// Raw QUIC sends the code as is and reads both the raw and the legacy mapped form.
    #[test]
    fn raw_stream_codes_pass_through() {
        let close = CloseReason::new(true);
        assert_eq!(close.encode_stream_code(5).into_inner(), 5);
        assert_eq!(close.decode_stream_code(5u32.into()), Some(5));
        assert_eq!(close.decode_stream_code(u32::MAX.into()), Some(u32::MAX));

        let legacy = noq::VarInt::try_from(web_transport_proto::error_to_http3(5)).unwrap();
        assert_eq!(close.decode_stream_code(legacy), Some(5));

        // Past u32 and outside the HTTP/3 range: neither form.
        let invalid = noq::VarInt::from_u64(1 << 40).unwrap();
        assert_eq!(close.decode_stream_code(invalid), None);
    }
}
