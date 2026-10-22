# Migrating qmux

How to update your code for each breaking change, newest first.

## Unreleased

### Sessions implement moq-net's poll traits (#PRNUM)

qmux moved into the moq repository and runs on moq-quic's stream state machine.
`Session`, `SendStream`, and `RecvStream` implement `moq_net::transport::poll` in place of `web_transport_trait`.
Import `moq_net::transport::poll::{Session as _, SendStream as _, RecvStream as _}` and use its `open_uni`, `accept_uni`, `write_buf`, `read_chunk`, `close`, and `closed`.
Streams opened with the same `SendGroup` (from `Session::send_group(priority)`) share a scheduling group.
`SendStream::reset_at(offset, code)` is new and sends draft-02's `RESET_STREAM_AT`.

### The legacy `webtransport` wire format is gone (#PRNUM)

`Version::WebTransport`, `Version::is_qmux`, and the `webtransport` entry in `ALPNS` are removed, as is the WebSocket server's fallback to it when a client offers no subprotocol.
Every peer must speak QMux draft-00, 01, or 02; a WebSocket client must offer one of `ALPNS` (or a `qmux-NN.<protocol>` subprotocol).

### `ws::Upgraded` takes the negotiated subprotocol, and fails loud (#PRNUM)

`Upgraded::new(ws)` and `with_alpn(alpn)` merge into `Upgraded::new(ws, alpn)`.
`Upgraded::connect` and `accept` return `Result<Session, Error>`, refusing a subprotocol that names no QMux version with `Error::InvalidProtocol`.

### `Config` collapses the stream windows and makes timers optional (#PRNUM)

`max_stream_data_bidi_local`, `max_stream_data_bidi_remote`, and `max_stream_data_uni` merge into `max_stream_data`.
`max_idle_timeout` (milliseconds), `handshake_timeout`, and `rtt_interval` become `Option<Duration>`, with `None` disabling each.
`tcp::Config::handshake_timeout` and `uds::Config::handshake_timeout` take an `Option<Duration>` to match.

### `Error` carries plain codes (#PRNUM)

`ConnectionClosed`, `ConnectionReset`, `StreamReset`, and `StreamStop` carry `u64` codes in place of `VarInt`.
`InvalidFrameType`, `InvalidStreamId`, `DuplicateParam`, `Short`, and `UnexpectedProtocols` are removed: malformed input surfaces as `FrameEncoding`, `ProtocolViolation`, or `TransportParameter`.
`DatagramTooLarge` is new.

### `StreamId` and `StreamDir` are no longer exported (#PRNUM)

Stream identity is internal to moq-quic's state machine; nothing in the public API takes one.
