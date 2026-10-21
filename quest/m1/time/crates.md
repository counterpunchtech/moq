# [XL] Every crate on moq-clock

## Goal

Every remaining workspace crate (moq-tokio, moq-relay, moq-cli,
moq-hls, moq-auth, moq-archive, moq-audio, moq-video, moq-gst, moq-rtc,
moq-rtmp, moq-srt, moq-shaper, moq-sock, moq-transcode, moq-e2ee, moq-boy,
moq-bench, moq-c, hang, and the rest under `rs/`) reads time and arms timers
only through `moq-clock`, and tests needed to prove those migrations use the sim, paused tokio, or
events. No production direct `Instant::now`, `SystemTime::now`, `tokio::time`, or
`std::thread::sleep` remains outside moq-clock's backends and explicit
performance measurement (moq-bench's timings).

## Plan

Follow the real-I/O rule in [the line's README](/quest/m1/time/README.md):
a migrated test with no real socket, process, or device uses the sim (or paused
tokio until it can); one that needs real I/O waits on events, never on paused
tokio, which auto-advances to a timer before the OS event arrives, and never
on a fixed sleep. moq-relay's in-memory integration tests
(`tests/integration/connection.rs`) move onto the sim with
[the runtime trait](/quest/m2/runtime-trait.md), not here.

moq-auth's `Grant` and lease clock, and moq-relay's lease, credential, and
admission timers, already run on moq-clock (`Grant::deadline` returns a
`moq_clock::Instant`); build on that rather than migrating them again.

Fix mixed clocks first: anything that reads std time but waits on tokio time
(a backoff's stability window, a gate computed with std `Instant` and slept
with tokio) is a bug under paused time.

Split the PR by crate group if review needs it; ban direct clock reads in each
production scope as it finishes. Unrelated legacy test conversions and the
workspace-wide lint ratchet follow in m2.

[#5987](https://github.com/moq-dev/moq/pull/5987) migrated moq-auth (auth
accounting, JWT checks, policy expiry), RTMP session timestamps and
deadlines, archive retention, and listener stall accounting, and added the
per-crate ratchet to moq-auth, moq-rtmp, and moq-archive. The ratchet also
covers moq-net, moq-mux, moq-ffi, moq-room, moq-stats, and moq-uring.

[#6135](https://github.com/moq-dev/moq/pull/6135) moved every other crate's production reads and
timers onto moq-clock (adding `timeout_at` and `interval_at`) and gave each a
ratchet, except moq-ice and moq-rtc, which drive str0m on std instants. What
remains:

- Third-party APIs on std's clock still convert at an `#[allow]`ed boundary
  with a second host read: str0m (moq-ice, moq-rtc, moq-tokio's `webrtc`),
  srt-tokio (moq-srt's `host_instant`), and winit (moq-cli's player window).
  They need clock support, such as a moq-clock conversion to and from
  `std::time::Instant`.
- Device threads (audio, video, moq-boy, moq-bench's sampler) block in
  `std::thread::sleep` outside any runtime, under a per-site `#[allow]`.
- Public wall times are still `SystemTime`: hang's `Clock::wall_clock` and
  `echo::Arrival`, moq-mux's `Clock::at` and `Timebase::place`, moq-hls's
  manifests, moq-quic's `TimeSource`, moq-relay's session start, and
  moq-tokio's certificate expiry. Their ratchets allow the `SystemTime` type
  until they move to `moq_clock::Wall`.

Public API: breaking wherever a crate exposes an instant or a time source.
Wire: none.

## Related

- [Time test sweep](/quest/m2/time-test-sweep.md) - unrelated test-only/private clock cleanup and global lint are deferred; migrate production/public APIs and their required regression tests here
