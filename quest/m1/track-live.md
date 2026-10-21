# [M] A reader can tell when its track is live

## Goal

A track reader in moq-net and `@moq/net` can wait for, and check, whether its
track's feed is live: a local producer from the start, a relay's copy once its
upstream subscription answers. A player uses it to skip the old cached groups
a warm copy serves while it waits for a fresh upstream subscription, instead
of rendering them.

## Plan

`largest()` already waits for live internally (#5924), so it never reports
what an ended feed left; this quest makes the signal itself public.

- Rust has the state: `TrackState::feed` (`Live`, `Idle`, `Answered`) and
  `readable()` in `rs/moq-net/src/model/track.rs`, the crate-private
  `resume` `poll_live`, and the crate-private `Producer::is_live`. Expose one
  reader-side `poll_live`/`live()` (or `is_live`) on `track::Subscriber` and
  `track::Ordered`.
- The publishers' wire answers (SUBSCRIBE_START, SUBSCRIBE_OK, TRACK_STATUS
  Largest) are marked here: `track::Subscriber::poll_live` and
  `latest_sequence` on `track::{Producer,Consumer,Subscriber}` in Rust, and
  `wire.ts` `Broadcast.largest`, `hooks.latestSequence`, and
  `hooks.bufferedLargest` in JS. `largest()` follows the source's reported
  largest, which can name a frame the cache does not hold yet, so the
  publishers answer with the buffered edge instead. Fold them into the public
  signal, or keep them private as passthrough.
- JS has no equivalent: a track never goes idle, so its reader is live from
  the start. Mirror the name, and the idle state if JS gains warm copies.
- Decide what a reader does with groups served before live: keep serving
  them (today) and let the caller skip by the signal, or offer a mode that
  starts at the live edge.

## Related

- [Public serving](/quest/m1/public-model/serve.md) - moved the publishers' `poll_live` and the JS largest hooks here
