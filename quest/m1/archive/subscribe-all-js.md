# [L] SUBSCRIBE_ALL in js/net

## Goal

`@moq/net` speaks moq-lite-07's SUBSCRIBE_ALL in both directions. A JS
consumer lists every broadcast under a prefix and every track each one's
producer creates, with the Rust names mirrored (`broadcasts(prefix)` on the
origin consumer, `tracks()` on the broadcast consumer). A JS publisher answers
a peer's SUBSCRIBE_ALL from its own broadcasts. Older versions refuse the
stream, as in Rust.

## Plan

See [SUBSCRIBE_ALL](/quest/m1/archive/subscribe-all.md) for the decisions and
the wire, which the Rust half fixed in `drafts/draft-lcurley-moq-lite.md`.

- Codec: SUBSCRIBE_ALL, SUBSCRIBE_ALL_UPDATE, the six typed responses, and the
  TRACK_GROUP header (uni 0x2), refused before lite-07.
- Subscriber: one request per prefix per session, shared by every listing of
  it; a TRACK_GROUP stream ahead of its TRACK_START waits for it.
- Publisher: list each broadcast as it appears and each track as its producer
  creates it, never one served only on request; no datagrams.
- Mirror whatever the Rust half settles for the open decisions it left
  (start floor, sharing listed tracks with ordinary requests).

Tests: Rust and JS list each other's broadcasts over lite-07, directly and
through moq-relay, including a track added later and a broadcast ending; a
lite-06 session lists nothing.

Public API: `broadcasts(prefix)` and `tracks()` in `@moq/net`. Wire: none
beyond the Rust half.

## Required

- [SUBSCRIBE_ALL](/quest/m1/archive/subscribe-all.md) - fixes the lite-07 wire and the Rust model this mirrors
