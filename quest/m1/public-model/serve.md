# [S] Public group expiry in JS

## Goal

A custom `@moq/net` publisher learns, through public calls, when a group it is
still writing has expired, as the Rust `group::Consumer::poll_expired` already
lets it. The built-in lite and IETF publishers use the same call.

## Plan

The rest of public serving landed: the `with_range`/`withRange` cursor, public
`poll_start`, `poll_drop`, `poll_expired`, `Position::final_sequence`,
`Subscription::{start, end, with_start, with_end}`, and in JS the `Recv` union
from `tryRecvGroup`, `groupChanged`, `Ordered.tryNextGroup`, `finalSequence`,
and `Track.Consumer.info({ hold })`. The live-edge answers moved to
[track-live](/quest/m1/track-live.md), and `track::Consumer::final_sequence`
to [the deferred quest](/quest/m1/public-model/deferred.md).

Left: `hooks.guardGroup` and `hooks.readGroupFrame` with `ReadGroupFrame`
(both publishers, `js/net/src/group.ts`). A publisher that already read frames
off a group must still abandon its stream once the subscription's max delay
expires the group. Reading a frame the publisher `complete()`s holds the
group's `closed` until it is written. **Decision** on the public shape:

- A public `expired` promise on `Group.Consumer`, resolving with the `Old`
  error once the budget gives up on the group, which a publisher races its
  writes against; the pending hold folds into `readFrame` (a read holds
  `closed` until the next read or close). Recommended: it mirrors
  `poll_expired` and adds no second read method.
- A public `guard(operation)` taking a thunk, as the hook does today. Shorter
  call sites, but a callback parameter.
- Keep both hooks private as plumbing, on the grounds that a JS publisher
  needs the wire stream types anyway.

Tests: a mock-time test that a fully read, unfinished group still reports its
expiry, mirroring the Rust `a_fully_read_group_still_reports_its_expiry`.

## Related

- [Public model](/quest/m1/public-model/README.md) - the epic, its rules, and the inventory method
- [A reader can tell when its track is live](/quest/m1/track-live.md) - owns the publishers' live-edge answers
