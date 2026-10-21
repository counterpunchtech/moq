# [XS] Public group abandonment in Rust

## Goal

A custom `moq-net` publisher waiting on its transport, for stream credit or to
drain, learns through a public call when to give up on a group: the budget
expired it, or its copy was aborted before the publisher read it all and no
route will continue it. The built-in lite and IETF publishers use the same
call.

## Plan

The rest of public serving landed: the `with_range`/`withRange` cursor, public
`poll_start`, `poll_drop`, `poll_expired`, `Position::final_sequence`,
`Subscription::{start, end, with_start, with_end}`, and in JS the `Recv` union
from `tryRecvGroup`, `groupChanged`, `Ordered.tryNextGroup`, `finalSequence`,
`Track.Consumer.info({ hold })`, and `Group.Consumer.expired`. The live-edge
answers moved to [track-live](/quest/m1/track-live.md), and
`track::Consumer::final_sequence` to
[the deferred quest](/quest/m1/public-model/deferred.md).

Left: the crate-private `group::Consumer::poll_abandoned(waiter, held)`,
which [#6140](https://github.com/moq-dev/moq/pull/6140) added so a group
parked on stream credit gives up. It answers `poll_expired` plus the aborted
copy. **Decision** on the public shape:

- Make `poll_abandoned` public and `poll_expired` private, dropping the
  `held` flag the way `poll_expired` did (`poll_open` passes `false` today, so
  an empty finished group parked on credit would then expire). Recommended:
  one call answers "stop writing this group".
- Keep both public, with `held` exposed. A bool parameter callers must reason
  about.
- Keep `poll_abandoned` private: a custom publisher reads the abort on its
  next frame read instead, but holds the credit it waited for until then.

Check whether the JS publishers need the same: `Group.Consumer.expired`
covers the budget, not an aborted copy parked on credit.

## Related

- [Public model](/quest/m1/public-model/README.md) - the epic, its rules, and the inventory method
- [A reader can tell when its track is live](/quest/m1/track-live.md) - owns the publishers' live-edge answers
