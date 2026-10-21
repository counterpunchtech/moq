# [L] Fair share across broadcasts on cluster links

## Goal

On a shared relay-to-relay session, broadcasts share the link fairly and
priority orders streams only inside each broadcast, so one tenant's viewers
asking for the highest priority never starve another tenant's broadcast.
Opening more subscriptions or broadcasts must never buy more bandwidth than
the bucket allows. Single-tenant deployments and the first and last mile keep
strict priority across the session, as today.

## Plan

Needs a fresh `/quest-plan` before starting: there is no settled design.
Revisit once [qmux on the QUIC stream state machine](/quest/m1/quic-qmux.md)
gives every transport moq-quic's scheduler, and once the auth line carries a
tenant identity.

Today every session schedules strictly: one `PriorityQueue` or one moq-quic
send group per subscription, ranked by the subscriber's priority, then the
publisher's track priority, then subscription id
(`rs/moq-net/src/priority.rs`). A relay forwards the max of its downstream
subscriber priorities upstream (`model/subscription.rs`), and
`rs/moq-relay/src/cluster.rs` runs one bidirectional session per peer pair
carrying every broadcast, so a viewer asking for 127 raises its track above
every other tenant's on the cluster link.

Direction carried over from 2026-09-19, not re-confirmed:

- The domain on a relay-to-relay session is the broadcast, keyed by its path,
  behind a relay setting (`cluster.fair`), off by default.
- A tenant-keyed domain (one bucket per customer, however many broadcasts they
  run) needs the tenant identity from the auth line and refines the same tier.
- moq-quic already has the fair tier: send domains sharing bytes by deficit
  round robin, with groups inside them, crate-private until a consumer lands.
  Exposing them gives `transport::poll::Session::send_group` a domain, a
  trait break for every implementer.
- The draft would say a relay MAY scope priority to the broadcast on a
  relay-to-relay connection; no carrier is needed on the wire.

Dropped 2026-10-20 from
[moq-dev/moq#6142](https://github.com/moq-dev/moq/pull/6142), which landed
only signed priorities and the publisher-priority tiebreak. Its first
revision prototyped a send domain per broadcast in the lite publisher
(`session::Schedule { Strict, Broadcast }`, `poll::Session::send_domain`,
public moq-quic domains, `cluster.fair`), falling back to strict on a
transport without domains. The maintainer: "IDK I feel like we don't have a
good plan here". Open questions it left:

- The IETF publisher schedules strictly; it needs the same domains.
- Tenant-keyed domains instead of, or above, broadcast domains.
- FETCH placement: cache-miss and history fetches carry Subscriber Priority
  outside any subscription, so leaving them outside every bucket bypasses
  isolation.
- Subscriber aggregate forwarding: under a fair schedule the downstream max
  should stop being forwarded upstream at its source, not just be ignored by
  the next fair hop.
- Transports without send domains (WebSocket, iroh, today's qmux) and
  moq-transport sessions: refuse, or degrade to strict.
- A cheaper capability probe than opening and dropping a domain.

Prove it with two broadcasts on one cluster session over a bufferbloated
link: neither starves, a downstream 127 does not change the upstream order,
and with fairness off the strict order is unchanged.

## Required

- [qmux on the QUIC stream state machine](/quest/m1/quic-qmux.md) - every transport shares moq-quic's scheduler, so fairness has one implementation

## Related

- [Egress held state](/quest/m1/egress-held-state.md) - whatever replaces parked group streams must still decide who gets the next stream across broadcasts
- [Bound incomplete messages](/quest/m1/quic/receive-memory.md) - the receive-side counterpart on the same moq-quic sessions
