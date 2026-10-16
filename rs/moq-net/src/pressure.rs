//! Receive-side recovery for when held connection credit fills the allowance.
//!
//! A [`Reader`](crate::coding::Reader) tracked here holds the connection credit for its
//! buffered bytes and its incomplete unit, so incomplete messages are bounded by the
//! transport's receive allowance. When the transport reports that the allowance is
//! spent ([`poll_credit_exhausted`](crate::transport::poll::Session::poll_credit_exhausted)),
//! nothing can arrive to complete any unit, so the [`Monitor`] cancels the least useful
//! holder with STOP_SENDING and the session continues with the credit it returns.
//!
//! Victims, first to last:
//! - Media, from the broadcast holding the most credit, lowest priority first. Within a
//!   broadcast the subscription's priority and group order rank the streams, as on the
//!   send side; raw priorities of unrelated broadcasts are never compared.
//! - Unidentified streams, whose header has yet to name a subscription, so a header
//!   gets the chance to identify itself before it is cancelled.
//! - Control streams. Cancelling one may end the session under its protocol, which the
//!   stream's handler reports like any other error.
//!
//! Ties go to the larger holder, then to the newest stream. There is no protected class:
//! the allowance fits the largest supported unit, so cancelling every other holder
//! always leaves room for one to complete.

use std::{
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	task::{Context, Poll, Waker},
};

use slab::Slab;

use crate::{PathOwned, priority::Priority};

/// How useful a tracked stream is, which orders the victims.
#[derive(Clone, Debug)]
pub(crate) enum Rank {
	/// A stream whose header has not identified it yet.
	Unidentified,
	/// A media stream of one broadcast, ranked within it.
	Media {
		/// The broadcast, the domain inside which priorities compare.
		broadcast: PathOwned,
		/// The stream's rank within its broadcast.
		priority: Priority,
	},
	/// A control stream.
	Control,
}

/// One session's tracked receivers.
#[derive(Clone, Default)]
pub(crate) struct Pressure {
	state: kio::Lock<State>,
}

#[derive(Default)]
struct State {
	entries: Slab<Entry>,
	/// Registration order, the last tiebreak: the newest stream goes first.
	next: u64,
}

struct Entry {
	slot: Arc<Slot>,
	rank: Rank,
	seq: u64,
}

/// What a [`Ticket`] shares with the registry without taking its lock.
#[derive(Default)]
struct Slot {
	/// Bytes of credit the reader holds.
	held: AtomicU64,
	/// The monitor chose this stream; its reader stops and lets go.
	evicted: AtomicBool,
	/// The reader's task, woken on eviction.
	waker: Mutex<Option<Waker>>,
}

impl Pressure {
	/// Track a new receiver at `rank`.
	pub fn track(&self, rank: Rank) -> Ticket {
		let slot = Arc::new(Slot::default());
		let mut state = self.state.lock();
		let seq = state.next;
		state.next += 1;
		let id = state.entries.insert(Entry {
			slot: slot.clone(),
			rank,
			seq,
		});
		Ticket {
			pressure: self.clone(),
			id,
			slot,
		}
	}

	/// Cancel the least useful holder of credit, returning whether there was one.
	pub fn evict(&self) -> bool {
		let state = self.state.lock();
		let Some(id) = victim(&state.entries) else {
			return false;
		};
		let slot = &state.entries[id].slot;
		slot.evicted.store(true, Ordering::Release);
		let waker = slot.waker.lock().unwrap_or_else(|err| err.into_inner()).take();
		tracing::debug!(rank = ?state.entries[id].rank, held = slot.held.load(Ordering::Relaxed), "receive credit exhausted, cancelling a stream");
		drop(state);
		if let Some(waker) = waker {
			waker.wake();
		}
		true
	}

	/// The bytes every tracked receiver holds, for tests.
	#[cfg(test)]
	pub fn held(&self) -> u64 {
		let state = self.state.lock();
		state.entries.iter().map(|(_, e)| e.slot.held.load(Ordering::Relaxed)).sum()
	}
}

/// Pick the victim: see the module docs for the order.
fn victim(entries: &Slab<Entry>) -> Option<usize> {
	let held = |entry: &Entry| entry.slot.held.load(Ordering::Relaxed);
	let holding = || {
		entries
			.iter()
			.filter(|(_, entry)| held(entry) > 0 && !entry.slot.evicted.load(Ordering::Relaxed))
	};

	// Media: the broadcast holding the most, then its least useful stream.
	let mut domains: Vec<(&PathOwned, u64)> = Vec::new();
	for (_, entry) in holding() {
		if let Rank::Media { broadcast, .. } = &entry.rank {
			match domains.iter_mut().find(|(path, _)| *path == broadcast) {
				Some((_, total)) => *total += held(entry),
				None => domains.push((broadcast, held(entry))),
			}
		}
	}
	// The larger holder, then the lower path, so the choice never depends on order.
	let domain = domains
		.into_iter()
		.max_by(|(a, a_held), (b, b_held)| a_held.cmp(b_held).then_with(|| b.as_str().cmp(a.as_str())));
	if let Some((domain, _)) = domain {
		return holding()
			.filter_map(|(id, entry)| match &entry.rank {
				Rank::Media { broadcast, priority } if broadcast == domain => Some((id, priority, entry)),
				_ => None,
			})
			// `Priority` sorts the most urgent first, so the greatest is the least useful.
			.max_by(|(_, a, a_entry), (_, b, b_entry)| {
				a.cmp(b)
					.then_with(|| held(a_entry).cmp(&held(b_entry)))
					.then_with(|| a_entry.seq.cmp(&b_entry.seq))
			})
			.map(|(id, ..)| id);
	}

	let largest = |control: bool| {
		holding()
			.filter(|(_, entry)| matches!(entry.rank, Rank::Control) == control)
			.max_by(|(_, a), (_, b)| held(a).cmp(&held(b)).then_with(|| a.seq.cmp(&b.seq)))
			.map(|(id, _)| id)
	};
	largest(false).or_else(|| largest(true))
}

/// A tracked receiver's registration, removed on drop.
pub(crate) struct Ticket {
	pressure: Pressure,
	id: usize,
	slot: Arc<Slot>,
}

impl Ticket {
	/// Re-rank the stream, once its header identifies it.
	pub fn rank(&self, rank: Rank) {
		self.pressure.state.lock().entries[self.id].rank = rank;
	}

	/// Record how many bytes of credit the reader holds.
	pub fn held(&self, bytes: u64) {
		self.slot.held.store(bytes, Ordering::Relaxed);
	}

	/// Whether the monitor chose this stream, registering `cx` to hear when it does.
	pub fn poll_evicted(&self, cx: &Context<'_>) -> bool {
		if self.slot.evicted.load(Ordering::Acquire) {
			return true;
		}
		let mut waker = self.slot.waker.lock().unwrap_or_else(|err| err.into_inner());
		match waker.as_mut() {
			Some(waker) => waker.clone_from(cx.waker()),
			None => *waker = Some(cx.waker().clone()),
		}
		drop(waker);
		// Checked again: an eviction between the load and the store took no waker.
		self.slot.evicted.load(Ordering::Acquire)
	}
}

impl Drop for Ticket {
	fn drop(&mut self) {
		self.pressure.state.lock().entries.remove(self.id);
	}
}

/// Cancels a stream each time the session's receive allowance fills.
pub(crate) struct Monitor<S> {
	session: S,
	pressure: Pressure,
}

impl<S: crate::transport::poll::Session> Monitor<S> {
	/// Watch `session`, cancelling streams tracked by `pressure`.
	pub fn new(session: S, pressure: Pressure) -> Self {
		Self { session, pressure }
	}

	/// Run until dropped. Each fill cancels one stream: the next fill can only come after
	/// it let go of its credit and the peer spent what that freed.
	pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
		while self.session.poll_credit_exhausted(cx).is_ready() {
			if !self.pressure.evict() {
				// Untracked readers or transport read-ahead hold it all; whoever holds it
				// lets go on their own, and a later fill is reported again.
				tracing::warn!("receive credit exhausted with no tracked holder to cancel");
			}
		}
		Poll::Pending
	}
}
