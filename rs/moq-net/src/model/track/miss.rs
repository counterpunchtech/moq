//! A reader's misses: the sequences it reads that the cache doesn't hold and the live feed
//! will never deliver, asked of the track's fills until each arrives or is given up.
//!
//! Nothing above the newest group the feed produced is a miss, since it arrives live, so a
//! fill is bounded by construction. Below it, a hole is a miss once the feed is past it:
//! under where the feed starts, or accounted for by the arrival ledger without being cached
//! (evicted, or a gap the feed skipped). A hole the ledger still waits for is deferred and
//! judged again as the feed moves.

use std::{
	collections::BTreeSet,
	ops::{self, RangeInclusive},
	task::Poll,
	time::Duration,
};

use rangemap::{RangeInclusiveSet, RangeSet};

use super::fill::{Ask, FillState, Join};
use super::{Drift, Order, Position, TrackState};
use crate::Error;

/// What one reader asks its fills with.
pub(super) struct Asking {
	/// Where the reader's own start falls, for the frame of a run beginning at its group.
	pub start: Option<Position>,
	pub priority: u8,
	pub max_delay: Option<Duration>,
	pub order: Order,
}

/// The misses of one cursor, judged as what it reads or the feed moves.
#[derive(Default)]
pub(super) struct Misses {
	/// What the last probe judged against: the span read, the feed's newest group, and
	/// where the feed starts and readers start. A probe that sees the same skips judging.
	at: Option<(u64, u64, Option<u64>, Option<u64>, Option<u64>, Option<u64>, u64)>,
	/// The sequences judged already, within what the reader reads up to the newest group.
	judged: RangeSet<u64>,
	/// Holes the live feed may still deliver, judged again on every probe.
	deferred: RangeSet<u64>,
	/// The fills this reader waits on.
	joins: Vec<Join>,
	/// The sequences of `joins` still unanswered.
	wanted: RangeSet<u64>,
	/// Answered by a fill and cached, not handed out yet.
	filled: BTreeSet<u64>,
	/// Never asked again: named dropped to the reader.
	given_up: RangeSet<u64>,
}

/// A run of misses the reader gives up, for the drop cursor to name.
pub(super) type GivenUp = Vec<ops::Range<u64>>;

impl Misses {
	/// The lowest sequence still to come from a fill: asked, or answered and not handed out.
	pub fn lowest(&self) -> Option<u64> {
		let wanted = self.wanted.iter().next().map(|run| run.start);
		let filled = self.filled.first().copied();
		match (wanted, filled) {
			(Some(a), Some(b)) => Some(a.min(b)),
			(a, b) => a.or(b),
		}
	}

	/// Whether a fill still owes this reader a group.
	pub fn is_waiting(&self) -> bool {
		!self.wanted.is_empty()
	}

	/// The lowest sequence asked and not answered yet.
	pub fn lowest_wanted(&self) -> Option<u64> {
		self.wanted.iter().next().map(|run| run.start)
	}

	/// Everything given up so far, for a drop cursor created after.
	pub fn given_up(&self) -> &RangeSet<u64> {
		&self.given_up
	}

	/// The lowest answered group still cached, forgetting one evicted before it was read.
	pub fn next_filled(&mut self, state: &TrackState, given_up: &mut GivenUp) -> Option<crate::group::Handle> {
		while let Some(&sequence) = self.filled.first() {
			if let Some(slot) = state.lookup.get(&sequence).filter(|slot| !slot.is_aborted()) {
				return Some(slot.serving().clone());
			}
			self.filled.remove(&sequence);
			self.give_up(sequence..sequence + 1, given_up);
		}
		None
	}

	/// Take `sequence` as handed out.
	pub fn take_filled(&mut self, sequence: u64) {
		self.filled.remove(&sequence);
	}

	/// Forget what the reader no longer reads: below `from`, or outside `admitted`. A fill
	/// nobody waits on is withdrawn once its last join goes.
	pub fn retain(&mut self, from: u64, admitted: &RangeInclusiveSet<u64>) {
		if self.wanted.is_empty() && self.filled.is_empty() {
			return;
		}
		let outside = |sequence: u64| sequence < from || !admitted.contains(&sequence);
		self.filled.retain(|sequence| !outside(*sequence));
		let mut kept = RangeSet::new();
		for run in self.wanted.iter() {
			for inside in clip(run.clone(), from, admitted) {
				kept.insert(inside);
			}
		}
		self.wanted = kept;
		let wanted = &self.wanted;
		self.joins.retain(|join| wanted.overlaps(&join.groups));
	}

	/// Take in what the joined fills answered, parking `waiter` on the rest. Returns the
	/// runs to ask again: refused as too old for a budget tighter than this reader's.
	pub fn resolve(
		&mut self,
		state: &kio::Consumer<TrackState>,
		max_delay: Option<Duration>,
		given_up: &mut GivenUp,
		waiter: &kio::Waiter,
	) -> RangeSet<u64> {
		let mut retry = RangeSet::new();
		let mut i = 0;
		while i < self.joins.len() {
			let join = &self.joins[i];
			let wanted: RangeSet<u64> = self
				.wanted
				.overlapping(&join.groups)
				.map(|run| run.start.max(join.groups.start)..run.end.min(join.groups.end))
				.collect();
			if wanted.is_empty() {
				self.joins.swap_remove(i);
				continue;
			}
			let answers = match join.outcome().poll(waiter, |outcome| {
				let answered = outcome.within(&wanted);
				match answered.is_empty() {
					true => Poll::Pending,
					false => Poll::Ready(answered),
				}
			}) {
				Poll::Ready(Ok(answers)) => answers,
				// Ended without answering the rest: its handlers went away. Whatever the cache
				// holds by now is served from it, and the rest is given up, as for an accept.
				Poll::Ready(Err(closed)) => {
					let mut answers = closed.within(&wanted);
					let mut left = wanted.clone();
					for (groups, _) in &answers {
						left.remove(groups.clone());
					}
					answers.extend(left.iter().map(|run| (run.clone(), Ok(()))));
					answers
				}
				Poll::Pending => {
					i += 1;
					continue;
				}
			};
			for (groups, answer) in answers {
				self.wanted.remove(groups.clone());
				match answer {
					Ok(()) => {
						let state = state.read();
						for sequence in groups {
							match state.lookup.get(&sequence).is_some_and(|slot| !slot.is_aborted()) {
								true => {
									self.filled.insert(sequence);
								}
								false => self.give_up(sequence..sequence + 1, given_up),
							}
						}
					}
					Err((Error::Old, asked)) if looser(max_delay, asked) => retry.insert(groups),
					// A drop the track recorded is named by the drop cursor already.
					Err(_) => self.give_up(groups, given_up),
				}
			}
			// Judged again: the same join may have more to answer, or park the waiter now.
		}
		retry
	}

	/// Judge the holes in `target` the last probe has not, returning the runs to ask a
	/// fill for. Gives up a run already stale under this reader's budget.
	pub fn judge(
		&mut self,
		state: &TrackState,
		key: (u64, u64, Option<u64>),
		target: impl FnOnce() -> RangeInclusiveSet<u64>,
		drift: &Drift,
		given_up: &mut GivenUp,
	) -> RangeSet<u64> {
		let mut asks = RangeSet::new();
		// The feed's start is unknown until the serving session resolves it, and readers
		// take nothing from a cache that does not reflect the feed.
		if state.start_pending || !state.readable() {
			return asks;
		}
		let Some(newest) = state.newest_produced else {
			return asks;
		};
		let at = (
			key.0,
			key.1,
			key.2,
			Some(newest),
			state.live_floor,
			state.start_sequence,
			state.drops_revision,
		);
		if self.at == Some(at) && self.deferred.is_empty() {
			return asks;
		}
		self.at = Some(at);

		let low = state.live_floor.unwrap_or(0);
		let mut span = RangeSet::new();
		if low <= newest {
			for run in target().overlapping(&(low..=newest)) {
				span.insert((*run.start()).max(low)..(*run.end()).min(newest) + 1);
			}
		}
		// What left the span is forgotten, so it is judged afresh if it comes back.
		let judged: RangeSet<u64> = self
			.judged
			.iter()
			.flat_map(|run| span.overlapping(run).map(move |s| s.start.max(run.start)..s.end.min(run.end)))
			.collect();
		let mut candidates: RangeSet<u64> = span.iter().flat_map(|run| judged.gaps(run)).collect();
		for run in std::mem::take(&mut self.deferred).iter() {
			for s in span.overlapping(run) {
				candidates.insert(s.start.max(run.start)..s.end.min(run.end));
			}
		}
		self.judged = judged;
		for run in candidates.iter() {
			self.judged.insert(run.clone());
		}

		let start = state.start_sequence.or(state.arrivals.first());
		let datagrams: BTreeSet<u64> = state.datagrams.iter().map(|datagram| datagram.sequence).collect();
		for run in candidates.iter() {
			for hole in holes(state, run.clone()) {
				for hole in self.unclaimed(state, hole) {
					let Some(start) = start else {
						self.deferred.insert(hole);
						continue;
					};
					// Below where the feed starts: it will never come live.
					if hole.start < start {
						asks.insert(hole.start..hole.end.min(start));
					}
					let above = hole.start.max(start)..hole.end;
					if above.is_empty() {
						continue;
					}
					// Accounted for by the feed and not cached: evicted, or skipped. A buffered
					// datagram arrived and is served as one.
					let mut from = above.start;
					for arrived in state.arrivals.accounted(above.clone()) {
						if from < arrived.start {
							self.deferred.insert(from..arrived.start);
						}
						asks.insert(arrived.clone());
						for sequence in datagrams.range(arrived.clone()) {
							asks.remove(*sequence..*sequence + 1);
						}
						from = arrived.end;
					}
					if from < above.end {
						self.deferred.insert(from..above.end);
					}
				}
			}
		}

		// Starts never rewind, so a run whose newest group is stale under this reader's
		// budget is stale throughout.
		let stale: Vec<ops::Range<u64>> = asks
			.iter()
			.filter(|run| state.drifted(run.end - 1, &drift.edge, drift.budget))
			.cloned()
			.collect();
		for run in stale {
			asks.remove(run.clone());
			self.give_up(run, given_up);
		}
		asks
	}

	/// Ask the fills for `runs`, giving up what no handler can take.
	pub fn ask(&mut self, fills: &kio::Shared<FillState>, runs: &RangeSet<u64>, asking: &Asking, given_up: &mut GivenUp) {
		for run in runs.iter() {
			let frame = asking
				.start
				.filter(|start| start.group == run.start)
				.map_or(0, |start| start.frame);
			let ask = Ask {
				start: Position {
					group: run.start,
					frame,
				},
				end: run.end,
				priority: asking.priority,
				max_delay: asking.max_delay,
				order: asking.order,
			};
			let (joins, unfillable) = fills.lock().join(fills, &ask);
			self.wanted.insert(run.clone());
			for groups in unfillable {
				self.wanted.remove(groups.clone());
				self.give_up(groups, given_up);
			}
			self.joins.extend(joins);
		}
	}

	/// The part of `hole` nobody has a verdict on yet.
	fn unclaimed(&self, state: &TrackState, hole: ops::Range<u64>) -> Vec<ops::Range<u64>> {
		let mut left = RangeSet::new();
		left.insert(hole);
		for claimed in [&state.drops, &self.given_up, &self.wanted] {
			for run in claimed.iter() {
				left.remove(run.clone());
			}
		}
		for sequence in &self.filled {
			left.remove(*sequence..*sequence + 1);
		}
		left.iter().cloned().collect()
	}

	fn give_up(&mut self, groups: ops::Range<u64>, given_up: &mut GivenUp) {
		if groups.is_empty() {
			return;
		}
		self.given_up.insert(groups.clone());
		given_up.push(groups);
	}
}

/// The runs of `run` the cache holds nothing servable for.
fn holes(state: &TrackState, run: ops::Range<u64>) -> Vec<ops::Range<u64>> {
	let mut holes = Vec::new();
	let mut from = run.start;
	for (&sequence, slot) in state.lookup.range(run.clone()) {
		if slot.is_aborted() {
			continue;
		}
		if from < sequence {
			holes.push(from..sequence);
		}
		from = sequence + 1;
	}
	if from < run.end {
		holes.push(from..run.end);
	}
	holes
}

/// The parts of `run` at or above `from` inside `admitted`.
fn clip(run: ops::Range<u64>, from: u64, admitted: &RangeInclusiveSet<u64>) -> Vec<ops::Range<u64>> {
	let run = run.start.max(from)..run.end;
	if run.is_empty() {
		return Vec::new();
	}
	let inclusive: RangeInclusive<u64> = run.start..=run.end - 1;
	admitted
		.overlapping(&inclusive)
		.map(|inside| (*inside.start()).max(run.start)..(*inside.end()).min(run.end - 1) + 1)
		.collect()
}

/// Whether `ours` tolerates more than the budget a fill `asked` with, so its [`Error::Old`]
/// was not this reader's.
fn looser(ours: Option<Duration>, asked: Option<Duration>) -> bool {
	match (asked, ours) {
		(Some(asked), Some(ours)) => ours > asked,
		(Some(_), None) => true,
		(None, _) => false,
	}
}
