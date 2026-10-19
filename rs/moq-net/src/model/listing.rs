//! Every broadcast published under a prefix, found through the routes covering it.
//!
//! A route claims capability, not inventory, so the route table alone cannot say which
//! broadcasts exist under a prefix. A broadcast published on this origin is its own
//! answer. A route a handler serves ([`Dynamic`](super::Dynamic)) is asked, through the
//! handler's [`Lister`], for a [`Listing`] of the prefix, which the handler fills as it
//! learns of broadcasts: a session forwards the request to its peer. A handler that never
//! answers lists nothing, which is how a service serving any path on request stays out of
//! the listing.

use std::{
	collections::{BTreeMap, HashMap, VecDeque},
	task::{Poll, ready},
};

use super::{AnnounceConsumer, AnnounceEvent, Consumer};
use crate::{AsPath, Error, PathOwned, broadcast};

/// The broadcasts a handler lists under one prefix, in the order it learned them.
#[derive(Default)]
pub(crate) struct ListingState {
	// Keyed by insertion order, so a reader resumes after the last one it saw.
	broadcasts: BTreeMap<u64, (broadcast::Consumer, Option<crate::Epoch>)>,
	next: u64,
}

/// The handler's side of a listing: the broadcasts it holds under the requested prefix.
///
/// Holding a broadcast here holds it for the readers to come. Readers going away is
/// [`Self::poll_unused`]; dropping the listing ends it for them.
pub(crate) struct Listing {
	prefix: PathOwned,
	state: kio::Producer<ListingState>,
}

impl Listing {
	/// The absolute prefix whose broadcasts are listed.
	pub fn prefix(&self) -> &crate::Path<'_> {
		&self.prefix
	}

	/// List a broadcast, published by the instance `epoch` names, returning the key that
	/// [`Self::remove`] takes.
	pub fn insert(&self, broadcast: broadcast::Consumer, epoch: Option<crate::Epoch>) -> u64 {
		let Ok(mut state) = self.state.write() else {
			return 0;
		};
		let key = state.next;
		state.next += 1;
		state.broadcasts.insert(key, (broadcast, epoch));
		key
	}

	/// Stop listing a broadcast: it ended, and nobody should find it here.
	pub fn remove(&self, key: u64) {
		if let Ok(mut state) = self.state.write() {
			state.broadcasts.remove(&key);
		}
	}

	/// Ready once nobody reads the listing.
	pub fn poll_unused(&self, waiter: &kio::Waiter) -> Poll<()> {
		self.state.poll_unused(waiter).map(|_| ())
	}
}

/// What a [`Broadcasts`] cursor reads from one listing.
struct ListingReader {
	state: kio::Consumer<ListingState>,
	next: u64,
}

impl ListingReader {
	/// The next listed broadcast, `Ready(None)` once the handler dropped the listing.
	fn poll_next(&mut self, waiter: &kio::Waiter) -> Poll<Option<(broadcast::Consumer, Option<crate::Epoch>)>> {
		let next = self.next;
		let found = self
			.state
			.poll(waiter, |state| match state.broadcasts.range(next..).next() {
				Some((key, (broadcast, epoch))) => Poll::Ready((*key, broadcast.clone(), epoch.clone())),
				None => Poll::Pending,
			});
		match ready!(found) {
			Ok((key, broadcast, epoch)) => {
				self.next = key + 1;
				Poll::Ready(Some((broadcast, epoch)))
			}
			Err(_) => Poll::Ready(None),
		}
	}
}

/// Where a handler takes the listings readers ask of it.
///
/// One per handler, shared by every route it serves: a reader asks it once per prefix
/// however many of its routes cover the prefix, and the handler fills one listing.
#[derive(Clone, Default)]
pub(crate) struct Lister {
	state: kio::Shared<Listings>,
}

impl Lister {
	/// Poll for the next prefix a reader wants the broadcasts of.
	///
	/// Fill the [`Listing`] with every broadcast the publisher behind the handler holds
	/// under its prefix, and keep it current. A handler that serves paths only on request
	/// never polls this, so nothing it would serve that way is ever listed.
	#[cfg_attr(not(feature = "async"), allow(dead_code))]
	pub fn poll_requested(&self, waiter: &kio::Waiter) -> Poll<Listing> {
		let mut state = ready!(self.state.poll(waiter, |state| match state.queued.is_empty() {
			true => Poll::Pending,
			false => Poll::Ready(()),
		}));
		Poll::Ready(state.queued.pop_front().expect("predicate guaranteed a listing"))
	}

	fn read(&self, prefix: &PathOwned) -> ListingReader {
		self.state.lock().read(prefix)
	}

	fn same_channel(&self, other: &Self) -> bool {
		self.state.same_channel(&other.state)
	}

	/// Drop every listing the handler has yet to take: the origin is gone.
	pub(super) fn close(&self) {
		self.state.lock().clear();
	}
}

/// The listings a handler has yet to take, and the ones its readers share.
#[derive(Default)]
struct Listings {
	queued: VecDeque<Listing>,
	// The readers' side of each prefix's listing, so a second reader joins the first.
	live: HashMap<PathOwned, kio::ConsumerWeak<ListingState>>,
}

impl Listings {
	/// Read the listing of `prefix`, asking the handler for one unless a reader already did.
	fn read(&mut self, prefix: &PathOwned) -> ListingReader {
		if let Some(weak) = self.live.get(prefix).filter(|weak| !weak.is_closed()) {
			let state = weak.consume();
			return ListingReader { state, next: 0 };
		}
		// Unread listings the handler never took are dead weight; drop them as we go.
		self.queued.retain(|listing| listing.state.is_used());
		let state = kio::Producer::<ListingState>::default();
		let reader = state.consume();
		self.live.retain(|_, weak| !weak.is_closed());
		self.live.insert(prefix.clone(), reader.weak());
		self.queued.push_back(Listing {
			prefix: prefix.clone(),
			state,
		});
		ListingReader { state: reader, next: 0 }
	}

	fn clear(&mut self) {
		self.queued.clear();
		self.live.clear();
	}
}

/// What serves an announced prefix, as far as listing goes.
enum Behind {
	/// A broadcast published on this origin at exactly the prefix.
	Local,
	/// A handler, asked for a listing.
	Server(Lister),
	/// Nothing that lists: an advertisement without a server.
	Nothing,
}

/// Every broadcast published under a prefix, from [`Consumer::broadcasts`].
///
/// Each one is yielded once, as it appears: a broadcast published on this origin
/// when it announces, and one behind a peer's route once the peer lists it. A new
/// publisher instance at the same path is another broadcast, yielded again. A
/// broadcast's [`tracks`](broadcast::Consumer::tracks) are every track its producer
/// creates.
pub struct Broadcasts {
	consumer: Consumer,
	announced: AnnounceConsumer,
	// The handler serving each announced prefix (absolute), to drop its listing once none does.
	routes: HashMap<PathOwned, Lister>,
	// One reader per handler, with how many announced prefixes it serves. Few: a session
	// is one handler however many routes it announces.
	listings: Vec<(Lister, ListingReader, usize)>,
	// Ready to yield, and what was yielded by path, to skip a broadcast reached twice.
	ready: VecDeque<(broadcast::Consumer, Option<crate::Epoch>)>,
	yielded: HashMap<PathOwned, Vec<broadcast::Consumer>>,
	// The yielded paths after the last sweep of ended broadcasts, to sweep once it doubles.
	swept: usize,
	// The prefix as the origin names it, for listing requests, and as the consumer does.
	prefix: PathOwned,
	relative: PathOwned,
}

impl Consumer {
	/// Every broadcast published under `prefix`, as each appears.
	///
	/// Together with [`broadcast::Consumer::tracks`], this is every track of every
	/// broadcast under the prefix. A broadcast published on this origin is yielded once it
	/// announces; one behind a peer's route once the peer lists it (moq-lite's
	/// SUBSCRIBE_ALL). Nothing is asked of a handler that serves paths only on request, so
	/// its broadcasts and tracks are never created for this.
	///
	/// Fails with [`Error::Unauthorized`] when this consumer's scope cannot reach the
	/// prefix.
	pub fn broadcasts(&self, prefix: impl AsPath) -> Result<Broadcasts, Error> {
		let prefix = prefix.as_path();
		let scoped = self.scope(&prefix, &crate::Patterns::from(crate::Pattern::subtree("")?))?;
		let named = self.root.join(&prefix).to_owned();
		let absolute = self.scope.resolve(&named).ok_or(Error::Unauthorized)?.to_owned();
		// A lookup, not announce forwarding: it must not drive the egress announce guards.
		let announced = scoped.untagged().announced();
		Ok(Broadcasts {
			consumer: self.clone(),
			announced,
			routes: HashMap::new(),
			listings: Vec::new(),
			ready: VecDeque::new(),
			yielded: HashMap::new(),
			swept: 0,
			prefix: absolute,
			relative: prefix.to_owned(),
		})
	}
}

impl Broadcasts {
	/// Poll for the next broadcast under the prefix, or `None` once the origin closes.
	pub fn poll_next(&mut self, waiter: &kio::Waiter) -> Poll<Option<broadcast::Consumer>> {
		loop {
			if let Some((broadcast, epoch)) = self.ready.pop_front() {
				if let Some(broadcast) = self.hand_out(broadcast, epoch) {
					return Poll::Ready(Some(broadcast));
				}
				continue;
			}

			let mut progress = false;
			let mut ended = false;
			match self.announced.poll_next(waiter) {
				Poll::Ready(Some(event)) => {
					self.route(event);
					progress = true;
				}
				Poll::Ready(None) => ended = true,
				Poll::Pending => {}
			}

			let mut index = 0;
			while index < self.listings.len() {
				match self.listings[index].1.poll_next(waiter) {
					Poll::Ready(Some(listed)) => {
						self.ready.push_back(listed);
						progress = true;
						index += 1;
					}
					// The handler dropped the listing: its broadcasts carry on, but it lists
					// no new ones.
					Poll::Ready(None) => {
						self.listings.swap_remove(index);
					}
					Poll::Pending => index += 1,
				}
			}

			if progress {
				continue;
			}
			if ended {
				return Poll::Ready(None);
			}
			return Poll::Pending;
		}
	}

	/// Wait for the next broadcast under the prefix, or `None` once the origin closes.
	#[cfg(feature = "async")]
	pub async fn next(&mut self) -> Option<broadcast::Consumer> {
		kio::wait(|waiter| self.poll_next(waiter)).await
	}

	/// Apply an announcement: list what serves the prefix, or forget what no longer does.
	fn route(&mut self, event: AnnounceEvent) {
		let (AnnounceEvent::Start(announce) | AnnounceEvent::Update(announce) | AnnounceEvent::Restart(announce)) =
			event
		else {
			let AnnounceEvent::End(announce) = event else {
				unreachable!()
			};
			let absolute = self.announced.absolute(&announce.prefix).to_owned();
			if let Some(lister) = self.routes.remove(&absolute) {
				self.release(&lister);
			}
			return;
		};
		let absolute = self.announced.absolute(&announce.prefix).to_owned();
		let lister = match self.behind(&absolute) {
			Behind::Local => {
				if let Some(source) = self.local(&absolute) {
					self.ready.push_back((source, announce.route.epoch.clone()));
				}
				None
			}
			Behind::Server(lister) => Some(lister),
			Behind::Nothing => None,
		};
		let previous = match lister {
			Some(lister) => {
				self.hold(&lister);
				self.routes.insert(absolute, lister)
			}
			None => self.routes.remove(&absolute),
		};
		if let Some(previous) = previous {
			self.release(&previous);
		}
	}

	/// Count another announced prefix `lister`'s handler serves, reading its listing on the
	/// first. A route above the prefix lists the prefix; one beneath it lists the whole
	/// prefix too, so a handler serving several routes beneath it answers one request.
	fn hold(&mut self, lister: &Lister) {
		match self.listings.iter_mut().find(|(held, ..)| held.same_channel(lister)) {
			Some((.., routes)) => *routes += 1,
			None => {
				let reader = lister.read(&self.prefix);
				self.listings.push((lister.clone(), reader, 1));
			}
		}
	}

	/// Count one fewer prefix `lister`'s handler serves, no longer reading its listing at none.
	fn release(&mut self, lister: &Lister) {
		let Some(index) = self.listings.iter().position(|(held, ..)| held.same_channel(lister)) else {
			return;
		};
		self.listings[index].2 -= 1;
		if self.listings[index].2 == 0 {
			self.listings.swap_remove(index);
		}
	}

	/// What wins `path` for this cursor's reader.
	fn behind(&self, path: &PathOwned) -> Behind {
		let table = self.consumer.shared.lock();
		let horizon = self.consumer.horizon.effective(&table.routes, &path.as_path());
		let Some(best) = table.best_route(&path.as_path(), horizon, &Default::default(), |_| true) else {
			return Behind::Nothing;
		};
		if best.source.is_some() && best.prefix == *path {
			return Behind::Local;
		}
		match &best.server {
			Some(server) => Behind::Server(server.lock().lister.clone()),
			None => Behind::Nothing,
		}
	}

	/// The broadcast published on this origin at `path`, if it still wins it.
	fn local(&self, path: &PathOwned) -> Option<broadcast::Consumer> {
		let table = self.consumer.shared.lock();
		let horizon = self.consumer.horizon.effective(&table.routes, &path.as_path());
		let best = table.best_route(&path.as_path(), horizon, &Default::default(), |_| true)?;
		best.source.clone().filter(|_| best.prefix == *path)
	}

	/// Stamp a broadcast as this cursor names it, unless it was already yielded or ended.
	fn hand_out(&mut self, broadcast: broadcast::Consumer, epoch: Option<crate::Epoch>) -> Option<broadcast::Consumer> {
		if broadcast.is_closed() {
			return None;
		}
		// Named as the consumer names it: the requested prefix, then the path beneath it.
		let suffix = broadcast.info().path.strip_prefix(&self.prefix)?.to_owned();
		let path = self.relative.join(&suffix).to_owned();
		let yielded = self.yielded.entry(path.clone()).or_default();
		yielded.retain(|yielded| !yielded.is_closed());
		// The same broadcast again, or a replica of an instance already yielded, reached
		// through another handler.
		if yielded
			.iter()
			.any(|yielded| yielded.is_clone(&broadcast) || (epoch.is_some() && yielded.info().epoch == epoch))
		{
			return None;
		}
		let scope = self.consumer.stats.egress(self.consumer.root.join(&path).to_owned());
		let broadcast = broadcast.with_path(path).with_epoch(epoch).with_stats(scope);
		yielded.push(broadcast.clone());
		// Ended broadcasts at paths never reached again, swept once their count doubles.
		if self.yielded.len() > 2 * self.swept.max(16) {
			self.yielded.retain(|_, yielded| {
				yielded.retain(|yielded| !yielded.is_closed());
				!yielded.is_empty()
			});
			self.swept = self.yielded.len();
		}
		Some(broadcast)
	}
}

#[cfg(all(test, feature = "async"))]
mod tests {
	use std::time::Duration;

	use super::super::ProduceTest;
	use super::*;
	use crate::{Hop, origin::Route};

	/// Routes sharing a handler's lister ask it once per prefix, and losing one of them
	/// keeps the listing for the rest.
	#[moq_clock::test]
	async fn routes_sharing_a_lister_are_asked_once() {
		let origin = Hop::new(1).unwrap().produce();
		let lister = Lister::default();
		let a = origin
			.dynamic_listed("room/a", Route::default(), lister.clone())
			.unwrap();
		let _b = origin
			.dynamic_listed("room/b", Route::default(), lister.clone())
			.unwrap();
		let mut cursor = origin.consume().broadcasts("room").unwrap();
		let quiet = Duration::from_millis(10);
		assert!(
			moq_clock::timeout(quiet, cursor.next()).await.is_err(),
			"nothing listed yet"
		);

		let Poll::Ready(listing) = lister.poll_requested(&kio::Waiter::noop()) else {
			panic!("the cursor asked for a listing");
		};
		assert!(lister.poll_requested(&kio::Waiter::noop()).is_pending(), "asked once");
		assert_eq!(listing.prefix().as_str(), "room");

		drop(a);
		assert!(
			moq_clock::timeout(quiet, cursor.next()).await.is_err(),
			"nothing listed yet"
		);
		let source = origin.create_source("room/b");
		listing.insert(source.consume(), None);
		let listed = moq_clock::timeout(quiet, cursor.next())
			.await
			.expect("still reading the listing")
			.expect("origin open");
		assert_eq!(listed.info().path.as_str(), "room/b");
	}
}
