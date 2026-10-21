//! Where a session's publisher places the streams it serves, following its [`Schedule`].

use std::{
	collections::HashMap,
	sync::{Arc, Mutex, Weak},
};

use crate::{Path, session::Schedule};

type Registry<D> = Mutex<HashMap<String, Weak<Held<D>>>>;

/// Ranks what a session's publisher serves: one strict order, or a send domain per broadcast.
pub(crate) struct Scheduler<D> {
	schedule: Schedule,
	/// Each served broadcast's send domain, while anything of it is in flight.
	domains: Arc<Registry<D>>,
}

impl<D> Clone for Scheduler<D> {
	fn clone(&self) -> Self {
		Self {
			schedule: self.schedule,
			domains: self.domains.clone(),
		}
	}
}

/// A broadcast's send domain, released once the last handle is.
pub(crate) type Domain<D> = Arc<Held<D>>;

pub(crate) struct Held<D> {
	domain: D,
	broadcast: String,
	domains: Weak<Registry<D>>,
}

impl<D> Held<D> {
	/// The transport's domain.
	pub fn get(&self) -> &D {
		&self.domain
	}
}

impl<D> Drop for Held<D> {
	fn drop(&mut self) {
		let Some(domains) = self.domains.upgrade() else {
			return;
		};
		let mut domains = domains.lock().unwrap();
		// A later stream of the broadcast may have opened a fresh domain since this one's last
		// handle dropped, and that entry stays.
		if domains
			.get(&self.broadcast)
			.is_some_and(|entry| entry.strong_count() == 0)
		{
			domains.remove(&self.broadcast);
		}
	}
}

impl<D> Scheduler<D> {
	pub fn new(schedule: Schedule) -> Self {
		Self {
			schedule,
			domains: Default::default(),
		}
	}

	/// The subscriber priority a peer's request is served at: as asked on a strict session,
	/// the unset default on one scheduled per broadcast, so it neither ranks the request's
	/// streams nor reaches the publisher upstream.
	pub fn subscriber(&self, requested: i8) -> i8 {
		match self.schedule {
			Schedule::Strict => requested,
			Schedule::Broadcast => 0,
		}
	}

	/// The send domain for streams of `broadcast`: `None`, the session's default, on a strict
	/// session, or the broadcast's own, opened with `open` unless one is already live.
	///
	/// Hold the returned handle for as long as any stream of the broadcast is in flight.
	pub fn domain(&self, broadcast: &Path, open: impl FnOnce() -> Option<D>) -> Option<Domain<D>> {
		if self.schedule == Schedule::Strict {
			return None;
		}

		let mut domains = self.domains.lock().unwrap();
		if let Some(held) = domains.get(broadcast.as_str()).and_then(Weak::upgrade) {
			return Some(held);
		}
		// A session scheduled per broadcast refuses to start on a transport without domains.
		let held = Arc::new(Held {
			domain: open()?,
			broadcast: broadcast.as_str().to_owned(),
			domains: Arc::downgrade(&self.domains),
		});
		domains.insert(held.broadcast.clone(), Arc::downgrade(&held));
		Some(held)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn strict_uses_the_default_domain() {
		let scheduler = Scheduler::<u32>::new(Schedule::Strict);
		assert!(scheduler.domain(&Path::new("a"), || Some(1)).is_none());
		assert_eq!(scheduler.subscriber(9), 9);
	}

	/// Streams of one broadcast share a domain while any is in flight, and the next one after
	/// the last left opens a fresh domain.
	#[test]
	fn one_domain_per_live_broadcast() {
		let scheduler = Scheduler::new(Schedule::Broadcast);
		let mut opened = 0;
		let mut open = || {
			opened += 1;
			Some(opened)
		};
		let a = scheduler.domain(&Path::new("a"), &mut open).unwrap();
		let again = scheduler.domain(&Path::new("a"), &mut open).unwrap();
		let b = scheduler.domain(&Path::new("b"), &mut open).unwrap();
		assert_eq!((*a.get(), *again.get(), *b.get()), (1, 1, 2));
		assert_eq!(scheduler.subscriber(9), 0, "the peer's priority is ignored");

		drop((a, again));
		assert_eq!(scheduler.domains.lock().unwrap().len(), 1, "a's entry left with it");
		let a = scheduler.domain(&Path::new("a"), &mut open).unwrap();
		assert_eq!(*a.get(), 3);
	}
}
