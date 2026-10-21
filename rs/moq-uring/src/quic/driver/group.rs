//! Send groups: a shared handle around one of moq-quic's.

use std::rc::Rc;

use super::Shared;

/// A send group on a [`Connection`](super::Connection).
///
/// Streams that [join](super::SendStream::set_group) a group share their [`SendDomain`]'s
/// bytes with its other groups: a higher priority group preempts every lower one, and groups of
/// equal priority get an equal share of bytes however many streams each has queued. A stream's
/// own priority orders it only against the group's other streams, and a datagram sent in the
/// group is ordered among them by its priority. Streams and datagrams outside any group go
/// before every group. See [`moq_quic::SendGroups`].
///
/// Clones share one group, released once the last clone is dropped and its streams finish
/// and its datagrams are sent.
#[derive(Clone)]
pub struct SendGroup(Rc<Inner>);

/// How an outgoing datagram is scheduled: its [`SendGroup`] and priority.
pub type SendOptions<'a> = moq_quic::SendOptions<'a, SendGroup>;

/// The options as the connection knows them.
pub(crate) fn to_quic(options: SendOptions<'_>) -> moq_quic::SendOptions<'_> {
	moq_quic::SendOptions {
		group: options.group.map(SendGroup::id),
		priority: options.priority,
	}
}

struct Inner {
	shared: Shared,
	group: Option<moq_quic::SendGroup>,
}

impl SendGroup {
	pub(crate) fn new(shared: Shared, domain: Option<&SendDomain>, priority: i64) -> Self {
		let group = shared
			.conn
			.borrow_mut()
			.send_groups()
			.open(domain.map(SendDomain::id), priority);
		Self(Rc::new(Inner {
			shared,
			group: Some(group),
		}))
	}

	/// Change the group's priority, moving any data it has queued along with it.
	pub fn set_priority(&self, priority: i64) {
		self.0
			.shared
			.conn
			.borrow_mut()
			.send_groups()
			.set_priority(self.id(), priority);
	}

	/// The group's priority.
	pub fn priority(&self) -> i64 {
		self.0.shared.conn.borrow_mut().send_groups().priority(self.id())
	}

	/// The group as the connection knows it, which refuses it on any other connection.
	pub(crate) fn id(&self) -> &moq_quic::SendGroup {
		self.0.group.as_ref().expect("only taken on drop")
	}
}

impl Drop for Inner {
	fn drop(&mut self) {
		if let Some(group) = self.group.take() {
			self.shared.conn.borrow_mut().send_groups().close(group);
		}
	}
}

impl std::fmt::Debug for SendGroup {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("SendGroup").finish_non_exhaustive()
	}
}

/// A domain of send groups on a [`Connection`](super::Connection).
///
/// Domains share the bytes the ungrouped streams leave equally, however many groups each
/// holds, and the groups opened without one form the connection's default domain. A group's
/// priority only orders it against the other groups of its domain. See
/// [`moq_quic::SendGroups::open_domain`].
///
/// Clones share one domain, released once the last clone is dropped and its groups are.
#[derive(Clone)]
pub struct SendDomain(Rc<DomainInner>);

struct DomainInner {
	shared: Shared,
	domain: Option<moq_quic::SendDomain>,
}

impl SendDomain {
	pub(crate) fn new(shared: Shared) -> Self {
		let domain = shared.conn.borrow_mut().send_groups().open_domain();
		Self(Rc::new(DomainInner {
			shared,
			domain: Some(domain),
		}))
	}

	/// The domain as the connection knows it, which refuses it on any other connection.
	fn id(&self) -> &moq_quic::SendDomain {
		self.0.domain.as_ref().expect("only taken on drop")
	}
}

impl Drop for DomainInner {
	fn drop(&mut self) {
		if let Some(domain) = self.domain.take() {
			self.shared.conn.borrow_mut().send_groups().close_domain(domain);
		}
	}
}

impl std::fmt::Debug for SendDomain {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("SendDomain").finish_non_exhaustive()
	}
}
