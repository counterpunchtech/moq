use std::sync::Arc;

use crate::quic::driver::connection::ConnectionInner;

/// A send group on a [`Connection`](crate::quic::driver::Connection)
///
/// Streams that [join](crate::quic::driver::SendStream::set_group) a group share their
/// [`SendDomain`]'s bytes with its other groups: a higher priority group preempts every lower
/// one, and groups of equal priority get an equal share of bytes however many streams each has
/// queued. A
/// stream's own priority orders it only against the group's other streams, and a datagram sent
/// in the group is ordered among them by its priority. Streams and datagrams outside any group
/// go before every group. See [`moq_quic::SendGroups`].
///
/// Clones share one group, released once the last clone is dropped and its streams finish
/// and its datagrams are sent. A group does not keep its connection open.
#[derive(Debug, Clone)]
pub struct SendGroup(Arc<Inner>);

/// How an outgoing stream or datagram is scheduled: its [`SendGroup`] and priority
pub type SendOptions<'a> = moq_quic::SendOptions<'a, SendGroup>;

/// The options as the connection knows them.
pub(crate) fn to_quic(options: SendOptions<'_>) -> moq_quic::SendOptions<'_> {
    moq_quic::SendOptions {
        group: options.group.map(SendGroup::id),
        priority: options.priority,
    }
}

#[derive(Debug)]
struct Inner {
    conn: Arc<ConnectionInner>,
    group: Option<moq_quic::SendGroup>,
}

impl SendGroup {
    pub(crate) fn new(
        conn: Arc<ConnectionInner>,
        domain: Option<&SendDomain>,
        priority: i64,
    ) -> Self {
        let group = conn
            .state
            .lock("SendGroup::new")
            .inner
            .send_groups()
            .open(domain.map(SendDomain::id), priority);
        Self(Arc::new(Inner {
            conn,
            group: Some(group),
        }))
    }

    /// Change the group's priority, moving any data it has queued along with it
    pub fn set_priority(&self, priority: i64) {
        let mut conn = self.0.conn.state.lock("SendGroup::set_priority");
        conn.inner.send_groups().set_priority(self.id(), priority);
    }

    /// Get the group's priority
    pub fn priority(&self) -> i64 {
        let mut conn = self.0.conn.state.lock("SendGroup::priority");
        conn.inner.send_groups().priority(self.id())
    }

    /// The group as the connection knows it, which refuses it on any other connection.
    pub(crate) fn id(&self) -> &moq_quic::SendGroup {
        self.0.group.as_ref().expect("only taken on drop")
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(group) = self.group.take() {
            let mut conn = self.conn.state.lock("SendGroup::drop");
            conn.inner.send_groups().close(group);
        }
    }
}

/// A domain of send groups on a [`Connection`](crate::quic::driver::Connection)
///
/// Domains share the bytes the ungrouped streams leave equally, however many groups each
/// holds, and the groups opened without one form the connection's default domain. A group's
/// priority only orders it against the other groups of its domain. See
/// [`moq_quic::SendGroups::open_domain`].
///
/// Clones share one domain, released once the last clone is dropped and its groups are.
#[derive(Debug, Clone)]
pub struct SendDomain(Arc<DomainInner>);

#[derive(Debug)]
struct DomainInner {
    conn: Arc<ConnectionInner>,
    domain: Option<moq_quic::SendDomain>,
}

impl SendDomain {
    pub(crate) fn new(conn: Arc<ConnectionInner>) -> Self {
        let domain = conn
            .state
            .lock("SendDomain::new")
            .inner
            .send_groups()
            .open_domain();
        Self(Arc::new(DomainInner {
            conn,
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
            let mut conn = self.conn.state.lock("SendDomain::drop");
            conn.inner.send_groups().close_domain(domain);
        }
    }
}
