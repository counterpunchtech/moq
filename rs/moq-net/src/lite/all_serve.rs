//! Serves a SUBSCRIBE_ALL: every track of every broadcast under a prefix, on one stream.
//!
//! Each broadcast and each track is its own task, so a wake costs the track that woke
//! rather than every one the request carries. They write their responses to a queue the
//! stream drains in order, which is what keeps the implicit Broadcast and Track IDs in
//! step with the wire.

use super::*;

use crate::coding::StreamCodes;

/// The request-wide delivery preferences a SUBSCRIBE_ALL_UPDATE moves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AllSteer {
	priority: u8,
	order: Order,
	max_delay: Duration,
}

/// What every broadcast and track of one request shares.
struct AllCtx<S: crate::transport::poll::Session> {
	shared: Arc<Shared<S>>,
	/// The SUBSCRIBE_ALL's id, named by every group stream it opens.
	id: u64,
	/// Responses in wire order, drained onto the stream.
	out: kio::Queue<lite::SubscribeAllResponse<'static>>,
	/// Assigned as each TRACK_START is queued, which is the order the peer counts them.
	next_track: AtomicU64,
	steer: kio::Consumer<AllSteer>,
}

impl<S: crate::transport::poll::Session> AllCtx<S> {
	fn send(&self, msg: lite::SubscribeAllResponse<'static>) {
		// Closed only once the request is over, when nobody reads it.
		let _ = self.out.try_push(msg);
	}
}

/// Answers a Subscribe All Stream until the requester closes it.
pub(super) struct AllServe<S: crate::transport::poll::Session> {
	shared: Arc<Shared<S>>,
	// Taken to abort.
	stream: Option<Stream<S, Version>>,
	state: AllState<S>,
	// Counted in `Shared::owed` until the stream ends, so a draining close waits for it.
	owing: bool,
	// One place under the session's subscription cap, whatever the request carries.
	_slot: Option<crate::session::Slot>,
	gate: Option<crate::auth::Gate>,
	// Log context, filled in after the decode.
	prefix: crate::PathOwned,
	// The requester's update failed to decode: the peer's fault, unlike a failed reply.
	malformed: bool,
}

enum AllState<S: crate::transport::poll::Session> {
	Decode,
	/// Waiting on the split-horizon origin, which waits on the peer's SETUP.
	Resolve {
		msg: lite::SubscribeAll<'static>,
	},
	Run(Box<AllRun<S>>),
	/// FIN and wait for the acknowledgement.
	Finish {
		finished: bool,
	},
}

impl<S: crate::transport::poll::Session> AllServe<S> {
	pub(super) fn new(shared: Arc<Shared<S>>, stream: Stream<S, Version>) -> Self {
		shared.owed.fetch_add(1, Ordering::Relaxed);
		Self {
			shared,
			stream: Some(stream),
			state: AllState::Decode,
			owing: true,
			_slot: None,
			gate: None,
			prefix: Default::default(),
			malformed: false,
		}
	}

	pub(super) fn poll(&mut self, waiter: &kio::Waiter) -> Poll<Result<(), Error>> {
		match ready!(self.poll_serve(waiter)) {
			Ok(()) => {
				tracing::info!(prefix = %self.prefix, "subscribe all complete");
				Poll::Ready(Ok(()))
			}
			// The peer's own malformed bytes are a control violation.
			Err(err) if matches!(self.state, AllState::Decode) || self.malformed => Poll::Ready(Err(err)),
			Err(err) => {
				match &err {
					err if is_cancel(err) => tracing::info!(prefix = %self.prefix, "subscribe all cancelled"),
					err => tracing::warn!(prefix = %self.prefix, %err, "subscribe all error"),
				}
				self.stream.take().expect("stream present").writer.abort(&err);
				Poll::Ready(Ok(()))
			}
		}
	}

	fn poll_serve(&mut self, waiter: &kio::Waiter) -> Poll<Result<(), Error>> {
		if let Some(gate) = &mut self.gate
			&& gate.poll_denied(waiter).is_ready()
		{
			return Poll::Ready(Err(Error::Unauthorized));
		}
		let mut cx = waiter.context();
		loop {
			if matches!(self.state, AllState::Resolve { .. } | AllState::Run(_))
				&& let Poll::Ready(res) = self.poll_requester(&mut cx)
			{
				// A FIN cancels the request: everything it carries ends with it.
				res?;
				self.state = AllState::Finish { finished: false };
			}

			let stream = self.stream.as_mut().expect("stream present");
			match &mut self.state {
				AllState::Decode => {
					let msg = ready!(stream.reader.poll_decode::<lite::SubscribeAll>(&mut cx))?;
					self.prefix = self.shared.origin.absolute(&msg.prefix).to_owned();
					match self.shared.interests.slots.acquire() {
						Ok(slot) => self._slot = Some(slot),
						// A peer past its limits loses the session, not just this request.
						Err(err) => {
							self.shared.session.clone().close(
								crate::lite::error::to_session_code(&crate::SessionError::from(&err)),
								"too many subscriptions",
							);
							return Poll::Ready(Err(err));
						}
					}
					tracing::info!(prefix = %self.prefix, request = ?msg, "subscribe all started");
					let mut gate = self.shared.gate(&msg.prefix);
					let denied = gate.poll_denied(waiter).is_ready();
					self.gate = Some(gate);
					self.state = AllState::Resolve { msg };
					if denied {
						return Poll::Ready(Err(Error::Unauthorized));
					}
				}
				AllState::Resolve { msg } => {
					let origin = ready!(self.shared.poll_serving_origin(waiter));
					let broadcasts = origin.broadcasts(&msg.prefix)?;
					let steer = AllSteer {
						priority: msg.priority,
						order: msg.order,
						max_delay: serving_max_delay(self.shared.version, msg.max_delay),
					};
					let run = AllRun::new(self.shared.clone(), msg, broadcasts, steer);
					self.state = AllState::Run(Box::new(run));
				}
				AllState::Run(run) => {
					ready!(run.poll(&mut stream.writer, waiter))?;
					self.state = AllState::Finish { finished: false };
				}
				AllState::Finish { finished } => {
					if !*finished {
						ready!(stream.writer.poll_flush(&mut cx))?;
						stream.writer.finish()?;
						*finished = true;
					}
					let res = ready!(stream.writer.poll_close(&mut cx));
					self.settle();
					return Poll::Ready(res);
				}
			}
		}
	}

	/// Read what the requester sends: ready once it closes its send direction, `Ok` for a
	/// FIN. Each update reaches every track.
	fn poll_requester(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
		let stream = self.stream.as_mut().expect("stream present");
		loop {
			let update = match ready!(stream.reader.poll_decode_maybe::<lite::SubscribeAllUpdate>(cx)) {
				Ok(Some(update)) => update,
				Ok(None) => return Poll::Ready(Ok(())),
				Err(err) => {
					self.malformed = matches!(err, Error::Decode(_));
					return Poll::Ready(Err(err));
				}
			};
			match &mut self.state {
				AllState::Run(run) => run.update(update, self.shared.version),
				// Not started yet: the request's own preferences become the update's.
				AllState::Resolve { msg } => {
					msg.priority = update.priority;
					msg.order = update.order;
					msg.max_delay = update.max_delay;
				}
				AllState::Decode | AllState::Finish { .. } => {}
			}
		}
	}

	/// The request no longer owes the peer anything a draining close should wait for.
	fn settle(&mut self) {
		if std::mem::take(&mut self.owing) {
			self.shared.owed.fetch_sub(1, Ordering::Relaxed);
		}
	}
}

impl<S: crate::transport::poll::Session> Drop for AllServe<S> {
	fn drop(&mut self) {
		self.settle();
	}
}

/// The running request: lists each broadcast as it appears and drains the responses.
struct AllRun<S: crate::transport::poll::Session> {
	ctx: Arc<AllCtx<S>>,
	prefix: crate::PathOwned,
	broadcasts: origin::Broadcasts,
	// The origin closed: no broadcast will appear.
	listed_all: bool,
	steer: kio::Producer<AllSteer>,
	next_broadcast: u64,
	children: kio::Tasks<ListedBroadcast<S>>,
	// A request that always has another broadcast ready still lets the session's other tasks run.
	budget: kio::coop::Budget,
}

impl<S: crate::transport::poll::Session> AllRun<S> {
	fn new(
		shared: Arc<Shared<S>>,
		msg: &lite::SubscribeAll<'static>,
		broadcasts: origin::Broadcasts,
		steer: AllSteer,
	) -> Self {
		let steer = kio::Producer::new(steer);
		Self {
			ctx: Arc::new(AllCtx {
				shared,
				id: msg.id,
				out: kio::Queue::new(),
				next_track: AtomicU64::new(0),
				steer: steer.consume(),
			}),
			prefix: msg.prefix.to_owned(),
			broadcasts,
			listed_all: false,
			steer,
			next_broadcast: 0,
			children: kio::Tasks::new(),
			budget: kio::coop::Budget::new(32),
		}
	}

	fn update(&mut self, update: lite::SubscribeAllUpdate, version: Version) {
		if let Ok(mut steer) = self.steer.write() {
			*steer = AllSteer {
				priority: update.priority,
				order: update.order,
				max_delay: serving_max_delay(version, update.max_delay),
			};
		}
	}

	/// Serve until the origin closes and every track it listed has ended.
	fn poll(&mut self, writer: &mut Writer<S::SendStream, Version>, waiter: &kio::Waiter) -> Poll<Result<(), Error>> {
		let mut cx = waiter.context();
		loop {
			ready!(self.budget.poll_yield(waiter));
			ready!(writer.poll_flush(&mut cx))?;

			let mut wrote = false;
			while let Ok(Some(msg)) = self.ctx.out.try_pop() {
				writer.buffer(&msg)?;
				wrote = true;
			}
			if wrote {
				continue;
			}

			let _ = self.children.poll(waiter);
			if !self.ctx.out.is_empty() {
				continue;
			}

			if !self.listed_all {
				match self.broadcasts.poll_next(waiter) {
					Poll::Ready(Some(broadcast)) => {
						self.list(broadcast);
						continue;
					}
					Poll::Ready(None) => self.listed_all = true,
					Poll::Pending => {}
				}
			}

			if self.listed_all && self.children.is_empty() {
				return Poll::Ready(Ok(()));
			}
			return Poll::Pending;
		}
	}

	/// Announce a broadcast and start listing its tracks, unless the session does not allow it.
	fn list(&mut self, broadcast: crate::broadcast::Consumer) {
		let path = broadcast.info().path.clone();
		let Some(suffix) = path.strip_prefix(&self.prefix).map(|suffix| suffix.to_owned()) else {
			return;
		};
		let mut gate = self.ctx.shared.gate(&path);
		if gate.poll_denied(&kio::Waiter::noop()).is_ready() {
			tracing::debug!(broadcast = %path, "not listing a broadcast outside the grant");
			return;
		}
		let id = self.next_broadcast;
		self.next_broadcast += 1;
		self.ctx
			.send(lite::SubscribeAllResponse::BroadcastStart(lite::BroadcastStart {
				suffix,
				epoch: broadcast.info().epoch.clone(),
			}));
		tracing::debug!(broadcast = %path, id, "listing broadcast");
		self.children.push(ListedBroadcast {
			ctx: self.ctx.clone(),
			id,
			tracks: Some(broadcast.tracks()),
			gate,
			path,
			children: kio::Tasks::new(),
		});
	}
}

/// One listed broadcast: lists each track its producer creates until it ends.
struct ListedBroadcast<S: crate::transport::poll::Session> {
	ctx: Arc<AllCtx<S>>,
	id: u64,
	/// `None` once BROADCAST_END went out.
	tracks: Option<crate::broadcast::Tracks>,
	gate: crate::auth::Gate,
	path: crate::PathOwned,
	children: kio::Tasks<ListedTrack<S>>,
}

impl<S: crate::transport::poll::Session> kio::Task for ListedBroadcast<S> {
	type Output = ();

	fn poll(&mut self, waiter: &kio::Waiter) -> Poll<()> {
		let _ = self.children.poll(waiter);
		while let Some(tracks) = &mut self.tracks {
			// A broadcast the session stops allowing lists nothing more; each of its tracks
			// ends on its own gate.
			let ended = match self.gate.poll_denied(waiter).is_ready() {
				true => true,
				false => match tracks.poll_next(waiter) {
					Poll::Ready(Some(track)) => {
						self.children.push(ListedTrack {
							ctx: self.ctx.clone(),
							broadcast: self.id,
							gate: self.ctx.shared.gate(&self.path),
							steer: self.ctx.steer.clone(),
							seen: *self.ctx.steer.read(),
							state: ListedState::Query {
								querying: track.query().into_inner(),
								track,
							},
							id: None,
							budget: kio::coop::Budget::new(32),
						});
						continue;
					}
					Poll::Ready(None) => true,
					Poll::Pending => false,
				},
			};
			if !ended {
				break;
			}
			tracing::debug!(broadcast = %self.path, id = self.id, "broadcast ended");
			self.ctx
				.send(lite::SubscribeAllResponse::BroadcastEnd(lite::BroadcastEnd { broadcast: self.id }));
			self.tracks = None;
		}

		// Newly listed tracks start now rather than on the next wake.
		match self.children.poll(waiter) {
			Poll::Ready(()) if self.tracks.is_none() => Poll::Ready(()),
			_ => Poll::Pending,
		}
	}
}

/// One listed track: its TRACK_START, then a subscription served like a SUBSCRIBE.
struct ListedTrack<S: crate::transport::poll::Session> {
	ctx: Arc<AllCtx<S>>,
	broadcast: u64,
	gate: crate::auth::Gate,
	steer: kio::Consumer<AllSteer>,
	// The preferences this track last applied.
	seen: AllSteer,
	state: ListedState<S>,
	// The Track ID, once TRACK_START went out.
	id: Option<u64>,
	// A track that always has another group ready still lets its siblings run.
	budget: kio::coop::Budget,
}

// A state machine's enum is its storage: one transient instance per track, so the big
// variant is the working state, not padding held in bulk.
#[allow(clippy::large_enum_variant)]
enum ListedState<S: crate::transport::poll::Session> {
	/// Waiting for the track's immutable properties.
	Query {
		track: track::Consumer,
		querying: track::Querying,
	},
	/// Waiting for the model subscription to be confirmed.
	Confirm { subscribing: track::Subscribing },
	Run(Box<TrackRun<S>>),
	/// The track finished: draining the in-flight group streams.
	Drain {
		children: kio::Tasks<GroupServe<S>>,
		// Demand lasts until the groups drain; see `SubscribeServe::Drain`.
		_track: Box<track::Subscriber>,
	},
	/// Only in passing between states.
	Done,
}

impl<S: crate::transport::poll::Session> kio::Task for ListedTrack<S> {
	type Output = ();

	fn poll(&mut self, waiter: &kio::Waiter) -> Poll<()> {
		let Err(err) = ready!(self.poll_serve(waiter)) else {
			return Poll::Ready(());
		};
		// A track that never went out has nothing to reset.
		if let Some(track) = self.id {
			match &err {
				err if is_cancel(err) => tracing::debug!(track, "listed track cancelled"),
				err => tracing::warn!(track, %err, "listed track error"),
			}
			let code = self
				.ctx
				.shared
				.version
				.encode_stream_code(&crate::StreamError::from(&err));
			self.ctx
				.send(lite::SubscribeAllResponse::TrackReset(lite::TrackReset {
					track,
					code: code.into(),
				}));
		}
		Poll::Ready(())
	}
}

impl<S: crate::transport::poll::Session> ListedTrack<S> {
	fn poll_serve(&mut self, waiter: &kio::Waiter) -> Poll<Result<(), Error>> {
		if self.gate.poll_denied(waiter).is_ready() {
			return Poll::Ready(Err(Error::Unauthorized));
		}
		loop {
			ready!(self.budget.poll_yield(waiter));
			match &mut self.state {
				ListedState::Query { track, querying } => {
					let info = ready!(querying.poll_ok(waiter))?;
					let id = self.ctx.next_track.fetch_add(1, Ordering::Relaxed);
					self.id = Some(id);
					self.ctx
						.send(lite::SubscribeAllResponse::TrackStart(lite::TrackStart {
							broadcast: self.broadcast,
							name: track.name().to_string().into(),
							info: lite::TrackInfo {
								priority: info.priority,
								max_age: info.max_age,
								timescale: info.timescale,
							},
						}));
					self.seen = *self.steer.read();
					let subscription = crate::track::Subscription {
						priority: self.seen.priority,
						max_delay: self.seen.max_delay,
						order: self.seen.order,
						..Default::default()
					};
					let subscribing = track.subscribe(subscription).into_inner();
					self.state = ListedState::Confirm { subscribing };
				}
				ListedState::Confirm { subscribing } => {
					let track = ready!(subscribing.poll_ok(waiter))?;
					let id = self.id.expect("assigned with TRACK_START");
					let shared = &self.ctx.shared;
					let steer = Steer {
						priority: self.seen.priority,
						order: self.seen.order,
					};
					let steer_tx = kio::Producer::new(steer);
					let mut session = shared.session.clone();
					let group = session.send_group(self.seen.priority.into());
					let sub = Subscription {
						session,
						id: self.ctx.id,
						track_name: Arc::from(track.name()),
						priority: shared.priority.clone(),
						group,
						steer: steer_tx.consume(),
						steer_seen: steer,
						version: shared.version,
						timescale: shared.version.wire_timescale(track.info().timescale),
						runtime: shared.runtime.clone(),
						opens: Default::default(),
						track: Some(id),
					};
					let bounds = Bounds {
						start_group: None,
						start_frame: 0,
						end_group: None,
						end_frame: None,
					};
					self.state = ListedState::Run(Box::new(TrackRun::new(sub, track, bounds, steer_tx)));
				}
				ListedState::Run(run) => {
					let current = *self.steer.read();
					if current != self.seen {
						self.seen = current;
						run.update(lite::SubscribeUpdate {
							priority: current.priority,
							max_delay: current.max_delay,
							start_group: None,
							end_group: None,
							start_frame: 0,
							end_frame: None,
							order: current.order,
						});
					}
					// Wakes on the next update.
					let _ = self.steer.poll(waiter, |steer| match **steer != current {
						true => Poll::Ready(()),
						false => Poll::Pending,
					});

					let mut out = Responder {
						track: self.id.expect("assigned with TRACK_START"),
						out: &self.ctx.out,
					};
					if ready!(run.poll_step(&mut out, waiter))?.is_continue() {
						continue;
					}
					let ListedState::Run(run) = std::mem::replace(&mut self.state, ListedState::Done) else {
						unreachable!()
					};
					let TrackRun { children, track, .. } = *run;
					self.state = ListedState::Drain {
						children,
						_track: Box::new(track),
					};
				}
				ListedState::Drain { children, .. } => {
					ready!(children.poll(waiter));
					return Poll::Ready(Ok(()));
				}
				ListedState::Done => return Poll::Ready(Ok(())),
			}
		}
	}
}

/// A listed track's responses, tagged with its Track ID on the shared stream.
struct Responder<'a> {
	track: u64,
	out: &'a kio::Queue<lite::SubscribeAllResponse<'static>>,
}

impl Respond for Responder<'_> {
	fn poll_flush(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Error>> {
		// The request drains the queue onto its stream.
		Poll::Ready(Ok(()))
	}

	fn respond(&mut self, resp: lite::SubscribeResponse) -> Result<(), Error> {
		let track = self.track;
		let msg = match resp {
			lite::SubscribeResponse::Start(start) => lite::SubscribeAllResponse::TrackOk(lite::TrackOk { track, start }),
			lite::SubscribeResponse::End(end) => lite::SubscribeAllResponse::TrackEnd(lite::TrackEnd { track, end }),
			// Lite-07 has neither, and it is the only version with SUBSCRIBE_ALL.
			lite::SubscribeResponse::Ok(_) | lite::SubscribeResponse::Drop(_) => {
				return Err(crate::coding::EncodeError::Version.into());
			}
		};
		let _ = self.out.try_push(msg);
		Ok(())
	}
}
