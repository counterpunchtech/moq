//! The SUBSCRIBE_ALL a session opens upstream for one prefix, feeding every listing of it.
//!
//! Every route the peer announces shares the session's lister, so readers ask the session
//! once per prefix, and the session answers with a single request: the peer lists
//! everything it holds there, whichever of its routes serves it.
//!
//! A listed broadcast resolves to the session's copy of it, the one a request the origin
//! routes here gets too, and each listed track is served by the same serve loop as a
//! requested one. The feed carries the track from its live edge, so the loop subscribes
//! upstream only for a floor a reader asks for below it; a track a request already serves
//! hands its subscription over to the feed instead. The request carries the aggregate of
//! what its tracks' readers ask for, updated as that moves.

use super::*;

use crate::coding::StreamCodes;

/// The request-wide preferences a SUBSCRIBE_ALL and its updates carry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Prefs {
	priority: u8,
	order: crate::track::Order,
	max_delay: Duration,
}

impl Prefs {
	fn of(subscription: &Subscription) -> Self {
		Self {
			priority: subscription.priority,
			order: subscription.order,
			max_delay: subscription.max_delay,
		}
	}
}

impl Default for Prefs {
	fn default() -> Self {
		Self::of(&Subscription::default())
	}
}

/// What the readers of one request's tracks ask for, counted per track, so the request
/// carries their aggregate the way one track's subscribers combine: the highest priority,
/// the longest max delay, and ascending order only when every track asks for it.
#[derive(Default)]
pub(super) struct Demands {
	priority: BTreeMap<u8, usize>,
	max_delay: BTreeMap<Duration, usize>,
	tracks: usize,
	descending: usize,
}

impl Demands {
	fn add(&mut self, prefs: Prefs) {
		*self.priority.entry(prefs.priority).or_default() += 1;
		*self.max_delay.entry(prefs.max_delay).or_default() += 1;
		self.tracks += 1;
		if prefs.order == crate::track::Order::Descending {
			self.descending += 1;
		}
	}

	fn remove(&mut self, prefs: Prefs) {
		fn take<K: Ord>(counts: &mut BTreeMap<K, usize>, key: K) {
			if let std::collections::btree_map::Entry::Occupied(mut entry) = counts.entry(key) {
				*entry.get_mut() -= 1;
				if *entry.get() == 0 {
					entry.remove();
				}
			}
		}
		take(&mut self.priority, prefs.priority);
		take(&mut self.max_delay, prefs.max_delay);
		self.tracks -= 1;
		if prefs.order == crate::track::Order::Descending {
			self.descending -= 1;
		}
	}

	fn aggregate(&self) -> Prefs {
		let none = Prefs::default();
		Prefs {
			priority: self.priority.keys().next_back().copied().unwrap_or(none.priority),
			max_delay: self.max_delay.keys().next_back().copied().unwrap_or(none.max_delay),
			order: match (self.tracks, self.descending) {
				(0, _) => none.order,
				(_, 0) => crate::track::Order::Ascending,
				_ => crate::track::Order::Descending,
			},
		}
	}
}

/// A listed track's TRACK_OK, TRACK_END or TRACK_RESET, for its serve loop.
pub(super) enum ListedResponse {
	Ok(lite::SubscribeStart),
	End(lite::SubscribeEnd),
	Reset(Error),
}

/// One track's feed from a SUBSCRIBE_ALL, as its serve loop holds it.
///
/// Dropping it stops routing the feed's group streams to the track and takes its readers
/// out of the request's aggregate.
pub(super) struct ListedFeed {
	/// The track's responses in wire order, closed once the request stops feeding it.
	pub(super) responses: kio::Queue<ListedResponse>,
	/// The feed's group streams, counted against its end.
	pub(super) tail: kio::Producer<Tail>,
	feed: Feed,
	subscribes: Lock<HashMap<Feed, TrackEntry>>,
	demands: kio::Producer<Demands>,
	counted: Option<Prefs>,
}

impl ListedFeed {
	/// Count what the track's readers ask for, `None` for no reader, in the request's
	/// aggregate.
	pub(super) fn count(&mut self, subscription: Option<&Subscription>) {
		let prefs = subscription.map(Prefs::of);
		if prefs == self.counted {
			return;
		}
		if let Ok(mut demands) = self.demands.write() {
			if let Some(counted) = self.counted {
				demands.remove(counted);
			}
			if let Some(prefs) = prefs {
				demands.add(prefs);
			}
		}
		self.counted = prefs;
	}
}

impl Drop for ListedFeed {
	fn drop(&mut self) {
		self.count(None);
		self.subscribes.lock().remove(&self.feed);
	}
}

/// One upstream SUBSCRIBE_ALL and the listings it fills.
pub(super) struct ListServe<S: crate::transport::poll::Session> {
	subscriber: Subscriber<S>,
	/// The prefix as the wire names it, relative to the session's root.
	prefix: PathOwned,
	/// Further listings of the same prefix, joining this request.
	input: kio::Queue<crate::model::Listing>,
	outputs: Vec<Output>,
	id: u64,
	state: ListState<S>,
	broadcasts: HashMap<u64, ListedBroadcast>,
	next_broadcast: u64,
	/// Each listed track's responses, `None` for one this request does not feed.
	tracks: HashMap<u64, Option<kio::Queue<ListedResponse>>>,
	/// The TRACK_STARTs read, which a group stream ahead of its own waits on.
	started: kio::Producer<u64>,
	/// The serve loops of the tracks this request created, which outlive it while readers
	/// hold them.
	serving: kio::Tasks<TrackServeRun<S>>,
	demands: kio::Producer<Demands>,
	demanded: kio::Consumer<Demands>,
	/// The preferences the request carries now.
	sent: Prefs,
	// Dedicated handles for opening the stream and watching the session close.
	session: S,
	closed: S,
}

/// One listing this request fills, and the key of each broadcast it holds there.
struct Output {
	listing: crate::model::Listing,
	keys: HashMap<u64, u64>,
}

/// A broadcast the peer listed.
struct ListedBroadcast {
	/// The session's copy, `None` for one outside what the session may subscribe to: its
	/// tracks are ignored.
	source: Option<crate::broadcast::Producer>,
	path: PathOwned,
	epoch: Option<crate::Epoch>,
	/// Closes the copy this listing minted once the peer ends the broadcast.
	_live: Option<kio::Producer<()>>,
}

enum ListState<S: crate::transport::poll::Session> {
	Open,
	Send {
		stream: Stream<S, Version>,
	},
	Run {
		stream: Stream<S, Version>,
	},
	/// Cancelled: FIN and wait for the acknowledgement.
	Finish {
		writer: crate::coding::Writer<S::SendStream, Version>,
	},
	Done,
}

impl<S: crate::transport::poll::Session> ListServe<S> {
	pub(super) fn new(
		subscriber: Subscriber<S>,
		prefix: PathOwned,
		input: kio::Queue<crate::model::Listing>,
		listing: crate::model::Listing,
	) -> Self {
		let id = subscriber.next_id.fetch_add(1, atomic::Ordering::Relaxed);
		let started = kio::Producer::new(0);
		subscriber.started.lock().insert(id, started.consume());
		let session = subscriber.session.clone();
		let closed = subscriber.session.clone();
		let demands = kio::Producer::<Demands>::default();
		Self {
			session,
			subscriber,
			prefix,
			input,
			outputs: vec![Output {
				listing,
				keys: HashMap::new(),
			}],
			id,
			state: ListState::Open,
			broadcasts: HashMap::new(),
			next_broadcast: 0,
			tracks: HashMap::new(),
			started,
			serving: kio::Tasks::new(),
			demanded: demands.consume(),
			demands,
			sent: Prefs::default(),
			closed,
		}
	}

	/// Fill another listing of the prefix with every broadcast listed so far.
	fn attach(&mut self, listing: crate::model::Listing) {
		let mut keys = HashMap::new();
		for (id, listed) in &self.broadcasts {
			if let Some(source) = &listed.source {
				keys.insert(*id, listing.insert(source.consume(), listed.epoch.clone()));
			}
		}
		self.outputs.push(Output { listing, keys });
	}

	/// Whether no reader is left on any listing, dropping the ones nobody reads.
	fn poll_unused(&mut self, waiter: &kio::Waiter) -> bool {
		self.outputs
			.retain(|output| output.listing.poll_unused(waiter).is_pending());
		self.outputs.is_empty()
	}

	fn poll_serve(&mut self, waiter: &kio::Waiter) -> Poll<Result<(), Error>> {
		let mut cx = waiter.context();
		loop {
			// The session's end aborts the tracks with its own error.
			if self.closed.poll_closed(&mut cx).is_ready() {
				return Poll::Ready(Ok(()));
			}
			while let Ok(Some(listing)) = self.input.try_pop() {
				self.attach(listing);
			}
			let _ = self.serving.poll(waiter);

			// Nobody reads any listing: cancel, unless one joins in the meantime.
			if !matches!(self.state, ListState::Finish { .. } | ListState::Done) && self.poll_unused(waiter) {
				self.input.close();
				while let Ok(Some(listing)) = self.input.try_pop() {
					self.attach(listing);
				}
				if self.outputs.is_empty() {
					tracing::debug!(prefix = %self.subscriber.log_path(&self.prefix), "subscribe all canceled (unused)");
					self.end(&Error::Cancel);
					match std::mem::replace(&mut self.state, ListState::Done) {
						ListState::Send { mut stream } | ListState::Run { mut stream } => {
							if stream.writer.finish().is_ok() {
								self.state = ListState::Finish { writer: stream.writer };
							}
						}
						ListState::Open | ListState::Finish { .. } | ListState::Done => {}
					}
					continue;
				}
			}

			match &mut self.state {
				ListState::Open => {
					let mut stream = ready!(Stream::poll_open(&mut self.session, self.subscriber.version, &mut cx))?;
					self.sent = self.demands.read().aggregate();
					stream.writer.buffer(&lite::ControlType::SubscribeAll)?;
					stream.writer.buffer(&lite::SubscribeAll {
						id: self.id,
						prefix: self.prefix.as_path(),
						priority: self.sent.priority,
						order: self.sent.order,
						max_delay: self.sent.max_delay,
					})?;
					tracing::info!(id = self.id, prefix = %self.subscriber.log_path(&self.prefix), "subscribe all started");
					self.state = ListState::Send { stream };
				}
				ListState::Send { stream } => {
					ready!(stream.writer.poll_flush(&mut cx))?;
					let ListState::Send { stream } = std::mem::replace(&mut self.state, ListState::Done) else {
						unreachable!()
					};
					self.state = ListState::Run { stream };
				}
				ListState::Run { stream } => {
					// What the tracks' readers ask for moved: carry it to every track at once.
					let sent = self.sent;
					let moved = match self.demanded.poll(waiter, |demands| match demands.aggregate() {
						prefs if prefs != sent => Poll::Ready(prefs),
						_ => Poll::Pending,
					}) {
						Poll::Ready(Ok(prefs)) => Some(prefs),
						_ => None,
					};
					if let Some(prefs) = moved {
						stream.writer.buffer(&lite::SubscribeAllUpdate {
							priority: prefs.priority,
							order: prefs.order,
							max_delay: prefs.max_delay,
						})?;
						self.sent = prefs;
					}
					if let Poll::Ready(Err(err)) = stream.writer.poll_flush(&mut cx) {
						return Poll::Ready(Err(err));
					}
					match ready!(stream.reader.poll_decode_maybe::<lite::SubscribeAllResponse>(&mut cx)) {
						Ok(Some(msg)) => self.handle(msg)?,
						// The publisher ended the request: its origin closed.
						Ok(None) => {
							self.end(&Error::Closed);
							return Poll::Ready(Ok(()));
						}
						Err(err) => return Poll::Ready(Err(err)),
					}
				}
				ListState::Finish { writer } => {
					let _ = ready!(writer.poll_close(&mut cx));
					self.state = ListState::Done;
				}
				ListState::Done => return Poll::Ready(Ok(())),
			}
		}
	}

	fn handle(&mut self, msg: lite::SubscribeAllResponse<'_>) -> Result<(), Error> {
		match msg {
			lite::SubscribeAllResponse::BroadcastStart(start) => {
				let id = self.next_broadcast;
				self.next_broadcast += 1;
				let path = self.prefix.join(&start.suffix).to_owned();
				let allowed = self
					.subscriber
					.auth
					.within_limit(crate::auth::Direction::Subscribe, path.as_str());
				let (source, live) = match allowed {
					true => {
						let (source, live) = self.resolve(&path, start.epoch.as_ref());
						(Some(source), live)
					}
					false => (None, None),
				};
				if let Some(source) = &source {
					for output in &mut self.outputs {
						let key = output.listing.insert(source.consume(), start.epoch.clone());
						output.keys.insert(id, key);
					}
				}
				tracing::debug!(broadcast = %self.subscriber.log_path(&path), id, allowed, "listed broadcast");
				self.broadcasts.insert(
					id,
					ListedBroadcast {
						source,
						path,
						epoch: start.epoch,
						_live: live,
					},
				);
			}
			lite::SubscribeAllResponse::BroadcastEnd(end) => {
				// Dropping it closes the copy it minted: no new tracks, while the listed ones
				// run to their own end.
				self.broadcasts.remove(&end.broadcast).ok_or(Error::ProtocolViolation)?;
				for output in &mut self.outputs {
					if let Some(key) = output.keys.remove(&end.broadcast) {
						output.listing.remove(key);
					}
				}
			}
			lite::SubscribeAllResponse::TrackStart(start) => {
				let id = *self.started.read();
				let listed = self.broadcasts.get(&start.broadcast).ok_or(Error::ProtocolViolation)?;
				let responses = match &listed.source {
					Some(source) => {
						let (source, path, epoch) = (source.clone(), listed.path.clone(), listed.epoch.clone());
						self.create(&source, path, epoch, id, &start)
					}
					None => None,
				};
				self.tracks.insert(id, responses);
				// Registered first, so a group stream waiting on it finds the track.
				if let Ok(mut started) = self.started.write() {
					*started += 1;
				}
			}
			lite::SubscribeAllResponse::TrackOk(ok) => {
				let responses = self.tracks.get(&ok.track).ok_or(Error::ProtocolViolation)?;
				if let Some(responses) = responses {
					let _ = responses.try_push(ListedResponse::Ok(ok.start));
				}
			}
			lite::SubscribeAllResponse::TrackEnd(end) => {
				let responses = self.tracks.remove(&end.track).ok_or(Error::ProtocolViolation)?;
				if let Some(responses) = responses {
					let _ = responses.try_push(ListedResponse::End(end.end));
					responses.close();
				}
			}
			lite::SubscribeAllResponse::TrackReset(reset) => {
				let responses = self.tracks.remove(&reset.track).ok_or(Error::ProtocolViolation)?;
				if let Some(responses) = responses {
					let code = u32::try_from(reset.code).unwrap_or(u32::MAX);
					let err = Error::Stream(self.subscriber.version.decode_stream_code(code));
					let _ = responses.try_push(ListedResponse::Reset(err));
					responses.close();
				}
			}
		}
		Ok(())
	}

	/// The session's copy of a listed broadcast: the one a request already resolved for
	/// the same publisher instance, or a new source served like one minted for a request,
	/// so the tracks the peer does not list can still be asked for. A minted copy closes
	/// with the returned handle, once the peer ends the broadcast.
	fn resolve(
		&self,
		path: &PathOwned,
		epoch: Option<&crate::Epoch>,
	) -> (crate::broadcast::Producer, Option<kio::Producer<()>>) {
		if let Some(copy) = self.subscriber.copy(path, epoch) {
			return (copy, None);
		}
		let live = kio::Producer::new(());
		let source = self.subscriber.origin.create_source(path);
		self.subscriber.hold_copy(path, epoch, &source);
		let dynamic = source.dynamic();
		let _ = self.subscriber.sources.try_push(MintedSource {
			validity: Default::default(),
			path: path.clone(),
			epoch: epoch.cloned(),
			source: crate::model::broadcast::SourceGuard::new(source.clone()),
			dynamic,
			route: live.consume(),
		});
		(source, Some(live))
	}

	/// Feed a listed track: create it in the broadcast's copy with a serve loop of its own,
	/// or hand the feed to the loop already serving it. Returns where its responses go,
	/// `None` when nothing reads this feed.
	fn create(
		&mut self,
		source: &crate::broadcast::Producer,
		path: PathOwned,
		epoch: Option<crate::Epoch>,
		id: u64,
		start: &lite::TrackStart<'_>,
	) -> Option<kio::Queue<ListedResponse>> {
		let feed = Feed {
			subscribe: self.id,
			track: Some(id),
		};
		let tail = kio::Producer::new(Tail::new(tail::grace(self.sent.max_delay)));
		let responses = kio::Queue::new();
		let listed = ListedFeed {
			responses: responses.clone(),
			tail: tail.clone(),
			feed,
			subscribes: self.subscriber.subscribes.clone(),
			demands: self.demands.clone(),
			counted: None,
		};
		let entry = |producer: &track::Producer| TrackEntry {
			producer: producer.clone(),
			timescale: start.info.timescale,
			tail: tail.clone(),
		};
		let info = track::Info::default()
			.with_timescale(start.info.timescale)
			.with_max_age(start.info.max_age)
			.with_priority(start.info.priority);
		match source.create_track(start.name.as_ref(), info) {
			Ok(mut producer) => {
				// Readers wait for TRACK_OK to say where the feed starts, as they wait for
				// SUBSCRIBE_START on lite-07.
				let _ = producer.request_start(None);
				self.subscriber.subscribes.lock().insert(feed, entry(&producer));
				let serve = TrackServe {
					validity: Default::default(),
					subscriber: self.subscriber.clone(),
					path,
					epoch,
					name: start.name.to_string(),
				};
				self.serving
					.push(TrackServeRun::listed(serve, producer, start.info.timescale, listed));
			}
			// A request got there first: feed its copy, whose subscription hands over to the
			// feed. One still asking the peer about the track keeps its own subscription.
			Err(Error::Duplicate) => {
				source.list_track(&start.name);
				let key = (path, Arc::from(start.name.as_ref()));
				let served = self
					.subscriber
					.served
					.lock()
					.get(&key)
					.and_then(|loops| loops.iter().find(|served| source.serves(&served.producer)).cloned())?;
				self.subscriber.subscribes.lock().insert(feed, entry(&served.producer));
				served.attach.try_push(listed).ok()?;
			}
			// The copy closed: it takes no new tracks.
			Err(err) => {
				tracing::debug!(track = %start.name, %err, "listed track not created");
				return None;
			}
		}
		Some(responses)
	}

	/// Stop feeding everything: the request is over. A track readers still hold carries on
	/// as a requested one when the request was only cancelled, and ends with it otherwise.
	fn end(&mut self, err: &Error) {
		for (_, responses) in self.tracks.drain() {
			let Some(responses) = responses else {
				continue;
			};
			if !matches!(err, Error::Cancel) {
				let _ = responses.try_push(ListedResponse::Reset(err.clone()));
			}
			responses.close();
		}
		self.subscriber.started.lock().remove(&self.id);
		let _ = self.started.close();
		self.broadcasts.clear();
		self.outputs.clear();
		self.input.close();
	}
}

impl<S: crate::transport::poll::Session> kio::Task for ListServe<S> {
	type Output = ();

	fn poll(&mut self, waiter: &kio::Waiter) -> Poll<()> {
		let Poll::Ready(res) = self.poll_serve(waiter) else {
			let _ = self.serving.poll(waiter);
			return Poll::Pending;
		};
		if let Err(err) = res {
			let err = lite::error::control(&mut self.subscriber.session.clone(), err);
			tracing::debug!(prefix = %self.subscriber.log_path(&self.prefix), %err, "subscribe all error");
			self.end(&err);
			if let ListState::Send { stream } | ListState::Run { stream } =
				std::mem::replace(&mut self.state, ListState::Done)
			{
				stream.writer.abort(&err);
			}
		}
		// The tracks readers still hold outlive the request.
		self.serving.poll(waiter)
	}
}

impl<S: crate::transport::poll::Session> Drop for ListServe<S> {
	fn drop(&mut self) {
		self.subscriber.started.lock().remove(&self.id);
	}
}
