//! The SUBSCRIBE_ALL a session opens upstream for one prefix, feeding every listing of it.
//!
//! Every route the peer announces shares the session's lister, so readers ask the session
//! once per prefix, and the session answers with a single request: the peer lists
//! everything it holds there, whichever of its routes serves it.

use super::*;

use crate::coding::StreamCodes;

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
	broadcasts: HashMap<u64, Listed>,
	next_broadcast: u64,
	tracks: HashMap<u64, Option<track::Producer>>,
	/// The TRACK_STARTs read, which a group stream ahead of its own waits on.
	started: kio::Producer<u64>,
	/// Ended tracks waiting for their last group streams.
	settling: kio::Tasks<SettleTrack>,
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
struct Listed {
	/// `None` for one outside what the session may subscribe to: its tracks are ignored.
	source: Option<crate::broadcast::Producer>,
	epoch: Option<crate::Epoch>,
	/// Closes the source's serve machine once the peer ends the broadcast.
	_live: kio::Producer<()>,
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
			settling: kio::Tasks::new(),
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
			let _ = self.settling.poll(waiter);

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
					let defaults = Subscription::default();
					stream.writer.buffer(&lite::ControlType::SubscribeAll)?;
					stream.writer.buffer(&lite::SubscribeAll {
						id: self.id,
						prefix: self.prefix.as_path(),
						priority: defaults.priority,
						order: defaults.order,
						max_delay: defaults.max_delay,
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
				let live = kio::Producer::new(());
				let allowed = self
					.subscriber
					.auth
					.within_limit(crate::auth::Direction::Subscribe, path.as_str());
				let source = allowed.then(|| self.mint(&path, start.epoch.clone(), &live));
				if let Some(source) = &source {
					for output in &mut self.outputs {
						let key = output.listing.insert(source.consume(), start.epoch.clone());
						output.keys.insert(id, key);
					}
				}
				tracing::debug!(broadcast = %self.subscriber.log_path(&path), id, allowed, "listed broadcast");
				self.broadcasts.insert(
					id,
					Listed {
						source,
						epoch: start.epoch,
						_live: live,
					},
				);
			}
			lite::SubscribeAllResponse::BroadcastEnd(end) => {
				// Dropping it closes the source: no new tracks, while the listed ones run to
				// their own end.
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
				let producer = match &listed.source {
					Some(source) => self.create(source, id, &start)?,
					None => None,
				};
				self.tracks.insert(id, producer);
				// Registered first, so a group stream waiting on it finds the track.
				if let Ok(mut started) = self.started.write() {
					*started += 1;
				}
			}
			lite::SubscribeAllResponse::TrackOk(ok) => {
				let entry = self.tracks.get_mut(&ok.track).ok_or(Error::ProtocolViolation)?;
				if let Some(producer) = entry {
					producer.set_live(ok.start.largest);
					if let Some(largest) = ok.start.largest {
						producer.report_largest(largest);
					}
					let _ = producer.start_at(ok.start.group);
				}
			}
			lite::SubscribeAllResponse::TrackEnd(end) => {
				let entry = self.tracks.remove(&end.track).ok_or(Error::ProtocolViolation)?;
				let Some(mut producer) = entry else {
					return Ok(());
				};
				let feed = Feed {
					subscribe: self.id,
					track: Some(end.track),
				};
				// Lower groups may still be on the wire behind the end, so readers wait at a
				// hole until the tail settles. A boundary below a received group contradicts it.
				if producer.finish_at_pending(Position::group(end.end.group)).is_err() {
					return Err(Error::ProtocolViolation);
				}
				let Some(tail) = self
					.subscriber
					.subscribes
					.lock()
					.get(&feed)
					.map(|entry| entry.tail.clone())
				else {
					return Ok(());
				};
				if let Ok(mut tail) = tail.write() {
					tail.expire(self.subscriber.runtime.now());
				}
				self.settling.push(SettleTrack {
					settle: Settle::new(&self.subscriber.runtime, tail.consume()),
					streams: end.end.streams,
					producer,
					feed,
					subscribes: self.subscriber.subscribes.clone(),
				});
			}
			lite::SubscribeAllResponse::TrackReset(reset) => {
				let entry = self.tracks.remove(&reset.track).ok_or(Error::ProtocolViolation)?;
				if let Some(producer) = entry {
					let code = u32::try_from(reset.code).unwrap_or(u32::MAX);
					let err = Error::Stream(self.subscriber.version.decode_stream_code(code));
					let _ = producer.abort(err);
				}
				self.subscriber.subscribes.lock().remove(&Feed {
					subscribe: self.id,
					track: Some(reset.track),
				});
			}
		}
		Ok(())
	}

	/// A source for a listed broadcast, served like one minted for a request so the
	/// tracks the peer does not list can still be asked for.
	fn mint(
		&self,
		path: &PathOwned,
		epoch: Option<crate::Epoch>,
		live: &kio::Producer<()>,
	) -> crate::broadcast::Producer {
		let source = self.subscriber.origin.create_source(path);
		let dynamic = source.dynamic();
		let _ = self.subscriber.sources.try_push(MintedSource {
			validity: Default::default(),
			path: path.clone(),
			epoch,
			source: crate::model::broadcast::SourceGuard::new(source.clone()),
			dynamic,
			route: live.consume(),
		});
		source
	}

	/// Create a listed track in its broadcast's source and route its group streams to it.
	fn create(
		&self,
		source: &crate::broadcast::Producer,
		id: u64,
		start: &lite::TrackStart<'_>,
	) -> Result<Option<track::Producer>, Error> {
		let info = track::Info::default()
			.with_timescale(start.info.timescale)
			.with_max_age(start.info.max_age)
			.with_priority(start.info.priority);
		let producer = match source.create_track(start.name.as_ref(), info) {
			Ok(producer) => producer,
			// A reader asked for the track before the peer listed it, and its own
			// subscription feeds it: list that one, and let this feed's groups go.
			Err(Error::Duplicate) => {
				source.list_track(&start.name);
				return Ok(None);
			}
			Err(err) => return Err(err),
		};
		let tail = kio::Producer::new(Tail::new(tail::grace(Subscription::default().max_delay)));
		self.subscriber.subscribes.lock().insert(
			Feed {
				subscribe: self.id,
				track: Some(id),
			},
			TrackEntry {
				producer: producer.clone(),
				timescale: start.info.timescale,
				tail,
			},
		);
		Ok(Some(producer))
	}

	/// Stop feeding everything: the request is over.
	fn end(&mut self, err: &Error) {
		let mut subscribes = self.subscriber.subscribes.lock();
		for (id, producer) in self.tracks.drain() {
			subscribes.remove(&Feed {
				subscribe: self.id,
				track: Some(id),
			});
			if let Some(producer) = producer {
				let _ = producer.abort(err.clone());
			}
		}
		drop(subscribes);
		self.broadcasts.clear();
		self.outputs.clear();
		self.input.close();
	}
}

impl<S: crate::transport::poll::Session> kio::Task for ListServe<S> {
	type Output = ();

	fn poll(&mut self, waiter: &kio::Waiter) -> Poll<()> {
		let res = ready!(self.poll_serve(waiter));
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
		// Tracks that ended before the request did still settle.
		self.settling.poll(waiter)
	}
}

impl<S: crate::transport::poll::Session> Drop for ListServe<S> {
	fn drop(&mut self) {
		self.subscriber.started.lock().remove(&self.id);
	}
}

/// A listed track the peer ended, waiting for its last group streams.
struct SettleTrack {
	settle: Settle,
	streams: u64,
	producer: track::Producer,
	feed: Feed,
	subscribes: Lock<HashMap<Feed, TrackEntry>>,
}

impl kio::Task for SettleTrack {
	type Output = ();

	fn poll(&mut self, waiter: &kio::Waiter) -> Poll<()> {
		let streams = self.streams;
		ready!(self.settle.poll(waiter, |tail| tail.streams() >= streams));
		self.subscribes.lock().remove(&self.feed);
		self.producer.set_tail_pending(false);
		let _ = self.producer.finish();
		Poll::Ready(())
	}
}
