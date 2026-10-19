//! SUBSCRIBE_ALL through a relay: listers each open one `broadcasts(prefix)` that covers
//! every track of a publisher, over the in-memory mock transport.
//!
//! `subscribe_all_round` times one round: every track writes a group and every lister
//! reads it. `subscribe_round` is its control, each track requested with its own SUBSCRIBE.
//! `subscribe_all_new_broadcast` times one broadcast's life: created with one track,
//! listed and read by every lister, then dropped; `subscribe_new_broadcast` is its control. Each sweeps listers against the
//! broadcasts already listed, so a cost that grows with the table instead of the touched
//! track shows as a slope.
//!
//! Everything runs on one current-thread runtime, so the only work measured is the
//! protocol and model code, never scheduling across threads.
//!
//! Run with `cargo bench -p moq-net --bench subscribe-all`.

// Custom timings measure the real time the work took, not the ambient clock.
#![allow(clippy::disallowed_methods)]

#[path = "../tests/integration/support/mod.rs"]
mod support;

use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use moq_net::{Epoch, Hop, Timestamp, Version, broadcast, origin, track};
use support::harness::{MockConnectOptions, MockPair, connect_mock};

const VERSION: &str = "moq-lite-07-wip";

/// Tracks per broadcast, as a small rendition ladder plus audio.
const TRACKS: usize = 4;

/// Listers sharing the relay.
const LISTERS: [usize; 3] = [1, 8, 32];

/// Broadcasts already published under the prefix.
const BROADCASTS: [usize; 3] = [1, 16, 64];

fn runtime() -> tokio::runtime::Runtime {
	tokio::runtime::Builder::new_current_thread()
		.enable_time()
		.build()
		.unwrap()
}

fn timed() -> track::Info {
	track::Info::default().with_timescale(moq_net::Timescale::MILLI)
}

fn produce(hop: u64) -> origin::Producer {
	let (producer, driver) = origin::Producer::new(origin::Config::new(Hop::new(hop).unwrap()));
	support::harness::spawn(driver);
	producer
}

/// Have `to` pull everything `from` publishes.
async fn link(from: &origin::Producer, to: &origin::Producer) -> MockPair {
	let mut options = MockConnectOptions::new(VERSION.parse::<Version>().unwrap());
	options.server_publish = Some(from.consume());
	options.client_subscribe = Some(to.clone());
	connect_mock(options).await
}

fn write(track: &track::Producer, sequence: u64) {
	let mut group = track.create_group(moq_net::group::Info { sequence }).unwrap();
	group
		.write_frame(Timestamp::from_millis(sequence).unwrap(), &[0u8; 64][..])
		.unwrap();
	group.finish().unwrap();
}

/// Read the next group whole, returning its sequence.
async fn read(subscriber: &mut track::Subscriber) -> u64 {
	let mut group = subscriber.recv_group().await.unwrap().expect("track ended");
	while group.read_frame().await.unwrap().is_some() {}
	group.sequence
}

/// Announced with an epoch, so a relay's listing and its requests share one copy.
fn route() -> origin::Route {
	origin::Route::default().with_epoch(Epoch::mint())
}

fn subscription() -> track::Subscription {
	track::Subscription::default().with_max_delay(Duration::from_secs(60))
}

/// A lister's open request, with every listed track subscribed.
struct Lister {
	/// None for the control, which requests each track by name.
	cursor: Option<origin::Broadcasts>,
	subscribers: Vec<track::Subscriber>,
	origin: origin::Producer,
	_pair: MockPair,
}

/// A publisher with `broadcasts` broadcasts of [`TRACKS`] tracks, a relay, and `listers`
/// listers that have read group 0 of every track.
struct Room {
	publisher: origin::Producer,
	tracks: Vec<track::Producer>,
	listers: Vec<Lister>,
	_broadcasts: Vec<broadcast::Producer>,
	_relay: origin::Producer,
	_pair: MockPair,
	sequence: u64,
}

impl Room {
	/// `listed` subscribes through SUBSCRIBE_ALL, otherwise through a SUBSCRIBE per track.
	async fn new(broadcasts: usize, listers: usize, listed: bool) -> Self {
		let publisher = produce(1);
		let relay = produce(2);
		let pair = link(&publisher, &relay).await;

		let mut tracks = Vec::new();
		let mut held = Vec::new();
		for index in 0..broadcasts {
			let broadcast = publisher.create_broadcast(format!("room/{index}")).unwrap();
			for name in 0..TRACKS {
				let track = broadcast.create_track(name.to_string(), timed()).unwrap();
				write(&track, 0);
				tracks.push(track);
			}
			broadcast.announce(route()).unwrap();
			held.push(broadcast);
		}

		let mut room = Self {
			publisher,
			tracks,
			listers: Vec::new(),
			_broadcasts: held,
			_relay: relay.clone(),
			_pair: pair,
			sequence: 0,
		};
		for lister in 0..listers {
			let origin = produce(3 + lister as u64);
			let pair = link(&relay, &origin).await;
			let consumer = origin.consume();
			consumer
				.routed(format!("room/{}", broadcasts - 1))
				.await
				.expect("announced");
			let mut tracks = Vec::new();
			let cursor = match listed {
				true => {
					let mut cursor = consumer.broadcasts("room").unwrap();
					let mut listed = Vec::new();
					for _ in 0..broadcasts {
						listed.push(cursor.next().await.expect("listing ended").tracks());
					}
					for listed in &mut listed {
						for _ in 0..TRACKS {
							tracks.push(listed.next().await.expect("tracks ended"));
						}
					}
					Some(cursor)
				}
				false => {
					for index in 0..broadcasts {
						let broadcast = consumer.routed_broadcast(format!("room/{index}")).await.unwrap();
						for name in 0..TRACKS {
							tracks.push(broadcast.track(&name.to_string()).unwrap());
						}
					}
					None
				}
			};
			let mut subscribers = Vec::new();
			for track in tracks {
				let mut subscriber = track.subscribe(subscription()).await.unwrap();
				assert_eq!(read(&mut subscriber).await, 0);
				subscribers.push(subscriber);
			}
			room.listers.push(Lister {
				cursor,
				subscribers,
				origin,
				_pair: pair,
			});
		}
		room
	}

	/// Every track writes a group and every lister reads it.
	async fn round(&mut self) {
		self.sequence += 1;
		for track in &self.tracks {
			write(track, self.sequence);
		}
		for lister in &mut self.listers {
			for subscriber in &mut lister.subscribers {
				assert_eq!(read(subscriber).await, self.sequence);
			}
		}
	}

	/// A new broadcast with one track, listed and read by every lister, then dropped.
	async fn new_broadcast(&mut self, index: usize) {
		let broadcast = self.publisher.create_broadcast(format!("room/new{index}")).unwrap();
		let track = broadcast.create_track("0", timed()).unwrap();
		write(&track, 0);
		broadcast.announce(route()).unwrap();
		for lister in &mut self.listers {
			let consumer = match lister.cursor.as_mut() {
				Some(cursor) => {
					let listed = cursor.next().await.expect("listing ended");
					listed.tracks().next().await.expect("tracks ended")
				}
				None => {
					let path = format!("room/new{index}");
					let listed = lister.origin.consume().routed_broadcast(path).await.unwrap();
					listed.track("0").unwrap()
				}
			};
			let mut subscriber = consumer.subscribe(subscription()).await.unwrap();
			assert_eq!(read(&mut subscriber).await, 0);
		}
	}
}

fn round(c: &mut Criterion) {
	round_with(c, "subscribe_all_round", true);
	round_with(c, "subscribe_round", false);
}

fn round_with(c: &mut Criterion, name: &str, listed: bool) {
	let rt = runtime();
	let mut group = c.benchmark_group(name);
	for listers in LISTERS {
		for broadcasts in BROADCASTS {
			let mut room = rt.block_on(Room::new(broadcasts, listers, listed));
			group.throughput(Throughput::Elements((listers * broadcasts * TRACKS) as u64));
			group.bench_function(
				BenchmarkId::new(format!("listers={listers}"), format!("broadcasts={broadcasts}")),
				|b| {
					b.iter_custom(|iterations| {
						let start = Instant::now();
						for _ in 0..iterations {
							rt.block_on(room.round());
						}
						start.elapsed()
					})
				},
			);
		}
	}
	group.finish();
}

fn new_broadcast(c: &mut Criterion) {
	new_broadcast_with(c, "subscribe_all_new_broadcast", true);
	new_broadcast_with(c, "subscribe_new_broadcast", false);
}

fn new_broadcast_with(c: &mut Criterion, name: &str, listed: bool) {
	let rt = runtime();
	let mut group = c.benchmark_group(name);
	for listers in LISTERS {
		for broadcasts in BROADCASTS {
			let mut room = rt.block_on(Room::new(broadcasts, listers, listed));
			let mut next = 0;
			group.throughput(Throughput::Elements(listers as u64));
			group.bench_function(
				BenchmarkId::new(format!("listers={listers}"), format!("broadcasts={broadcasts}")),
				|b| {
					b.iter_custom(|iterations| {
						let start = Instant::now();
						for _ in 0..iterations {
							rt.block_on(room.new_broadcast(next));
							next += 1;
						}
						start.elapsed()
					})
				},
			);
		}
	}
	group.finish();
}

criterion_group!(benches, round, new_broadcast);
criterion_main!(benches);
