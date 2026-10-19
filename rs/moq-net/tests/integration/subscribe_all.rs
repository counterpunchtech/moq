#![cfg(feature = "async")]

//! moq-lite-07's SUBSCRIBE_ALL, end to end: listing every broadcast under a prefix and
//! every track each one's producer creates, directly and through a relay.
//!
//! Deterministic: paused time and a single-threaded runtime over the mock transport.

use crate::support;

use std::time::Duration;

use moq_net::{Epoch, Hop, Timestamp, Version, broadcast, origin, track};
use support::harness::{MockConnectOptions, MockPair, connect_mock};

const TIMEOUT: Duration = Duration::from_secs(10);

/// The Subscribe All Stream's type.
const SUBSCRIBE_ALL: u8 = 0x8;

fn version() -> Version {
	"moq-lite-07-wip".parse().unwrap()
}

fn timed() -> track::Info {
	track::Info::default().with_timescale(moq_net::Timescale::MILLI)
}

fn produce_origin(hop: u64) -> origin::Producer {
	let (producer, driver) = origin::Producer::new(origin::Config::new(Hop::new(hop).unwrap()));
	support::harness::spawn(driver);
	producer
}

/// Have `to` pull everything `from` publishes.
async fn link(from: &origin::Producer, to: &origin::Producer) -> MockPair {
	let mut options = MockConnectOptions::new(version());
	options.server_publish = Some(from.consume());
	options.client_subscribe = Some(to.clone());
	connect_mock(options).await
}

/// A publisher, and the origin a subscriber reads it through, with `relay` hops between.
struct Chain {
	publisher: origin::Producer,
	subscriber: origin::Producer,
	/// The session the subscriber's side opened toward the publisher's, first hop first.
	links: Vec<MockPair>,
	_relays: Vec<origin::Producer>,
}

impl Chain {
	async fn new(relay: bool) -> Self {
		let publisher = produce_origin(1);
		let subscriber = produce_origin(3);
		let mut links = Vec::new();
		let mut relays = Vec::new();
		match relay {
			true => {
				let middle = produce_origin(2);
				links.push(link(&publisher, &middle).await);
				links.push(link(&middle, &subscriber).await);
				relays.push(middle);
			}
			false => links.push(link(&publisher, &subscriber).await),
		}
		Self {
			publisher,
			subscriber,
			links,
			_relays: relays,
		}
	}
}

/// Publish `path` with one timed track per name, each holding group 0.
fn publish(origin: &origin::Producer, path: &str, tracks: &[&str]) -> (broadcast::Producer, Vec<track::Producer>) {
	let broadcast = origin.create_broadcast(path).unwrap();
	let tracks = tracks
		.iter()
		.map(|name| {
			let track = broadcast.create_track(*name, timed()).unwrap();
			write(&track, 0);
			track
		})
		.collect();
	broadcast.announce(Default::default()).unwrap();
	(broadcast, tracks)
}

fn write(track: &track::Producer, sequence: u64) {
	let mut group = track.create_group(moq_net::group::Info { sequence }).unwrap();
	group
		.write_frame(Timestamp::from_millis(sequence).unwrap(), &b"x"[..])
		.unwrap();
	group.finish().unwrap();
}

async fn next_broadcast(cursor: &mut origin::Broadcasts) -> broadcast::Consumer {
	moq_clock::timeout(TIMEOUT, cursor.next())
		.await
		.expect("listing hung")
		.expect("listing ended")
}

async fn next_track(tracks: &mut broadcast::Tracks) -> track::Consumer {
	moq_clock::timeout(TIMEOUT, tracks.next())
		.await
		.expect("tracks hung")
		.expect("tracks ended")
}

/// The first group the track delivers, read whole.
async fn read_group(track: &track::Consumer) -> u64 {
	let subscription = track::Subscription::default().with_max_delay(Duration::from_secs(60));
	let mut subscriber = moq_clock::timeout(TIMEOUT, track.subscribe(subscription))
		.await
		.expect("subscribe hung")
		.expect("subscribe");
	let mut group = moq_clock::timeout(TIMEOUT, subscriber.recv_group())
		.await
		.expect("group hung")
		.expect("group")
		.expect("track ended");
	let frame = moq_clock::timeout(TIMEOUT, group.read_frame())
		.await
		.expect("frame hung")
		.expect("frame");
	assert!(frame.is_some(), "group {} has its frame", group.sequence);
	group.sequence
}

/// Tracks present at the request arrive, as does one the producer creates later.
async fn tracks_now_and_later(relay: bool) {
	let chain = Chain::new(relay).await;
	let (broadcast, _tracks) = publish(&chain.publisher, "room/a", &["video", "audio"]);

	let consumer = chain.subscriber.consume();
	moq_clock::timeout(TIMEOUT, consumer.routed("room/a"))
		.await
		.expect("announce hung");
	let mut cursor = consumer.broadcasts("room").unwrap();
	let listed = next_broadcast(&mut cursor).await;
	assert_eq!(listed.info().path.as_str(), "room/a");

	let mut tracks = listed.tracks();
	let mut names = Vec::new();
	for _ in 0..2 {
		let track = next_track(&mut tracks).await;
		assert_eq!(read_group(&track).await, 0, "relay={relay}");
		names.push(track.name().to_string());
	}
	names.sort();
	assert_eq!(names, ["audio", "video"], "relay={relay}");

	let late = broadcast.create_track("late", timed()).unwrap();
	let track = next_track(&mut tracks).await;
	assert_eq!(track.name(), "late");
	write(&late, 3);
	assert_eq!(read_group(&track).await, 3, "relay={relay}");
}

#[moq_clock::test]
async fn lists_tracks_now_and_later() {
	tracks_now_and_later(false).await;
}

#[moq_clock::test]
async fn lists_tracks_now_and_later_through_a_relay() {
	tracks_now_and_later(true).await;
}

/// A broadcast that appears after the request is listed; once it ends, so do its
/// tracks and its listing.
async fn broadcast_appears_and_ends(relay: bool) {
	let chain = Chain::new(relay).await;
	let (_first, _first_tracks) = publish(&chain.publisher, "room/a", &["video"]);

	let consumer = chain.subscriber.consume();
	moq_clock::timeout(TIMEOUT, consumer.routed("room/a"))
		.await
		.expect("announce hung");
	let mut cursor = consumer.broadcasts("room").unwrap();
	assert_eq!(next_broadcast(&mut cursor).await.info().path.as_str(), "room/a");

	let (second, mut second_tracks) = publish(&chain.publisher, "room/b", &["video"]);
	let listed = next_broadcast(&mut cursor).await;
	assert_eq!(listed.info().path.as_str(), "room/b", "relay={relay}");
	let mut tracks = listed.tracks();
	let track = next_track(&mut tracks).await;
	let subscription = track::Subscription::default().with_max_delay(Duration::from_secs(60));
	let mut subscriber = track.subscribe(subscription).await.expect("subscribe");

	let video = second_tracks.pop().unwrap();
	video.finish().unwrap();
	drop(video);
	drop(second);

	// Group 0, then the end of the track.
	let group = moq_clock::timeout(TIMEOUT, subscriber.recv_group())
		.await
		.expect("group hung")
		.expect("group");
	assert_eq!(group.map(|group| group.sequence), Some(0), "relay={relay}");
	let end = moq_clock::timeout(TIMEOUT, subscriber.recv_group())
		.await
		.expect("track end hung");
	assert!(
		matches!(end, Ok(None)),
		"relay={relay}: {:?}",
		end.map(|group| group.map(|group| group.sequence))
	);
	let ended = moq_clock::timeout(TIMEOUT, tracks.next())
		.await
		.expect("tracks end hung");
	assert!(ended.is_none(), "relay={relay}");
}

#[moq_clock::test]
async fn lists_a_broadcast_that_appears_and_ends() {
	broadcast_appears_and_ends(false).await;
}

#[moq_clock::test]
async fn lists_a_broadcast_that_appears_and_ends_through_a_relay() {
	broadcast_appears_and_ends(true).await;
}

fn epochs() -> (Epoch, Epoch) {
	(
		"01900000-0000-7000-8000-000000000001".parse().unwrap(),
		"01900000-0000-7000-8000-000000000002".parse().unwrap(),
	)
}

/// A new publisher instance at the same path is another broadcast, listed again.
async fn new_epoch(relay: bool) {
	let chain = Chain::new(relay).await;
	let (old, new) = epochs();
	let first = chain.publisher.create_broadcast("room/a").unwrap();
	let _first_track = first.create_track("video", timed()).unwrap();
	first
		.announce(origin::Route::default().with_epoch(old.clone()))
		.unwrap();

	let consumer = chain.subscriber.consume();
	moq_clock::timeout(TIMEOUT, consumer.routed("room/a"))
		.await
		.expect("announce hung");
	let mut cursor = consumer.broadcasts("room").unwrap();
	let listed = next_broadcast(&mut cursor).await;
	assert_eq!(listed.info().epoch, Some(old), "relay={relay}");

	let second = chain.publisher.create_broadcast("room/a").unwrap();
	let _second_track = second.create_track("video", timed()).unwrap();
	second
		.announce(origin::Route::default().with_epoch(new.clone()))
		.unwrap();
	let listed = next_broadcast(&mut cursor).await;
	assert_eq!(listed.info().path.as_str(), "room/a");
	assert_eq!(listed.info().epoch, Some(new), "relay={relay}");
	assert_eq!(next_track(&mut listed.tracks()).await.name(), "video");
}

#[moq_clock::test]
async fn lists_a_new_epoch_at_the_same_path() {
	new_epoch(false).await;
}

#[moq_clock::test]
async fn lists_a_new_epoch_at_the_same_path_through_a_relay() {
	new_epoch(true).await;
}

/// A track served only on request is never created by listing.
async fn dynamic_only(relay: bool) {
	let chain = Chain::new(relay).await;
	let broadcast = chain.publisher.create_broadcast("room/a").unwrap();
	let mut dynamic = broadcast.dynamic();
	let track = broadcast.create_track("static", timed()).unwrap();
	write(&track, 0);
	broadcast.announce(Default::default()).unwrap();

	let consumer = chain.subscriber.consume();
	moq_clock::timeout(TIMEOUT, consumer.routed("room/a"))
		.await
		.expect("announce hung");
	let mut cursor = consumer.broadcasts("room").unwrap();
	let listed = next_broadcast(&mut cursor).await;
	let mut tracks = listed.tracks();
	let listed_track = next_track(&mut tracks).await;
	assert_eq!(listed_track.name(), "static");
	assert_eq!(read_group(&listed_track).await, 0);

	assert!(
		moq_clock::timeout(Duration::from_secs(1), tracks.next()).await.is_err(),
		"relay={relay}: nothing else is listed"
	);
	assert!(
		moq_clock::timeout(Duration::from_secs(1), dynamic.requested_track())
			.await
			.is_err(),
		"relay={relay}: listing asked the handler for a track"
	);
}

#[moq_clock::test]
async fn never_creates_a_dynamic_track() {
	dynamic_only(false).await;
}

#[moq_clock::test]
async fn never_creates_a_dynamic_track_through_a_relay() {
	dynamic_only(true).await;
}

/// Two downstream listings of one prefix share the relay's one upstream SUBSCRIBE_ALL.
#[moq_clock::test]
async fn downstream_requests_share_one_upstream() {
	let chain = Chain::new(true).await;
	let (_broadcast, _tracks) = publish(&chain.publisher, "room/a", &["video"]);

	let other = produce_origin(4);
	let _other_link = link(&chain._relays[0], &other).await;

	let mut readers = Vec::new();
	for origin in [&chain.subscriber, &other] {
		let consumer = origin.consume();
		moq_clock::timeout(TIMEOUT, consumer.routed("room/a"))
			.await
			.expect("announce hung");
		let mut cursor = consumer.broadcasts("room").unwrap();
		let listed = next_broadcast(&mut cursor).await;
		let track = next_track(&mut listed.tracks()).await;
		assert_eq!(read_group(&track).await, 0);
		readers.push(cursor);
	}

	let upstream = chain.links[0]
		.client_transport
		.bidi_types()
		.into_iter()
		.filter(|kind| *kind == SUBSCRIBE_ALL)
		.count();
	assert_eq!(upstream, 1, "one upstream SUBSCRIBE_ALL for both downstreams");
}

/// An older peer has no SUBSCRIBE_ALL, and nothing emulates it: the listing stays empty
/// while ordinary requests still work.
#[moq_clock::test]
async fn older_versions_list_nothing() {
	let publisher = produce_origin(1);
	let subscriber = produce_origin(3);
	let (_broadcast, _tracks) = publish(&publisher, "room/a", &["video"]);
	let mut options = MockConnectOptions::new("moq-lite-06".parse().unwrap());
	options.server_publish = Some(publisher.consume());
	options.client_subscribe = Some(subscriber.clone());
	let pair = connect_mock(options).await;

	let consumer = subscriber.consume();
	moq_clock::timeout(TIMEOUT, consumer.routed("room/a"))
		.await
		.expect("announce hung");
	let mut cursor = consumer.broadcasts("room").unwrap();
	assert!(moq_clock::timeout(Duration::from_secs(1), cursor.next()).await.is_err());
	assert!(!pair.client_transport.bidi_types().contains(&SUBSCRIBE_ALL));
}
