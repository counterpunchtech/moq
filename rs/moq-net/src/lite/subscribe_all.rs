use std::{borrow::Cow, time::Duration};

use crate::{
	Path,
	coding::{Decode, DecodeError, Decoder, Encode, EncodeError, Encoder},
	track::Order,
};

use super::{Message, SubscribeEnd, SubscribeStart, TrackInfo, Version};

/// Sent by the subscriber on a Subscribe All Stream (0x8) to subscribe to every track of
/// every broadcast under a prefix, including tracks and broadcasts that appear later.
///
/// Lite07+ only.
#[derive(Clone, Debug)]
pub struct SubscribeAll<'a> {
	/// Shares the SUBSCRIBE id space: group streams name it.
	pub id: u64,
	pub prefix: Path<'a>,
	pub priority: u8,
	pub order: Order,
	pub max_delay: Duration,
}

impl Message for SubscribeAll<'_> {
	fn decode_msg(r: &mut Decoder<'_>, version: Version) -> Result<Self, DecodeError> {
		if !version.has_subscribe_all() {
			return Err(DecodeError::Version);
		}

		Ok(Self {
			id: r.varint()?,
			prefix: Path::decode(r, version)?,
			priority: r.u8()?,
			order: super::subscribe::decode_order(r, version)?,
			max_delay: Duration::from_millis(r.varint()?),
		})
	}

	fn encode_msg(&self, w: &mut Encoder<'_>, version: Version) -> Result<(), EncodeError> {
		if !version.has_subscribe_all() {
			return Err(EncodeError::Version);
		}

		w.varint(self.id)?;
		self.prefix.encode(w, version)?;
		w.u8(self.priority);
		super::subscribe::encode_order(w, version, self.order)?;
		w.varint(u64::try_from(self.max_delay.as_millis()).map_err(|_| EncodeError::BoundsExceeded)?)
	}
}

/// Sent by the subscriber to change every track of a SUBSCRIBE_ALL at once.
///
/// Lite07+ only.
#[derive(Clone, Debug)]
pub struct SubscribeAllUpdate {
	pub priority: u8,
	pub order: Order,
	pub max_delay: Duration,
}

impl Message for SubscribeAllUpdate {
	fn decode_msg(r: &mut Decoder<'_>, version: Version) -> Result<Self, DecodeError> {
		if !version.has_subscribe_all() {
			return Err(DecodeError::Version);
		}

		Ok(Self {
			priority: r.u8()?,
			order: super::subscribe::decode_order(r, version)?,
			max_delay: Duration::from_millis(r.varint()?),
		})
	}

	fn encode_msg(&self, w: &mut Encoder<'_>, version: Version) -> Result<(), EncodeError> {
		if !version.has_subscribe_all() {
			return Err(EncodeError::Version);
		}

		w.u8(self.priority);
		super::subscribe::encode_order(w, version, self.order)?;
		w.varint(u64::try_from(self.max_delay.as_millis()).map_err(|_| EncodeError::BoundsExceeded)?)
	}
}

/// A broadcast under the prefix, implicitly assigned the next Broadcast ID on the stream.
#[derive(Clone, Debug)]
pub struct BroadcastStart<'a> {
	/// The broadcast path relative to the requested prefix.
	pub suffix: Path<'a>,
	pub epoch: Option<crate::Epoch>,
}

impl Message for BroadcastStart<'_> {
	fn decode_msg(r: &mut Decoder<'_>, version: Version) -> Result<Self, DecodeError> {
		Ok(Self {
			suffix: Path::decode(r, version)?,
			epoch: super::epoch::decode_epoch(r, version)?,
		})
	}

	fn encode_msg(&self, w: &mut Encoder<'_>, version: Version) -> Result<(), EncodeError> {
		self.suffix.encode(w, version)?;
		super::epoch::encode_epoch(w, version, self.epoch.as_ref())
	}
}

/// A broadcast that takes no new tracks. Its announced tracks end on their own.
#[derive(Clone, Debug)]
pub struct BroadcastEnd {
	pub broadcast: u64,
}

impl Message for BroadcastEnd {
	fn decode_msg(r: &mut Decoder<'_>, _: Version) -> Result<Self, DecodeError> {
		Ok(Self { broadcast: r.varint()? })
	}

	fn encode_msg(&self, w: &mut Encoder<'_>, _: Version) -> Result<(), EncodeError> {
		w.varint(self.broadcast)
	}
}

/// A track of a listed broadcast with its immutable properties, implicitly assigned the
/// next Track ID on the stream.
#[derive(Clone, Debug)]
pub struct TrackStart<'a> {
	pub broadcast: u64,
	pub name: Cow<'a, str>,
	pub info: TrackInfo,
}

impl Message for TrackStart<'_> {
	fn decode_msg(r: &mut Decoder<'_>, version: Version) -> Result<Self, DecodeError> {
		Ok(Self {
			broadcast: r.varint()?,
			name: Cow::Owned(r.string()?),
			info: TrackInfo::decode_msg(r, version)?,
		})
	}

	fn encode_msg(&self, w: &mut Encoder<'_>, version: Version) -> Result<(), EncodeError> {
		w.varint(self.broadcast)?;
		w.string(&self.name)?;
		self.info.encode_msg(w, version)
	}
}

/// A listed track's SUBSCRIBE_OK.
#[derive(Clone, Debug)]
pub struct TrackOk {
	pub track: u64,
	pub start: SubscribeStart,
}

impl Message for TrackOk {
	fn decode_msg(r: &mut Decoder<'_>, version: Version) -> Result<Self, DecodeError> {
		Ok(Self {
			track: r.varint()?,
			start: SubscribeStart::decode_msg(r, version)?,
		})
	}

	fn encode_msg(&self, w: &mut Encoder<'_>, version: Version) -> Result<(), EncodeError> {
		w.varint(self.track)?;
		self.start.encode_msg(w, version)
	}
}

/// A listed track's SUBSCRIBE_END.
#[derive(Clone, Debug)]
pub struct TrackEnd {
	pub track: u64,
	pub end: SubscribeEnd,
}

impl Message for TrackEnd {
	fn decode_msg(r: &mut Decoder<'_>, version: Version) -> Result<Self, DecodeError> {
		Ok(Self {
			track: r.varint()?,
			end: SubscribeEnd::decode_msg(r, version)?,
		})
	}

	fn encode_msg(&self, w: &mut Encoder<'_>, version: Version) -> Result<(), EncodeError> {
		w.varint(self.track)?;
		self.end.encode_msg(w, version)
	}
}

/// A listed track that failed, standing in for the reset of its own Subscribe Stream.
#[derive(Clone, Debug)]
pub struct TrackReset {
	pub track: u64,
	/// A stream error code.
	pub code: u64,
}

impl Message for TrackReset {
	fn decode_msg(r: &mut Decoder<'_>, _: Version) -> Result<Self, DecodeError> {
		Ok(Self {
			track: r.varint()?,
			code: r.varint()?,
		})
	}

	fn encode_msg(&self, w: &mut Encoder<'_>, _: Version) -> Result<(), EncodeError> {
		w.varint(self.track)?;
		w.varint(self.code)
	}
}

/// A message the publisher sends on a Subscribe All Stream, prefixed with its type.
#[derive(Clone, Debug)]
pub enum SubscribeAllResponse<'a> {
	BroadcastStart(BroadcastStart<'a>),
	BroadcastEnd(BroadcastEnd),
	TrackStart(TrackStart<'a>),
	TrackOk(TrackOk),
	TrackEnd(TrackEnd),
	TrackReset(TrackReset),
}

impl Encode<Version> for SubscribeAllResponse<'_> {
	fn encode(&self, w: &mut Encoder<'_>, version: Version) -> Result<(), EncodeError> {
		if !version.has_subscribe_all() {
			return Err(EncodeError::Version);
		}
		match self {
			Self::BroadcastStart(msg) => {
				w.varint(0)?;
				msg.encode(w, version)
			}
			Self::BroadcastEnd(msg) => {
				w.varint(1)?;
				msg.encode(w, version)
			}
			Self::TrackStart(msg) => {
				w.varint(2)?;
				msg.encode(w, version)
			}
			Self::TrackOk(msg) => {
				w.varint(3)?;
				msg.encode(w, version)
			}
			Self::TrackEnd(msg) => {
				w.varint(4)?;
				msg.encode(w, version)
			}
			Self::TrackReset(msg) => {
				w.varint(5)?;
				msg.encode(w, version)
			}
		}
	}
}

impl Decode<Version> for SubscribeAllResponse<'_> {
	fn decode(r: &mut Decoder<'_>, version: Version) -> Result<Self, DecodeError> {
		if !version.has_subscribe_all() {
			return Err(DecodeError::Version);
		}
		match r.varint()? {
			0 => Ok(Self::BroadcastStart(BroadcastStart::decode(r, version)?)),
			1 => Ok(Self::BroadcastEnd(BroadcastEnd::decode(r, version)?)),
			2 => Ok(Self::TrackStart(TrackStart::decode(r, version)?)),
			3 => Ok(Self::TrackOk(TrackOk::decode(r, version)?)),
			4 => Ok(Self::TrackEnd(TrackEnd::decode(r, version)?)),
			5 => Ok(Self::TrackReset(TrackReset::decode(r, version)?)),
			typ => Err(DecodeError::InvalidMessage(typ)),
		}
	}
}

/// The header of a group stream (0x2) carrying a SUBSCRIBE_ALL track.
#[derive(Clone, Debug)]
pub struct TrackGroup {
	pub subscribe: u64,
	pub track: u64,
	pub sequence: u64,
	/// See [`super::Group::frame_start`].
	pub frame_start: u64,
}

impl Message for TrackGroup {
	fn decode_msg(r: &mut Decoder<'_>, version: Version) -> Result<Self, DecodeError> {
		if !version.has_subscribe_all() {
			return Err(DecodeError::Version);
		}
		Ok(Self {
			subscribe: r.varint()?,
			track: r.varint()?,
			sequence: r.varint()?,
			frame_start: r.varint()?,
		})
	}

	fn encode_msg(&self, w: &mut Encoder<'_>, version: Version) -> Result<(), EncodeError> {
		if !version.has_subscribe_all() {
			return Err(EncodeError::Version);
		}
		w.varint(self.subscribe)?;
		w.varint(self.track)?;
		w.varint(self.sequence)?;
		w.varint(self.frame_start)
	}
}

#[cfg(test)]
mod test {
	use super::*;

	fn encode<T: Encode<Version>>(msg: &T, version: Version) -> Result<Vec<u8>, EncodeError> {
		let mut buf = Vec::new();
		msg.encode(&mut Encoder::new(&mut buf, version.into()), version)?;
		Ok(buf)
	}

	#[test]
	fn request_roundtrips() {
		let msg = SubscribeAll {
			id: 3,
			prefix: Path::new("room/a"),
			priority: 7,
			order: Order::Ascending,
			max_delay: Duration::from_millis(250),
		};
		let buf = encode(&msg, Version::Lite07).unwrap();
		let (got, _) = SubscribeAll::decode_slice(&buf, Version::Lite07).unwrap();
		assert_eq!(got.id, 3);
		assert_eq!(got.prefix.as_str(), "room/a");
		assert_eq!((got.priority, got.order, got.max_delay), (7, Order::Ascending, msg.max_delay));

		let update = SubscribeAllUpdate {
			priority: 1,
			order: Order::Descending,
			max_delay: Duration::ZERO,
		};
		let buf = encode(&update, Version::Lite07).unwrap();
		let (got, _) = SubscribeAllUpdate::decode_slice(&buf, Version::Lite07).unwrap();
		assert_eq!((got.priority, got.order, got.max_delay), (1, Order::Descending, Duration::ZERO));
	}

	/// Older versions have no Subscribe All Stream, and nothing emulates it.
	#[test]
	fn refused_before_lite07() {
		let msg = SubscribeAll {
			id: 0,
			prefix: Path::new(""),
			priority: 0,
			order: Order::Descending,
			max_delay: Duration::ZERO,
		};
		assert!(encode(&msg, Version::Lite06).is_err());
		let buf = encode(&msg, Version::Lite07).unwrap();
		assert!(SubscribeAll::decode_slice(&buf, Version::Lite06).is_err());
	}

	#[test]
	fn responses_roundtrip() {
		let epoch = crate::Epoch::mint();
		let responses = [
			SubscribeAllResponse::BroadcastStart(BroadcastStart {
				suffix: Path::new("alice"),
				epoch: Some(epoch.clone()),
			}),
			SubscribeAllResponse::BroadcastEnd(BroadcastEnd { broadcast: 4 }),
			SubscribeAllResponse::TrackStart(TrackStart {
				broadcast: 4,
				name: "video".into(),
				info: TrackInfo {
					priority: 2,
					max_age: Some(Duration::from_secs(1)),
					timescale: Some(crate::Timescale::MILLI),
				},
			}),
			SubscribeAllResponse::TrackOk(TrackOk {
				track: 9,
				start: SubscribeStart {
					group: 5,
					largest: Some(crate::track::Position { group: 5, frame: 1 }),
				},
			}),
			SubscribeAllResponse::TrackEnd(TrackEnd {
				track: 9,
				end: SubscribeEnd { group: 6, streams: 2 },
			}),
			SubscribeAllResponse::TrackReset(TrackReset { track: 9, code: 0x3b }),
		];
		for msg in responses {
			let buf = encode(&msg, Version::Lite07).unwrap();
			let (got, _) = SubscribeAllResponse::decode_slice(&buf, Version::Lite07).unwrap();
			match (msg, got) {
				(SubscribeAllResponse::BroadcastStart(a), SubscribeAllResponse::BroadcastStart(b)) => {
					assert_eq!((a.suffix.as_str(), a.epoch), (b.suffix.as_str(), b.epoch))
				}
				(SubscribeAllResponse::BroadcastEnd(a), SubscribeAllResponse::BroadcastEnd(b)) => {
					assert_eq!(a.broadcast, b.broadcast)
				}
				(SubscribeAllResponse::TrackStart(a), SubscribeAllResponse::TrackStart(b)) => {
					assert_eq!((a.broadcast, a.name), (b.broadcast, b.name));
					assert_eq!(
						(a.info.priority, a.info.max_age, a.info.timescale),
						(b.info.priority, b.info.max_age, b.info.timescale)
					);
				}
				(SubscribeAllResponse::TrackOk(a), SubscribeAllResponse::TrackOk(b)) => {
					assert_eq!((a.track, a.start.group, a.start.largest), (b.track, b.start.group, b.start.largest))
				}
				(SubscribeAllResponse::TrackEnd(a), SubscribeAllResponse::TrackEnd(b)) => {
					assert_eq!((a.track, a.end.group, a.end.streams), (b.track, b.end.group, b.end.streams))
				}
				(SubscribeAllResponse::TrackReset(a), SubscribeAllResponse::TrackReset(b)) => {
					assert_eq!((a.track, a.code), (b.track, b.code))
				}
				(a, b) => panic!("decoded {b:?} for {a:?}"),
			}
		}
	}

	#[test]
	fn group_header_roundtrips() {
		let msg = TrackGroup {
			subscribe: 1,
			track: 2,
			sequence: 3,
			frame_start: 4,
		};
		let buf = encode(&msg, Version::Lite07).unwrap();
		assert_eq!(buf, [4, 1, 2, 3, 4]);
		let (got, _) = TrackGroup::decode_slice(&buf, Version::Lite07).unwrap();
		assert_eq!((got.subscribe, got.track, got.sequence, got.frame_start), (1, 2, 3, 4));
		assert!(encode(&msg, Version::Lite06).is_err());
	}
}
