//! Default delivery priorities for hang tracks.

/// Delivery priorities for each hang track kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Priorities {
	/// Catalog track priority.
	pub catalog: i8,
	/// Text (caption/subtitle) track priority.
	pub text: i8,
	/// Audio track priority.
	pub audio: i8,
	/// Video track priority.
	pub video: i8,
}

/// Default delivery priorities for hang tracks, with higher values sent first.
///
/// All of them sit above 0, the unset default, so hang media outranks a track that never set
/// a priority. Text sits just below the catalog and above audio/video: cues are tiny and accessibility-critical,
/// so they should never be starved behind a media backlog.
pub const PRIORITY: Priorities = Priorities {
	catalog: 100,
	text: 90,
	audio: 80,
	video: 60,
};

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn media_priorities_match_the_browser() {
		assert_eq!(PRIORITY.catalog, 100);
		assert_eq!(PRIORITY.text, 90);
		assert_eq!(PRIORITY.audio, 80);
		assert_eq!(PRIORITY.video, 60);
	}

	#[test]
	fn media_outranks_the_unset_default() {
		let unset = moq_net::track::Info::default().priority;
		assert!([PRIORITY.catalog, PRIORITY.text, PRIORITY.audio, PRIORITY.video].iter().all(|&p| p > unset));
	}
}
