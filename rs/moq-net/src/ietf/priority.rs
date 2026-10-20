//! Priority conversion between the IETF wire and the MoQ model.

/// Convert an IETF priority into the model's higher-first priority.
///
/// IETF is lower-first. The mapping is one-to-one over the whole byte and keeps the IETF
/// byte at `255 -` the moq-lite byte, so one urgency reads the same on both wires. That puts
/// the model's default 0 at IETF 127, one step off the draft's usual 128.
pub(super) const fn from_wire(priority: u8) -> i8 {
	(i8::MAX as u8).wrapping_sub(priority) as i8
}

/// Convert the model's higher-first priority into an IETF priority.
pub(super) const fn to_wire(priority: i8) -> u8 {
	(i8::MAX as u8).wrapping_sub(priority as u8)
}

/// Convert an IETF priority that may be absent, which is the model's default.
pub(super) fn from_wire_or_default(priority: Option<u8>) -> i8 {
	priority.map_or(0, from_wire)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn ietf_priority_is_lower_first() {
		assert_eq!(to_wire(i8::MAX), 0);
		assert_eq!(to_wire(0), 127);
		assert_eq!(to_wire(i8::MIN), u8::MAX);
		assert_eq!(from_wire(0), i8::MAX);
		assert_eq!(from_wire(127), 0);
		assert_eq!(from_wire(u8::MAX), i8::MIN);
		assert!((0u8..u8::MAX).all(|priority| from_wire(priority) > from_wire(priority + 1)));
	}

	#[test]
	fn absent_priority_is_the_default() {
		assert_eq!(from_wire_or_default(None), 0);
		assert_eq!(from_wire_or_default(Some(127)), 0);
	}

	#[test]
	fn priority_round_trips() {
		assert!((i8::MIN..=i8::MAX).all(|priority| from_wire(to_wire(priority)) == priority));
	}

	/// One urgency on both wires: the IETF byte mirrors the moq-lite byte.
	#[test]
	fn ietf_mirrors_lite() {
		assert!((i8::MIN..=i8::MAX).all(|priority| to_wire(priority) == u8::MAX - crate::lite::priority::to_wire(priority)));
	}
}
