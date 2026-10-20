//! Priority conversion between the moq-lite wire and the MoQ model.

/// Convert a moq-lite priority byte into the model's signed priority.
///
/// The byte is higher-first like the model, offset so the model's default 0 is byte 128.
pub(crate) const fn from_wire(priority: u8) -> i8 {
	(priority ^ 0x80) as i8
}

/// Convert the model's signed priority into a moq-lite priority byte.
pub(crate) const fn to_wire(priority: i8) -> u8 {
	(priority as u8) ^ 0x80
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn lite_priority_is_offset_by_128() {
		assert_eq!(to_wire(i8::MIN), 0);
		assert_eq!(to_wire(0), 128);
		assert_eq!(to_wire(i8::MAX), u8::MAX);
		assert_eq!(from_wire(0), i8::MIN);
		assert_eq!(from_wire(128), 0);
		assert_eq!(from_wire(u8::MAX), i8::MAX);
	}

	#[test]
	fn lite_priority_keeps_order() {
		assert!((i8::MIN..i8::MAX).all(|priority| to_wire(priority) < to_wire(priority + 1)));
		assert!((0u8..=u8::MAX).all(|byte| to_wire(from_wire(byte)) == byte));
	}
}
