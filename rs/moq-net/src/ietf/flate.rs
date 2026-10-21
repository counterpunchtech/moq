//! Negotiation for DEFLATE-compressed tracks (draft-lcurley-moq-flate).

use crate::coding::DecodeError;

use super::{ParameterVarInt, Parameters, Version};

/// FLATE Setup Option, whose value 1 offers the group-scoped DEFLATE format.
pub(super) const OPTION: u64 = 0x40B6A;

pub(crate) fn supported(version: Version) -> bool {
	!matches!(version, Version::Draft14 | Version::Draft15)
}

#[cfg(test)]
fn into_setup(parameters: &mut Parameters, version: Version) {
	if supported(version) {
		parameters.set_varint(ParameterVarInt::Flate, 1);
	}
}

pub(crate) fn from_setup(parameters: &Parameters, version: Version) -> Result<bool, DecodeError> {
	if !supported(version) {
		return Ok(false);
	}
	match parameters.get_varint(ParameterVarInt::Flate) {
		None => Ok(false),
		Some(1) => Ok(true),
		Some(_) => Err(DecodeError::InvalidValue),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn negotiation_requires_an_offer_on_draft16_or_later() {
		for version in Version::ALL {
			let mut parameters = Parameters::default();
			assert!(!from_setup(&parameters, version).unwrap());
			into_setup(&mut parameters, version);
			assert_eq!(from_setup(&parameters, version).unwrap(), supported(version));
			use crate::coding::{Decode, Encode};
			let encoded = parameters.encode_bytes(version).unwrap();
			let (decoded, consumed) = Parameters::decode_slice(&encoded, version).unwrap();
			assert_eq!(consumed, encoded.len());
			assert_eq!(from_setup(&decoded, version).unwrap(), supported(version));
			for invalid in [0, 2, u64::MAX >> 2] {
				parameters.set_varint(ParameterVarInt::Flate, invalid);
				assert_eq!(from_setup(&parameters, version).is_err(), supported(version));
			}
		}
	}

	#[test]
	fn setup_code_point_is_even_and_not_grease() {
		assert_eq!(OPTION % 2, 0);
		assert_ne!((OPTION - 0x9d) % 0x7f, 0);
		const { assert!(OPTION >= 0x4000) };
	}
}
