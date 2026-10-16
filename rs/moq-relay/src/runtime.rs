//! Flags for serving QUIC from per-core workers.
//!
//! The workers themselves are [`moq_tokio::worker`]; this is the operator-facing
//! half, which is the only part specific to the relay. Unset (the default) shards
//! QUIC across one worker per core where the kernel can steer a group (Linux) and
//! keeps it on the shared runtime elsewhere.

use serde::{Deserialize, Serialize};

/// How the relay lays its QUIC work out over threads.
#[derive(usage::Args, Clone, Debug, Default, Deserialize, Serialize)]
#[usage(unknown_flags = "error", args_override_self = false)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct Config {
	/// Serve QUIC from this many single-threaded workers, each with its own
	/// socket on the listen address, instead of the shared runtime.
	///
	/// A connection is handled start to finish by one worker, which trades the
	/// shared runtime's load balancing for no cross-thread traffic per packet.
	/// Unset (the default) runs one worker per core on Linux and the shared
	/// runtime elsewhere; `0` keeps the shared runtime. Any other count is
	/// Linux-only. Every worker shares the relay's served identity and reloads.
	#[usage(long = "runtime-workers", env = "MOQ_RUNTIME_WORKERS", setting = "runtime.workers")]
	pub workers: Option<u16>,

	/// Pin each worker to a CPU core, defaulting to off.
	///
	/// The mode's measured win comes from each worker owning a socket and a
	/// runtime, not from pinning: on a single-socket machine, pinned and unpinned
	/// benchmark inside run-to-run noise of each other. Pinning stops the
	/// scheduler migrating a busy worker, which may matter on a multi-socket or
	/// NUMA machine with the cores to itself. It is off by default because every
	/// process pins its workers to the same first cores: two relays, or a relay
	/// limited to fewer CPUs than the host has, would crowd onto those cores.
	#[usage(long = "runtime-pin", env = "MOQ_RUNTIME_PIN", setting = "runtime.pin", bool_value)]
	pub pin: bool,

	/// Drive the QUIC workers with io_uring instead of tokio.
	///
	/// Each worker owns a `SINGLE_ISSUER` ring, batched UDP (multishot receive
	/// with `UDP_GRO`, `UDP_SEGMENT` send), userspace timers, and a local task
	/// set; sessions are moq-lite only (native raw QUIC and browser
	/// WebTransport alike), and everything else (HTTP, WebSocket, cluster
	/// dials, stats) stays on the shared runtime. Requires QUIC workers (any
	/// `runtime-workers` but `0`), Linux 6.12+, and refuses to start anywhere it
	/// cannot deliver.
	#[usage(
		long = "runtime-io-uring",
		env = "MOQ_RUNTIME_IO_URING",
		setting = "runtime.io_uring",
		bool_value
	)]
	pub io_uring: bool,
}

impl Config {
	/// The worker group this asks for, or `None` to keep QUIC on the shared
	/// runtime.
	///
	/// `per_core` is the count an unset `workers` falls back to: one per core
	/// where the kernel can steer a group, or `None` where the listener cannot be
	/// sharded at all. `0` is the explicit way back to the shared runtime.
	pub(crate) fn workers(&self, per_core: Option<u16>) -> Option<moq_tokio::worker::Config> {
		let count = match self.workers {
			Some(0) => return None,
			Some(count) => count,
			None => per_core?,
		};
		Some(moq_tokio::worker::Config::new(count).with_pin(self.pin))
	}

	/// Whether the workers should run on io_uring instead of tokio.
	pub fn io_uring(&self) -> bool {
		self.io_uring
	}
}
