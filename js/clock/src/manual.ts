import { install } from "./ambient.ts";
import type { Backend } from "./backend.ts";
import { drain } from "./host.ts";
import { Instant, Nano, Wall } from "./types.ts";

type Timer = { at: number; order: number; index: number; ready: () => void };

/**
 * A clock that starts at zero and advances only when a test moves it.
 * Drive it serially: overlapping advance, jump, or fire calls fail immediately.
 */
export class Manual {
	/** The reproducible starting wall time, 2025-01-01T00:00:00Z. */
	static readonly WALL_START = Wall(1_735_689_600_000);
	#now = Instant.zero;
	// Wall time is derived from an anchor so tiny advances don't vanish into an epoch-sized float.
	#wallAnchor = Manual.WALL_START;
	#wallAnchorAt = Instant.zero;
	#timers: Timer[] = [];
	#order = 0;
	#revision = 0;
	#fired = 0;
	#driving = false;
	#advancing = false;
	#armed?: () => void;
	#backend: Backend = {
		now: () => this.#now,
		wall: () => this.wall(),
		origin: () => this.#wallAnchor - Nano.toMilli(Nano(this.#wallAnchorAt)),
		arm: (at, ready) => {
			const timer = { at, order: this.#order++, index: this.#timers.length, ready };
			this.#timers.push(timer);
			this.#up(timer.index);
			this.#revision++;
			if (at <= this.#now && !this.#advancing) this.#ready(timer);
			this.#armed?.();
			return () => {
				this.#remove(timer);
			};
		},
	};

	/** Install this clock in the current realm and return an idempotent restore function. */
	install(): () => void {
		return install(this.#backend);
	}

	/** Read this clock's instant without installing it. */
	now(): Instant {
		return this.#now;
	}

	/** Read this clock's wall time without installing it. */
	wall(): Wall {
		return Wall.add(this.#wallAnchor, Instant.durationSince(this.#now, this.#wallAnchorAt));
	}

	/** Jump forward without draining tasks, firing due timers late as a stalled event loop would. */
	jump(duration: Nano): void {
		if (this.#driving) throw new Error("time driver already pending");
		this.#driving = true;
		try {
			this.#now = Instant.add(this.#now, duration);
			this.#fire();
		} finally {
			this.#driving = false;
		}
	}

	/** Advance through each due deadline, draining tasks before continuing to the target. */
	async advance(duration: Nano): Promise<void> {
		if (this.#driving) throw new Error("time driver already pending");
		this.#driving = true;
		try {
			await this.#advance(duration);
		} finally {
			this.#driving = false;
		}
	}

	/**
	 * Wait until a timer is armed, advance exactly to the earliest deadline, then drain like
	 * `advance` and return the instant reached. Awaits forever if no timer is ever armed.
	 */
	async fire(): Promise<Instant> {
		if (this.#driving) throw new Error("time driver already pending");
		this.#driving = true;
		const fired = this.#fired;
		try {
			for (;;) {
				// Lets a pending microtask chain arm an earlier timer before choosing the deadline.
				await drain();
				// arm() runs an already-due timer at once, so it never waits in the heap.
				const at = this.#fired !== fired ? this.#now : this.#timers[0]?.at;
				if (at !== undefined) {
					await this.#advance(Instant.durationSince(Instant(at), this.#now));
					return this.#now;
				}
				// A host task, such as I/O, may arm the timer later than any drain can wait for.
				await new Promise<void>((resolve) => {
					this.#armed = resolve;
				});
				this.#armed = undefined;
			}
		} finally {
			this.#driving = false;
		}
	}

	/** Set wall time forward or backward without passing monotonic time. */
	stepWall(wall: Wall): void {
		this.#wallAnchor = Wall(wall);
		this.#wallAnchorAt = this.#now;
	}

	async #advance(duration: Nano): Promise<void> {
		const target = Instant.add(this.#now, duration);
		// Reached timers armed while draining join the heap, behind existing peers.
		this.#advancing = true;
		try {
			for (;;) {
				const revision = this.#revision;
				await drain();
				const first = this.#timers[0];
				if (first !== undefined && first.at <= target) {
					this.#now = Instant(Math.max(this.#now, first.at));
					this.#ready(first);
				} else if (this.#now < target) {
					this.#now = target;
				} else if (revision === this.#revision) {
					return;
				}
			}
		} finally {
			this.#advancing = false;
		}
	}

	#before(a: Timer, b: Timer): boolean {
		return a.at < b.at || (a.at === b.at && a.order < b.order);
	}

	#swap(a: number, b: number): void {
		const left = this.#timers[a];
		const right = this.#timers[b];
		this.#timers[a] = right;
		this.#timers[b] = left;
		left.index = b;
		right.index = a;
	}

	#up(index: number): void {
		while (index > 0) {
			const parent = (index - 1) >> 1;
			if (!this.#before(this.#timers[index], this.#timers[parent])) return;
			this.#swap(index, parent);
			index = parent;
		}
	}

	#down(index: number): void {
		for (;;) {
			let child = index * 2 + 1;
			if (child >= this.#timers.length) return;
			if (child + 1 < this.#timers.length && this.#before(this.#timers[child + 1], this.#timers[child])) child++;
			if (!this.#before(this.#timers[child], this.#timers[index])) return;
			this.#swap(index, child);
			index = child;
		}
	}

	#remove(timer: Timer): void {
		if (timer.index < 0) return;
		const index = timer.index;
		const last = this.#timers.pop();
		timer.index = -1;
		if (!last || last === timer) return;
		this.#timers[index] = last;
		last.index = index;
		this.#up(index);
		this.#down(last.index);
	}

	#ready(timer: Timer): void {
		this.#remove(timer);
		this.#revision++;
		this.#fired++;
		timer.ready();
	}

	#fire(): void {
		for (;;) {
			const first = this.#timers[0];
			if (!first || first.at > this.#now) return;
			this.#ready(first);
		}
	}
}
