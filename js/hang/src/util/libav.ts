let loading: Promise<boolean> | undefined;

// The WebCodecs registry's AAC codec strings. The polyfill refuses every "mp4a" string, so these
// reach libav's own AAC decoder through the polyfill's libav.js codec escape hatch instead.
const AAC = new Set(["mp4a.40.2", "mp4a.40.02", "mp4a.40.5", "mp4a.40.05", "mp4a.40.29", "mp4a.67"]);

/** Load the libav WebCodecs polyfill (Opus, plus AAC decoding) if AudioEncoder/AudioDecoder are missing. Resolves true once available. */
export async function polyfill(): Promise<boolean> {
	if (globalThis.AudioEncoder && globalThis.AudioDecoder) {
		return true;
	}

	if (!loading) {
		console.warn("using libav WebCodecs polyfill; performance may be degraded");

		// No published libav.js variant decodes AAC, so this one is opus-af plus the AAC decoder.
		// I forked libavjs-webcodecs-polyfill to avoid Typescript errors; there's no changes otherwise.
		loading = Promise.all([
			import("@kixelated/libav.js-variant-opus-aac-af"),
			import("@kixelated/libavjs-webcodecs-polyfill"),
		]).then(async ([variant, libav]) => {
			const native = globalThis.AudioDecoder;
			await libav.load({
				LibAV: variant,
				polyfill: true,
			});
			if (native) return true;

			type Config = Parameters<InstanceType<typeof libav.AudioDecoder>["configure"]>[0];
			const aac = (config: Config) => typeof config.codec === "string" && AAC.has(config.codec);
			const route = (config: Config): Config =>
				aac(config) ? { ...config, codec: { libavjs: { codec: "aac" } } } : config;

			class AudioDecoder extends libav.AudioDecoder {
				#planar = false;

				constructor(init: ConstructorParameters<typeof libav.AudioDecoder>[0]) {
					super({ ...init, output: (data) => init.output(this.#planar ? planar(data) : data) });
				}

				override configure(config: Config): void {
					this.#planar = aac(config);
					super.configure(route(config));
				}

				static override async isConfigSupported(config: Config) {
					const { supported } = await libav.AudioDecoder.isConfigSupported(route(config));
					return { supported, config };
				}
			}
			globalThis.AudioDecoder = AudioDecoder as unknown as typeof globalThis.AudioDecoder;
			return true;
		});
	}
	return await loading;
}

// libav's AAC decoder always emits planar float, but the polyfill concatenates the planes and labels
// the result interleaved "f32". Relabel it so channels read back from the right plane.
function planar<T extends AudioData>(data: T): T {
	if (data.format !== "f32") return data;
	const samples = new Float32Array(data.numberOfFrames * data.numberOfChannels);
	data.copyTo(samples, { planeIndex: 0 });
	const relabeled = new (data.constructor as typeof AudioData)({
		format: "f32-planar",
		sampleRate: data.sampleRate,
		numberOfFrames: data.numberOfFrames,
		numberOfChannels: data.numberOfChannels,
		timestamp: data.timestamp,
		data: samples,
	});
	data.close();
	return relabeled as T;
}
