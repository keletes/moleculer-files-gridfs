import { MongoClient } from "mongodb";
import { PINNED_MONGOD_VERSION } from "../pin";

// Guard: the suite must run on the mongod we pinned, built for the host architecture. These checks apply only to
// the server the suite starts itself; when MONGO_SERVER points at an external database they are skipped.
const managed = process.env.MONGO_SERVER_MANAGED === "1";
const maybe = managed ? describe : describe.skip;

const HOST_ARCH: Record<string, string> = { arm64: "aarch64", x64: "x86_64" };
const HOST_OS: Record<string, string> = { darwin: "macOS", linux: "linux" };

maybe("test mongod", () => {
	let info: any;

	beforeAll(async () => {
		const client = await MongoClient.connect(`${process.env.MONGO_SERVER}`);
		try {
			info = await client.db().admin().buildInfo();
		} finally {
			await client.close();
		}
	});

	it("is the pinned version", () => {
		expect(info.version).toBe(PINNED_MONGOD_VERSION);
	});

	it("is at least MongoDB 5.0 (driver 6 support, server-side dotted field names)", () => {
		expect(info.versionArray[0]).toBeGreaterThanOrEqual(5);
	});

	it("was built for the host architecture", () => {
		const target = String(info.buildEnvironment?.target_arch ?? "").toLowerCase();
		expect(HOST_ARCH[process.arch]).toBeDefined();
		expect(target).toBe(HOST_ARCH[process.arch]);
	});

	it("was built for the host OS", () => {
		const os = String(info.buildEnvironment?.target_os ?? "").toLowerCase();
		expect(HOST_OS[process.platform]).toBeDefined();
		expect(os).toBe(HOST_OS[process.platform].toLowerCase());
	});
});
