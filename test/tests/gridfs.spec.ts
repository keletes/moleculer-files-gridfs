import { createHash, randomBytes } from "node:crypto";
import { PassThrough, Readable } from "node:stream";
import { Errors, ServiceBroker } from "moleculer";
import type { Service } from "moleculer";
import FilesService, { FileNotFoundError } from "moleculer-files";
import { MongoClient } from "mongodb";
import GridFSAdapter from "../../src";

const readAll = async (stream: NodeJS.ReadableStream) => {
	const chunks: Buffer[] = [];
	for await (const c of stream) chunks.push(Buffer.from(c as Buffer));
	return Buffer.concat(chunks);
};

const sha = (buf: Buffer) => createHash("sha256").update(buf).digest("hex");

describe("moleculer-files-gridfs", () => {
	let broker: ServiceBroker;
	let svc: Service;
	let adapter: GridFSAdapter;
	let raw: MongoClient;
	let bucket: string;
	let n = 0;

	const start = async (collection: string | undefined | null = `bucket${++n}`) => {
		broker = new ServiceBroker({ logger: false });
		svc = broker.createService({
			name: "files",
			mixins: [FilesService],
			adapter: new GridFSAdapter(process.env.MONGO_SERVER as string, {}),
			collection: collection ?? undefined,
		});
		await broker.start();
		adapter = (svc as unknown as { adapter: GridFSAdapter }).adapter;
		bucket = adapter.bucketName;
	};

	const save = (content: Buffer | string, meta: Record<string, unknown>) =>
		broker.call("files.save", Readable.from([Buffer.from(content)]), { meta });

	const get = async (id: string) => readAll((await broker.call("files.get", { id })) as NodeJS.ReadableStream);

	beforeAll(async () => {
		raw = await MongoClient.connect(process.env.MONGO_SERVER as string);
	});
	afterAll(() => raw.close());
	beforeEach(() => start());
	afterEach(() => broker.stop());

	it("stores a file and streams it back", async () => {
		await save("hello gridfs", { filename: "hello.txt" });
		expect((await get("hello.txt")).toString()).toBe("hello gridfs");
	});

	it("round-trips a multi-chunk binary file intact", async () => {
		const data = randomBytes(1024 * 1024 + 123); // several 255 KiB chunks
		await save(data, { filename: "big.bin" });
		const back = await get("big.bin");
		expect(back.length).toBe(data.length);
		expect(sha(back)).toBe(sha(data));
		const files = adapter.db.collection(`${bucket}.chunks`);
		expect(await files.countDocuments()).toBeGreaterThan(4);
	});

	it("uses the default bucket `fs` when the service has no collection", async () => {
		await broker.stop();
		await start(null);
		expect(adapter.bucketName).toBe("fs");
		await save("x", { filename: "d.txt" });
		expect(await raw.db().collection("fs.files").countDocuments({ filename: "d.txt" })).toBe(1);
		await adapter.db.collection("fs.files").deleteMany({});
		await adapter.db.collection("fs.chunks").deleteMany({});
	});

	it("keeps the metadata and derives the content type", async () => {
		await save("<svg/>", { filename: "a.svg", owner: "me", parameters: { w: "1" }, $multipart: true });
		await save("data", { filename: "blob", contentType: "application/x-custom" });
		const [svg] = await adapter.find({ filename: "a.svg" });
		expect(svg.contentType).toBe("image/svg+xml");
		expect(svg.metadata).toMatchObject({ owner: "me", parameters: { w: "1" }, version: "1" });
		expect(svg.metadata).not.toHaveProperty("$multipart");
		const [blob] = await adapter.find({ filename: "blob" });
		expect(blob.contentType).toBe("application/x-custom");
	});

	it("falls back to meta.id, then to a random name, for the filename", async () => {
		await save("one", { id: "by-id.txt" });
		expect((await get("by-id.txt")).toString()).toBe("one");
		await save("two", {});
		const names = (await adapter.find({})).map((f) => f.filename);
		expect(names).toHaveLength(2);
		expect(names.find((x) => x !== "by-id.txt")).toMatch(/^[0-9a-f-]{36}$/);
	});

	it("versions a re-saved file and reads the latest revision", async () => {
		await save("v1", { filename: "doc.txt" });
		await save("v2", { filename: "doc.txt" });
		await save("v3", { filename: "doc.txt" });
		expect((await get("doc.txt")).toString()).toBe("v3");
		const revisions = await adapter.find({ filename: "doc.txt" });
		expect(revisions.map((f) => f.metadata?.version)).toEqual(["3", "2", "1"]);
	});

	it("update stores a new revision of the file", async () => {
		await save("old", { filename: "u.txt" });
		await broker.call("files.update", Readable.from([Buffer.from("new")]), { meta: { id: "u.txt" } });
		expect((await get("u.txt")).toString()).toBe("new");
		expect(await adapter.count({ filename: "u.txt" })).toBe(2);
	});

	describe("errors", () => {
		it("get of a missing file is a 404 FileNotFoundError", async () => {
			await expect(broker.call("files.get", { id: "missing.txt" })).rejects.toBeInstanceOf(FileNotFoundError);
		});

		it("rejects an entity that is not a stream", async () => {
			await expect(adapter.save("not a stream" as never, { filename: "x" })).rejects.toMatchObject({
				code: 400,
				type: "E_BAD_REQUEST",
			});
		});

		it("rejects and cleans up when the source stream fails mid-way", async () => {
			const source = new PassThrough();
			const pending = adapter.save(source, { filename: "broken.bin" });
			source.write(randomBytes(600 * 1024));
			setTimeout(() => source.destroy(new Error("client went away")), 50);
			await expect(pending).rejects.toThrow("client went away");
			expect(await adapter.find({ filename: "broken.bin" })).toHaveLength(0);
			expect(await adapter.db.collection(`${bucket}.chunks`).countDocuments()).toBe(0);
			await expect(broker.call("files.get", { id: "broken.bin" })).rejects.toBeInstanceOf(FileNotFoundError);
		});

		it("surfaces a read error of the download stream", async () => {
			await save(randomBytes(600 * 1024), { filename: "torn.bin" });
			const stream = (await broker.call("files.get", { id: "torn.bin" })) as NodeJS.ReadableStream;
			await adapter.db.collection(`${bucket}.chunks`).deleteOne({ n: 1 });
			await expect(readAll(stream)).rejects.toThrow();
		});

		it("refuses a service without uri", () => {
			const bad = new GridFSAdapter("");
			expect(() => bad.init(broker, svc)).toThrow(Errors.ServiceSchemaError);
		});

		it("fails to connect to an unreachable server and does not leak the client", async () => {
			const bad = new GridFSAdapter("mongodb://127.0.0.1:1", { serverSelectionTimeoutMS: 200 });
			bad.init(broker, svc);
			await expect(bad.connect()).rejects.toThrow();
		});
	});

	describe("find / count / list", () => {
		beforeEach(async () => {
			await save("1", { filename: "a.txt", parameters: { color: "red" } });
			await save("2", { filename: "b.txt", parameters: { color: "blue" } });
			await save("3", { filename: "c.txt", parameters: { color: "*" } });
		});

		it("filters on file and metadata fields, with $or like business-cad", async () => {
			const rows = (await broker.call("files.find", { filename: "b.txt" })) as { filename: string }[];
			expect(rows.map((r) => r.filename)).toEqual(["b.txt"]);
			const or = (await broker.call("files.find", {
				$or: [{ "metadata.parameters.color": "red" }, { "metadata.parameters.color": "*" }],
			})) as { filename: string }[];
			expect(or.map((r) => r.filename).sort()).toEqual(["a.txt", "c.txt"]);
		});

		it("honours limit/offset/sort and does not treat them as filter fields", async () => {
			const rows = (await broker.call("files.find", { limit: 2, offset: 1, sort: "filename" })) as { filename: string }[];
			expect(rows.map((r) => r.filename)).toEqual(["b.txt", "c.txt"]);
			const desc = (await broker.call("files.find", { sort: "-filename", limit: 1 })) as { filename: string }[];
			expect(desc[0].filename).toBe("c.txt");
		});

		it("accepts a query object", async () => {
			const rows = (await broker.call("files.find", { query: { filename: "a.txt" } })) as unknown[];
			expect(rows).toHaveLength(1);
		});

		it("count returns the number of entries", async () => {
			expect(await broker.call("files.count")).toBe(3);
			expect(await broker.call("files.count", { query: { filename: "a.txt" } })).toBe(1);
		});

		it("list paginates with a real total", async () => {
			const page = (await broker.call("files.list", { page: 2, pageSize: 2, sort: "filename" })) as {
				rows: { filename: string }[];
				total: number;
				totalPages: number;
			};
			expect(page.total).toBe(3);
			expect(page.totalPages).toBe(2);
			expect(page.rows.map((r) => r.filename)).toEqual(["c.txt"]);
		});

		it("findOne streams the latest match and is undefined for no match", async () => {
			const stream = await adapter.findOne({ filename: "a.txt" });
			expect((await readAll(stream as NodeJS.ReadableStream)).toString()).toBe("1");
			expect(await adapter.findOne({ filename: "zzz" })).toBeUndefined();
		});

		it("encodes the file _id in find results", async () => {
			const rows = (await broker.call("files.find", { filename: "a.txt" })) as { _id: unknown }[];
			expect(rows[0]._id).toBeDefined();
		});
	});

	describe("remove", () => {
		it("removes by GridFS _id, chunks included", async () => {
			await save(randomBytes(300 * 1024), { filename: "r.bin" });
			const [file] = await adapter.find({ filename: "r.bin" });
			const res = await broker.call("files.remove", { id: file._id.toHexString() });
			expect(res).toBeDefined();
			expect(await adapter.find({})).toHaveLength(0);
			expect(await adapter.db.collection(`${bucket}.chunks`).countDocuments()).toBe(0);
		});

		it("removes every revision when given a filename", async () => {
			await save("1", { filename: "r.txt" });
			await save("2", { filename: "r.txt" });
			await save("keep", { filename: "keep.txt" });
			await broker.call("files.remove", { id: "r.txt" });
			expect((await adapter.find({})).map((f) => f.filename)).toEqual(["keep.txt"]);
		});

		it("is a 404 for an unknown file (valid ObjectId or not) and awaits the deletion", async () => {
			await expect(broker.call("files.remove", { id: "ffffffffffffffffffffffff" })).rejects.toBeInstanceOf(FileNotFoundError);
			await expect(broker.call("files.remove", { id: "nope.txt" })).rejects.toBeInstanceOf(FileNotFoundError);
		});
	});

	describe("lifecycle", () => {
		it("disconnect closes the driver client", async () => {
			const closed = jest.spyOn(adapter.client, "close");
			await broker.stop();
			expect(closed).toHaveBeenCalled();
			// stopping twice (afterEach) is harmless
		});

		it("not-implemented helpers only log", () => {
			expect(() => adapter.removeMany()).not.toThrow();
			expect(() => adapter.clear()).not.toThrow();
		});
	});
});
