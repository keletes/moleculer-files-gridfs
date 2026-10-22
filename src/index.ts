import { randomUUID } from "node:crypto";
import { pipeline } from "node:stream/promises";
import { GridFSBucket, MongoClient, ObjectId } from "mongodb";
import type { Db, Document, Filter, GridFSFile, MongoClientOptions, Sort } from "mongodb";
import mime from "mime-types";
import { Errors } from "moleculer";
import type { Service, ServiceBroker } from "moleculer";
import type { AdapterSchema, QueryParams } from "moleculer-files";

const { MoleculerError, ServiceSchemaError } = Errors;

/** Newest revision first. Revisions of a file share its `filename`, `metadata.version` counts them. */
const LATEST_FIRST: Sort = { "metadata.version": -1 };

const isReadable = (value: unknown): value is NodeJS.ReadableStream =>
	typeof value === "object" && value !== null && typeof (value as { pipe?: unknown }).pipe === "function";

/** Params the moleculer-files actions add on top of the GridFS filter. */
const QUERY_KEYS = ["limit", "offset", "sort", "fields", "populate", "search", "searchFields", "page", "pageSize", "query"];

/**
 * GridFS storage driver for moleculer-files.
 *
 * The bucket is named after `collection` in the service schema (default `fs`). Files are addressed by `filename`
 * and are versioned: saving an existing filename adds a revision (`metadata.version`), reads return the latest.
 */
export class GridFSAdapter implements AdapterSchema {
	broker!: ServiceBroker;
	bucketFS!: GridFSBucket;
	bucketName = "fs";
	client!: MongoClient;
	db!: Db;
	dbName?: string;
	service!: Service;
	uri: string;
	opts: MongoClientOptions;

	constructor(uri: string, opts: MongoClientOptions = {}) {
		this.uri = uri;
		this.opts = opts;
	}

	init(broker: ServiceBroker, service: Service) {
		this.broker = broker;
		this.service = service;
		this.bucketName = this.service.schema.collection || "fs";

		if (!this.uri) {
			throw new ServiceSchemaError("Missing `uri` definition!", undefined);
		}
	}

	async connect() {
		const client = new MongoClient(this.uri, this.opts);
		try {
			await client.connect();
		} catch (err) {
			await client.close().catch(() => undefined);
			throw err;
		}
		this.client = client;
		this.db = client.db(this.dbName ?? client.options.dbName);
		this.bucketFS = new GridFSBucket(this.db, { bucketName: this.bucketName });

		this.service.logger.info("GridFS adapter has connected successfully.");

		/* istanbul ignore next */
		client.on("close", () => this.service.logger.warn("MongoDB adapter has disconnected."));
		/* istanbul ignore next */
		client.on("error", (err) => this.service.logger.error("MongoDB error.", err));
	}

	async disconnect() {
		await this.client?.close();
	}

	/** Splits the action params into the GridFS filter and the paging/sorting options. */
	private parse(params: QueryParams = {}) {
		const filter: Record<string, unknown> = {};
		for (const [key, value] of Object.entries(params)) {
			if (!QUERY_KEYS.includes(key)) filter[key] = value;
		}
		Object.assign(filter, params.query);

		let sort: Sort = LATEST_FIRST;
		if (Array.isArray(params.sort) && params.sort.length > 0) {
			sort = Object.fromEntries(
				params.sort.filter(Boolean).map((s) => (s.startsWith("-") ? [s.slice(1), -1] : [s, 1])),
			);
		}

		return {
			filter: filter as Filter<GridFSFile>,
			sort,
			limit: typeof params.limit === "number" && params.limit > 0 ? params.limit : 0,
			skip: typeof params.offset === "number" && params.offset > 0 ? params.offset : 0,
		};
	}

	/** File entries (GridFS `files` documents) matching the filter in `params`. */
	async find(params?: QueryParams): Promise<GridFSFile[]> {
		const { filter, sort, limit, skip } = this.parse(params);
		return this.bucketFS.find(filter, { sort, limit, skip }).toArray();
	}

	/** Number of file entries (revisions included) matching the filter in `params`. */
	async count(params?: QueryParams): Promise<number> {
		const { filter } = this.parse(params);
		return this.db.collection(`${this.bucketName}.files`).countDocuments(filter as Document);
	}

	/** Download stream of the latest file matching the query, `undefined` when there is none. */
	async findOne(query: Filter<GridFSFile>) {
		const [file] = await this.bucketFS.find(query, { sort: LATEST_FIRST, limit: 1 }).toArray();
		return file ? this.bucketFS.openDownloadStream(file._id) : undefined;
	}

	/** Download stream of the latest revision of the file named `fd`, `undefined` when there is none. */
	async findById(fd: string | number) {
		return this.findOne({ filename: String(fd) });
	}

	/**
	 * Stores the stream as `meta.filename` (else `meta.id`, else a random UUID). An existing name gets a new
	 * revision: `metadata.version` is the previous one plus one. `meta` becomes the file's `metadata` (and is
	 * annotated with `version`).
	 */
	async save(entity: NodeJS.ReadableStream, meta: Document = {}): Promise<void> {
		if (!isReadable(entity)) throw new MoleculerError("Entity is not a stream", 400, "E_BAD_REQUEST");

		const filename = String(meta.filename || meta.id || randomUUID());
		const contentType = meta.contentType || mime.lookup(filename) || undefined;

		delete meta.$multipart;

		const [latest] = await this.bucketFS.find({ filename }, { sort: LATEST_FIRST, limit: 1 }).toArray();
		meta.version = latest ? String((parseInt(latest.metadata?.version) || 0) + 1) : "1";

		const upload = this.bucketFS.openUploadStream(filename, { metadata: meta, contentType });
		try {
			await pipeline(entity, upload);
		} catch (err) {
			// Do not leave the chunks of a half-written file behind
			await upload.abort().catch(() => undefined);
			throw err;
		}
	}

	/** New revision of the file `id` (moleculer-files passes the id; a metadata object is accepted too). */
	async updateById(entity: NodeJS.ReadableStream, id: string | number | Document) {
		const meta = typeof id === "object" && id !== null ? id : { filename: String(id) };
		return this.save(entity, meta);
	}

	/**
	 * Removes a file: by GridFS `_id` when `id` is one that exists, else every revision of the file named `id`.
	 * Resolves to `{ id }`, or `undefined` when nothing matched.
	 */
	async removeById(id: string | number | ObjectId) {
		if (id instanceof ObjectId || (typeof id === "string" && ObjectId.isValid(id))) {
			const _id = new ObjectId(id);
			const exists = await this.bucketFS.find({ _id }, { limit: 1 }).hasNext();
			if (exists) {
				await this.bucketFS.delete(_id);
				return { id: _id };
			}
		}
		const files = await this.bucketFS.find({ filename: String(id) }).toArray();
		if (files.length === 0) return undefined;
		for (const file of files) await this.bucketFS.delete(file._id);
		return { id };
	}

	// TODO: Implement
	removeMany(_filter: Filter<GridFSFile> = {}) {
		this.service.logger.info("`removeMany` is not currently implemented for GridFSAdapter.");
	}

	// TODO: Implement
	clear() {
		this.service.logger.info("`clear` is not currently implemented for GridFSAdapter.");
	}
}

export default GridFSAdapter;
