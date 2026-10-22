import { GridFSBucket, MongoClient, ObjectId } from "mongodb";
import type { Db, Document, Filter, GridFSFile, MongoClientOptions } from "mongodb";
import type { Service, ServiceBroker } from "moleculer";
import type { AdapterSchema, QueryParams } from "moleculer-files";
/**
 * GridFS storage driver for moleculer-files.
 *
 * The bucket is named after `collection` in the service schema (default `fs`). Files are addressed by `filename`
 * and are versioned: saving an existing filename adds a revision (`metadata.version`), reads return the latest.
 */
export declare class GridFSAdapter implements AdapterSchema {
    broker: ServiceBroker;
    bucketFS: GridFSBucket;
    bucketName: string;
    client: MongoClient;
    db: Db;
    dbName?: string;
    service: Service;
    uri: string;
    opts: MongoClientOptions;
    constructor(uri: string, opts?: MongoClientOptions);
    init(broker: ServiceBroker, service: Service): void;
    connect(): Promise<void>;
    disconnect(): Promise<void>;
    /** Splits the action params into the GridFS filter and the paging/sorting options. */
    private parse;
    /** File entries (GridFS `files` documents) matching the filter in `params`. */
    find(params?: QueryParams): Promise<GridFSFile[]>;
    /** Number of file entries (revisions included) matching the filter in `params`. */
    count(params?: QueryParams): Promise<number>;
    /** Download stream of the latest file matching the query, `undefined` when there is none. */
    findOne(query: Filter<GridFSFile>): Promise<import("mongodb").GridFSBucketReadStream | undefined>;
    /** Download stream of the latest revision of the file named `fd`, `undefined` when there is none. */
    findById(fd: string | number): Promise<import("mongodb").GridFSBucketReadStream | undefined>;
    /**
     * Stores the stream as `meta.filename` (else `meta.id`, else a random UUID). An existing name gets a new
     * revision: `metadata.version` is the previous one plus one. `meta` becomes the file's `metadata` (and is
     * annotated with `version`).
     */
    save(entity: NodeJS.ReadableStream, meta?: Document): Promise<void>;
    /** New revision of the file `id` (moleculer-files passes the id; a metadata object is accepted too). */
    updateById(entity: NodeJS.ReadableStream, id: string | number | Document): Promise<void>;
    /**
     * Removes a file: by GridFS `_id` when `id` is one that exists, else every revision of the file named `id`.
     * Resolves to `{ id }`, or `undefined` when nothing matched.
     */
    removeById(id: string | number | ObjectId): Promise<{
        id: string | number | ObjectId;
    } | undefined>;
    removeMany(_filter?: Filter<GridFSFile>): void;
    clear(): void;
}
export default GridFSAdapter;
