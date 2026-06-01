"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const mongodb_1 = require("mongodb");
const mime_types_1 = __importDefault(require("mime-types"));
const uuid_1 = require("uuid");
const is_stream_1 = __importDefault(require("is-stream"));
const moleculer_1 = require("moleculer");
const promises_1 = require("node:stream/promises");
const { MoleculerError, ServiceSchemaError } = moleculer_1.Errors;
const mongoClient = mongodb_1.MongoClient;
class FSAdapter {
    constructor(uri, opts) {
        this.uri = uri;
        this.opts = opts;
    }
    init(broker, service) {
        this.broker = broker;
        this.service = service;
        this.bucketName = this.service.schema.collection || "fs";
        if (!this.uri) {
            throw new ServiceSchemaError("Missing `uri` definition!", undefined);
        }
    }
    async connect() {
        this.client = new mongoClient(this.uri, this.opts);
        return this.client.connect().then(() => {
            this.db = this.client.db(this.dbName ?? this.client.options.dbName);
            this.bucketFS = new mongodb_1.GridFSBucket(this.db, {
                bucketName: this.bucketName,
            });
            this.service.logger.info("GridFS adapter has connected successfully.");
            this.client.on("close", () => this.service.logger.warn("MongoDB adapter has disconnected."));
            this.client.on("error", (err) => this.service.logger.error("MongoDB error.", err));
            this.client.on("reconnect", () => this.service.logger.info("MongoDB adapter has reconnected."));
        });
    }
    disconnect() {
        return Promise.resolve();
    }
    async find(filters) {
        try {
            return await this.bucketFS.find(filters).sort({ "metadata.version": -1 }).toArray();
        }
        catch (error) {
            return error;
        }
    }
    findOne(query) {
        return new Promise(async (resolve, reject) => {
            try {
                const file = await this.bucketFS.find(query).sort({ "metadata.version": -1 }).toArray();
                if (file.length > 0)
                    resolve(this.bucketFS.openDownloadStream(file[0]._id));
                else
                    resolve(undefined);
            }
            catch (error) {
                reject(error);
            }
        });
    }
    findById(fd) {
        return new Promise(async (resolve, reject) => {
            try {
                const file = await this.bucketFS.find({ filename: fd }).sort({ "metadata.version": -1 }).toArray();
                if (file.length > 0)
                    resolve(this.bucketFS.openDownloadStreamByName(fd));
                else
                    resolve(undefined);
            }
            catch (error) {
                reject(error);
            }
        });
    }
    async count(_filter = {}) {
        this.service.logger.info("`count` is not currently implemented for GridFSAdapter.");
    }
    async save(entity, meta) {
        if (!(0, is_stream_1.default)(entity))
            throw new MoleculerError("Entity is not a stream", 400, "E_BAD_REQUEST");
        const filename = meta.id || meta.filename || (0, uuid_1.v4)();
        const contentType = meta.contentType || mime_types_1.default.lookup(String(filename));
        if (meta?.$multipart) {
            delete meta.$multipart;
        }
        try {
            meta.version = "1";
            let file = await this.bucketFS.find({ filename: String(filename) }).sort({ "metadata.version": -1 }).toArray();
            if (file.length > 0 && file[0].metadata) {
                if (file[0].metadata.version)
                    meta.version = String((parseInt(file[0].metadata.version) || 0) + 1);
            }
        }
        catch (error) { }
        let stream = this.bucketFS.openUploadStream(meta.filename, {
            metadata: meta,
            contentType: contentType,
        });
        return (0, promises_1.pipeline)(entity, stream);
    }
    async updateById(entity, meta) {
        return await this.save(entity, meta);
    }
    removeMany(_filter = {}) {
        this.service.logger.info("`removeMany` is not currently implemented for GridFSAdapter.");
    }
    async removeById(_id) {
        const id = new mongodb_1.ObjectId(_id);
        this.bucketFS.delete(id);
        return { id };
    }
    clear() {
        this.service.logger.info("`clear` is not currently implemented for GridFSAdapter.");
    }
}
exports.default = FSAdapter;
