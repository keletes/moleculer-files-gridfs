"use strict";

import { GridFSBucket, MongoClient, ObjectId } from "mongodb";
import type { Db, Filter, Document, GridFSFile, MongoClientOptions } from "mongodb";
import mime from "mime-types";
import { v4 } from "uuid";
import isStream from "is-stream";
import { Errors } from "moleculer";
import { pipeline } from 'node:stream/promises';
import type { Service, ServiceBroker } from "moleculer";
import type { AdapterSchema } from "moleculer-files";
const { MoleculerError, ServiceSchemaError } = Errors;

const mongoClient = MongoClient;

class FSAdapter implements AdapterSchema {
  broker: ServiceBroker;
  bucketFS: GridFSBucket;
  bucketName: string;
  client: MongoClient;
  db: Db;
  dbName: string;
  service: Service;
  uri: string;
  opts: MongoClientOptions;

  constructor(uri: string, opts: MongoClientOptions) {
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
    this.client = new mongoClient(
      this.uri,
      this.opts
    );
    return this.client.connect().then(() => {
      this.db = this.client.db(this.dbName ?? this.client.options.dbName);

      this.bucketFS = new GridFSBucket(this.db, {
        bucketName: this.bucketName,
      });

      this.service.logger.info("GridFS adapter has connected successfully.");

      /* istanbul ignore next */
      this.client.on("close", () =>
        this.service.logger.warn("MongoDB adapter has disconnected.")
      );
      this.client.on("error", (err) =>
        this.service.logger.error("MongoDB error.", err)
      );
      this.client.on("reconnect", () =>
        this.service.logger.info("MongoDB adapter has reconnected.")
      );
    });
  }

  disconnect() {
    return Promise.resolve();
  }

  async find(filters: Filter<GridFSFile>) {
    try {
      return await this.bucketFS.find(filters).sort( { "metadata.version": -1 } ).toArray();
    } catch (error) {
      return error;
    }
  }

  findOne(query: Filter<GridFSFile>) {
    return new Promise(async (resolve, reject) => {
      try {
        const file = await this.bucketFS.find(query).sort( { "metadata.version": -1 } ).toArray();
        if( file.length > 0 )
          resolve(this.bucketFS.openDownloadStream(file[0]._id));
        else
          resolve(undefined);
      } catch (error) {
        reject(error);
      }
    })
  }

  findById(fd: string) {
    return new Promise(async (resolve, reject) => {
      try {
        const file = await this.bucketFS.find({filename: fd}).sort( { "metadata.version": -1 } ).toArray();
        if( file.length > 0 )
          resolve(this.bucketFS.openDownloadStreamByName(fd));
        else
          resolve(undefined);
      } catch (error) {
        reject(error);
      }
    })
  }

  // TODO: Implement
  async count(_filter: Filter<GridFSFile> = {}) {
    this.service.logger.info("`count` is not currently implemented for GridFSAdapter.");
  }

  async save(entity: NodeJS.ReadableStream, meta: Document) {
    if (!isStream(entity)) throw new MoleculerError("Entity is not a stream", 400, "E_BAD_REQUEST");

    const filename = meta.id || meta.filename || v4();
    const contentType = meta.contentType || mime.lookup(String(filename));

    if (meta?.$multipart) {
      delete meta.$multipart;
    }

    // If filename exists - version it
    try {
      meta.version = "1"
      let file = await this.bucketFS.find({filename: String(filename)}).sort( { "metadata.version": -1 } ).toArray();
      // Get file latest version and increment to new file
      if( file.length > 0 && file[0].metadata ){
        if( file[0].metadata.version )
          meta.version = String( (parseInt(file[0].metadata.version) || 0) + 1 )
      }
    } catch (error) {}

    let stream = this.bucketFS.openUploadStream(meta.filename, {
      metadata: meta,
      contentType: contentType,
    });

    return pipeline(entity, stream);
  }

  async updateById(entity: NodeJS.ReadableStream, meta: Document) {
    return await this.save(entity, meta);
  }

  // TODO: Implement
  removeMany(_filter: Filter<GridFSFile> = {}) {
    this.service.logger.info("`removeMany` is not currently implemented for GridFSAdapter.");
  }

  async removeById(_id: string) {
    const id = new ObjectId(_id);
    this.bucketFS.delete(id);
    return { id };
  }

  // TODO: Implement
  clear() {
    this.service.logger.info("`clear` is not currently implemented for GridFSAdapter.");
  }
}

export default FSAdapter;
