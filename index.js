"use strict";

const Mongo = require("mongodb");
const ObjectId = require("mongodb").ObjectId;
const mime = require("mime-types");
const uuidv4 = require("uuid/v4");
const fs = require("fs");
const isStream = require("is-stream");
const { MoleculerError, ServiceSchemaError } = require("moleculer").Errors;

const MongoClient = Mongo.MongoClient;

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
      throw new ServiceSchemaError("Missing `uri` definition!");
    }
  }

  async connect() {
    this.client = new MongoClient(
      this.uri,
      Object.assign({ useUnifiedTopology: true }, this.opts)
    );
    return this.client.connect().then(() => {
      this.db = this.client.db(this.dbName);

      this.bucketFS = new Mongo.GridFSBucket(this.db, {
        bucketName: this.bucketName,
      });

      this.service.logger.info("GridFS adapter has connected successfully.");

      /* istanbul ignore next */
      this.db.on("close", () =>
        this.service.logger.warn("MongoDB adapter has disconnected.")
      );
      this.db.on("error", (err) =>
        this.service.logger.error("MongoDB error.", err)
      );
      this.db.on("reconnect", () =>
        this.service.logger.info("MongoDB adapter has reconnected.")
      );
    });
  }

  disconnect() {
    return Promise.resolve();
  }

  async find(filters) {
    try {
      return await this.bucketFS.find(filters).sort( { "metadata.version": -1 } ).toArray();
    } catch (error) {
      return error;
    }
  }

  findOne(query) {
    // To be implemented
    return;
  }

  findById(fd) {
    return new Promise(async (resolve, reject) => {
      try {
        const file = await this.bucketFS.find({filename: fd}).sort( { "metadata.version": -1 } ).toArray();
        if( file.length > 0 )
          resolve(this.bucketFS.openDownloadStreamByName(fd));
        else
          reject(new MoleculerError("CAD file not found", 404, "ERR_NOT_FOUND"));
      } catch (error) {
        reject(error);
      }
    })
  }

  async count(filters = {}) {
    // To be implemented
    return;
  }

  async save(entity, meta) {
    return new Promise(async(resolve, reject) => {
      if (!isStream(entity)) reject(new MoleculerError("Entity is not a stream", 400, "E_BAD_REQUEST"));

      const filename = meta.id || meta.filename || uuidv4();
      const contentType = meta.contentType || mime.lookup(filename);

      if (meta?.$multipart) {
        delete meta.$multipart;
      }

      // If filename exists - version it
      try {
        meta.version = "1"
        let file = await this.bucketFS.find({filename: filename}).sort( { "metadata.version": -1 } ).toArray();
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

      return await entity
        .pipe(stream)
        .on("error", function (error) {
          reject(error);
        })
        .on("finish", function (response) {
          resolve(response);
        });
    })
  }

  async updateById(entity, meta) {
    return await this.save(entity, meta);
  }

  removeMany(query) {
    // To Be Implemented.
  }

  async removeById(_id) {
    _id = new ObjectId(_id);
    this.bucketFS.delete(_id);
    return { id: _id };
  }

  clear() {
    // To be implemented
    return;
  }
}

module.exports = FSAdapter;
