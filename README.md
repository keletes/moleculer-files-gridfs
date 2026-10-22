# moleculer-files-gridfs

GridFS driver for [`moleculer-files`](https://github.com/keletes/moleculer-files), on the **current `mongodb` driver (6.x)**:
it uses *your* `mongodb` dependency, one driver in the tree. TypeScript, compiled output and declarations included.

## Install

Not on npm; installed from a git tag (compiled output is committed on the release commit, no build step, no auth: the repo is public).

```json
"moleculer-files-gridfs": "git+https://github.com/keletes/moleculer-files-gridfs.git#vX.Y.Z",
"moleculer-files": "git+https://github.com/keletes/moleculer-files.git#vX.Y.Z",
"mongodb": "^6.21.0",
"moleculer": "^0.14.32"
```

Node >= 22. `mongodb`, `moleculer` and `moleculer-files` are peer dependencies.

## Usage

```ts
import FilesService from "moleculer-files";
import GridFs from "moleculer-files-gridfs"; // or { GridFSAdapter }

export default {
	name: "cad",
	mixins: [FilesService],
	adapter: new GridFs(process.env.MONGO_SERVER!, {}), // uri, MongoClientOptions
	collection: "cad", // GridFS bucket name, default "fs"
};
```

After connecting, `this.adapter.client`, `.db` and `.bucketFS` are the driver objects, for anything the mixin does not cover.

## Behaviour

- **Addressing and versions.** A file is addressed by `filename` (`meta.filename`, else `meta.id`, else a random UUID). Saving an existing name adds a revision
  (`metadata.version`, `"1"`, `"2"`, ...); `get` returns the newest. `meta` (minus `$multipart`) is stored as the file's `metadata`; `contentType` comes from `meta.contentType`, else the extension.
- **`find`/`count`/`list`.** Params other than `limit`, `offset`, `sort`, `query`, `fields`, ... are a GridFS filter on the `<bucket>.files` documents (for example `{ "metadata.parameters.color": "red" }`, `$or`).
  `sort` defaults to newest revision first.
- **`remove`.** By GridFS `_id` (hex string) when one matches, otherwise every revision of the file named `id`. Missing file: 404.
- **Streams.** `save` pipes with backpressure; if the source stream fails the half-written upload (chunks) is aborted. `get` returns the driver's download stream; read errors are emitted on it.
- `disconnect` closes the MongoDB client.

### Changes from 0.1.x

Same public API (default export, `bucketFS`/`db`/`client`). Fixes: `updateById` (used by the `update` action) crashed on the id string; `remove` did not await the deletion
(unhandled rejections) and never reported a missing file; `find` returned the error object instead of throwing; `count` was a stub, so `list` had no total;
`limit`/`offset`/`sort` were treated as filter fields; the client was never closed. `uuid`, `is-stream` dependencies dropped.
