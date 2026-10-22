/**
 * The mongod the suite runs against. Single source of truth: `config.mongodbMemoryServer.version` in package.json
 * (also read by mongodb-memory-server's own postinstall, so the binary it pre-downloads is the pinned one rather
 * than the library default). MONGOMS_VERSION overrides it, as it does for the library itself.
 */
// eslint-disable-next-line @typescript-eslint/no-var-requires
const pkg = require("../package.json");

export const PINNED_MONGOD_VERSION: string = process.env.MONGOMS_VERSION || pkg.config.mongodbMemoryServer.version;
