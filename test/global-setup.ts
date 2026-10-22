import { MongoMemoryServer } from "mongodb-memory-server";
import { PINNED_MONGOD_VERSION } from "./pin";

/**
 * Starts a throwaway real `mongod` for the suite unless MONGO_SERVER already points at a database to test
 * against. The binary version is pinned explicitly (see ./pin.ts). Jest workers inherit the env set here.
 */
export default async function globalSetup() {
	if (process.env.MONGO_SERVER) return;
	const mongod = await MongoMemoryServer.create({ binary: { version: PINNED_MONGOD_VERSION } });
	(globalThis as any).__MONGOD__ = mongod;
	process.env.MONGO_SERVER = mongod.getUri("keletes");
	process.env.MONGO_SERVER_MANAGED = "1"; // started by us: the version/arch guard applies
}
