export default async function globalTeardown() {
	await (globalThis as any).__MONGOD__?.stop();
}
