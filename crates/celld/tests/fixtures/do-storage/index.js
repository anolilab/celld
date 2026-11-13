import { DurableObject } from "cloudflare:workers";

const entries = (map) => Object.fromEntries(map);

const scenarios = {
  async crud(storage) {
    await storage.put("a", 1);
    const single = await storage.get("a");
    const missing = await storage.get("nope");
    await storage.put({ b: 2, c: 3 });
    const many = await storage.get(["a", "b", "c", "nope"]);
    return {
      single,
      missingIsUndefined: missing === undefined,
      many: entries(many),
      manyIsMap: many instanceof Map,
    };
  },
  async deletes(storage) {
    await storage.put({ a: 1, b: 2, c: 3 });
    const existing = await storage.delete("a");
    const absent = await storage.delete("a");
    const count = await storage.delete(["b", "c", "never-existed"]);
    return { existing, absent, count, left: (await storage.list()).size };
  },
  async order(storage) {
    // The last two differ between UTF-8 and UTF-16 order: U+FF5E sorts before
    // an astral code point in UTF-8 but after its surrogate pair in UTF-16.
    const keys = ["b", "a", "B", "aa", "a/b", "é", "\u{1F600}", "～"];
    for (const key of keys) await storage.put(key, true);
    return { keys: [...(await storage.list()).keys()] };
  },
  async listOptions(storage) {
    for (let i = 0; i < 10; i++) await storage.put("k" + i, i);
    await storage.put("j", "outside");
    await storage.put("l", "outside");
    const keys = async (options) => [...(await storage.list(options)).keys()];
    return {
      prefix: await keys({ prefix: "k" }),
      start: await keys({ prefix: "k", start: "k7" }),
      end: await keys({ prefix: "k", end: "k3" }),
      startEnd: await keys({ start: "k3", end: "k6" }),
      startAfter: await keys({ prefix: "k", startAfter: "k7" }),
      limit: await keys({ prefix: "k", limit: 3 }),
      reverse: await keys({ prefix: "k", reverse: true, limit: 3 }),
      reverseRange: await keys({ start: "k3", end: "k6", reverse: true }),
      all: await keys(),
    };
  },
  async deleteAll(storage) {
    await storage.put({ a: 1, b: 2 });
    await storage.deleteAll();
    return { size: (await storage.list()).size, a: (await storage.get("a")) ?? null };
  },
  async rollback(storage) {
    await storage.put("x", 1);
    let message = null;
    try {
      await storage.transaction(async (txn) => {
        await txn.put("x", 2);
        await txn.put("y", 3);
        throw new Error("boom");
      });
    } catch (error) {
      message = error.message;
    }
    const committed = await storage.transaction(async (txn) => {
      await txn.put("z", 9);
      return "returned";
    });
    return {
      message,
      x: await storage.get("x"),
      y: (await storage.get("y")) ?? null,
      z: await storage.get("z"),
      committed,
    };
  },
  async explicitRollback(storage) {
    await storage.put("x", 1);
    await storage.transaction(async (txn) => {
      await txn.put("x", 2);
      txn.rollback();
    });
    return { x: await storage.get("x") };
  },
  async clone(storage) {
    await storage.put("map", new Map([["k", { n: 1 }]]));
    await storage.put("date", new Date(1700000000000));
    await storage.put("bytes", new Uint8Array([1, 2, 255]));
    await storage.put("nested", { list: [1, "two", null], when: new Date(5) });
    const map = await storage.get("map");
    const date = await storage.get("date");
    const bytes = await storage.get("bytes");
    const nested = await storage.get("nested");
    return {
      isMap: map instanceof Map,
      mapValue: map.get("k"),
      isDate: date instanceof Date,
      time: date.getTime(),
      isBytes: bytes instanceof Uint8Array,
      bytes: [...bytes],
      nestedList: nested.list,
      nestedIsDate: nested.when instanceof Date,
    };
  },
  async write(storage) {
    await storage.put({ one: 1, two: [2], three: { three: 3 } });
    return { written: true };
  },
  async read(storage) {
    return entries(await storage.get(["one", "two", "three"]));
  },
};

export class Cell extends DurableObject {
  async fetch(request) {
    const name = new URL(request.url).pathname.slice(1);
    const scenario = scenarios[name];
    if (!scenario) return new Response("unknown scenario " + name, { status: 404 });
    return Response.json(await scenario(this.ctx.storage));
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    // One object per scenario unless the test names one with ?o=.
    const name = url.searchParams.get("o") ?? url.pathname;
    return env.OBJ.get(env.OBJ.idFromName(name)).fetch(request);
  },
};
