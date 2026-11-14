import { DurableObject, RpcTarget, WorkflowEntrypoint } from "cloudflare:workers";

const post = (body) => ({ method: "POST", body: JSON.stringify(body) });

// A capability to one cell's storage, handed to whoever asks.
class Handle extends RpcTarget {
  constructor(storage) {
    super();
    this.storage = storage;
  }
  async bump() {
    const n = ((await this.storage.get("n")) ?? 0) + 1;
    await this.storage.put("n", n);
    return n;
  }
}

// A cell with data of its own.
export class Vault extends DurableObject {
  async fetch() {
    return new Response((await this.ctx.storage.get("secret")) ?? "none");
  }
  handle() {
    return new Handle(this.ctx.storage);
  }
  // Starts a query and a key listing, and leaves both unread.
  leakCursors() {
    this.ctx.storage.sql.exec("INSERT INTO s VALUES ('second'), ('third')");
    globalThis.__leakedSql = this.ctx.storage.sql.exec("SELECT v FROM s");
    globalThis.__leakedList = this.ctx.storage.kv.list();
  }
  async hold() {
    await this.ctx.blockConcurrencyWhile(async () => {
      await this.ctx.storage.put("held", true);
    });
    return await this.ctx.storage.get("held");
  }
  async transact() {
    await this.ctx.storage.transaction(async (txn) => {
      await txn.put("t", 1);
      await txn.put("u", 2);
    });
    return [await this.ctx.storage.get("t"), await this.ctx.storage.get("u")];
  }
  async arm() {
    await this.ctx.storage.setAlarm(Date.now() + 50);
  }
  async alarm() {
    await this.ctx.storage.put("alarmed", true);
  }
  async alarmed() {
    return (await this.ctx.storage.get("alarmed")) ?? false;
  }
  put(value) {
    this.ctx.storage.put("secret", value);
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS s (v TEXT)");
    this.ctx.storage.sql.exec("INSERT INTO s VALUES (?)", value);
    return this.ctx.id.toString();
  }
}

// A workflow runs the application's code in a cell of the engine's own class.
// Its steps try what any script's code could: a database no binding names, and
// a cell the script declared.
export class Probe extends WorkflowEntrypoint {
  async run(event, step) {
    return await step.do("probe", async () => {
      const Namespace = this.env.VAULT.constructor;
      const namespace = new Namespace("__D1Database", "cells:v1:d1:__D1Database");
      let undeclared;
      try {
        const response = await namespace.getByName("other").fetch(
          "http://d1/",
          post({ exec: { sql: "SELECT 1 AS reached" } }),
        );
        undeclared = { status: response.status };
      } catch (error) {
        undeclared = { error: String(error) };
      }
      const declared = await (await this.env.VAULT.getByName("v").fetch("http://v/")).text();
      return { undeclared, declared };
    });
  }
}

const FACET_CODE = `
  import { DurableObject } from "cloudflare:workers";
  export class App extends DurableObject {
    async fetch() {
      const n = (this.ctx.storage.kv.get("n") ?? 0) + 1;
      this.ctx.storage.kv.put("n", n);
      this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS f (v)");
      this.ctx.storage.sql.exec("INSERT INTO f VALUES (?)", n);
      return new Response(String(n));
    }
  }
`;

// An object that keeps its data in a facet it loaded.
export class Host extends DurableObject {
  fetch(request) {
    const worker = this.env.LOADER.get("app-v1", () => ({
      compatibilityDate: "2025-01-01",
      mainModule: "app.js",
      modules: { "app.js": FACET_CODE },
    }));
    const facet = this.ctx.facets.get("app", () => ({
      class: worker.getDurableObjectClass("App"),
    }));
    return facet.fetch(request);
  }
}

// A cell that points its storage at another cell's scope, as a script can by
// writing the scope onto the object it holds.
export class Thief extends DurableObject {
  // Reads the cursors another cell left behind.
  readLeakedSql() {
    try {
      return { rows: globalThis.__leakedSql.toArray() };
    } catch (error) {
      return { error: String(error) };
    }
  }
  readLeakedList() {
    try {
      return { entries: Array.from(globalThis.__leakedList) };
    } catch (error) {
      return { error: String(error) };
    }
  }
  // Calls another cell from inside this one, as a cell does when it composes
  // with its neighbours: a stub, then a method on what the stub returned.
  async callVault() {
    const vault = this.env.VAULT.getByName("v");
    const handle = await vault.handle();
    return [await handle.bump(), await handle.bump()];
  }
  async callVaultMethod() {
    return await this.env.VAULT.getByName("v").hold();
  }
  // Uses a capability another cell handed out.
  async useHandle(handle) {
    return await handle.bump();
  }
  // Keeps a stub in storage, and calls it from a later event.
  async remember(stub) {
    await this.ctx.storage.put("stub", stub);
  }
  async callRemembered() {
    const stub = await this.ctx.storage.get("stub");
    return await (await stub.fetch("http://v/")).text();
  }
  async readKv(scope) {
    const storage = this.ctx.storage;
    storage._scope = scope;
    try {
      return { value: (await storage.get("secret")) ?? null };
    } catch (error) {
      return { error: String(error) };
    }
  }
  readSql(scope) {
    const sql = this.ctx.storage.sql;
    sql._scope = scope;
    try {
      return { rows: sql.exec("SELECT v FROM s").toArray() };
    } catch (error) {
      return { error: String(error) };
    }
  }
  async writeKv(scope, value) {
    const storage = this.ctx.storage;
    storage._scope = scope;
    try {
      await storage.put("secret", value);
      return { ok: true };
    } catch (error) {
      return { error: String(error) };
    }
  }
}


export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const arg = url.searchParams.get("v") ?? "";
    if (url.pathname === "/__ready") return new Response("ready");

    if (url.pathname === "/vault/set") {
      await env.VAULT.getByName("v").put(arg);
      return Response.json({ scope: env.VAULT.idFromName("v")._scope() });
    }
    if (url.pathname === "/vault/get") {
      return new Response(await (await env.VAULT.getByName("v").fetch("http://v/")).text());
    }

    // ---- ordinary use that scoping must not break ----
    if (url.pathname === "/ok/handle") {
      const handle = await env.VAULT.getByName("v").handle();
      return Response.json([await handle.bump(), await handle.bump()]);
    }
    if (url.pathname === "/ok/handle-via-cell") {
      const handle = await env.VAULT.getByName("v").handle();
      return Response.json(await env.THIEF.getByName("t").useHandle(handle));
    }
    if (url.pathname === "/ok/cell-calls-cell-handle") {
      try {
        return Response.json(await env.THIEF.getByName("t").callVault());
      } catch (error) {
        return Response.json({ error: String(error) });
      }
    }
    if (url.pathname === "/ok/cell-calls-cell") {
      return Response.json(await env.THIEF.getByName("t").callVaultMethod());
    }
    if (url.pathname === "/workflow/probe") {
      const instance = await env.FLOW.create({});
      for (let i = 0; i < 100; i++) {
        const status = await instance.status();
        if (status.status === "complete") return Response.json(status.output);
        if (status.status === "errored") return Response.json({ errored: status.error });
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      return Response.json({ timeout: true });
    }
    if (url.pathname === "/ok/remember") {
      await env.THIEF.getByName("t").remember(env.VAULT.getByName("v"));
      return new Response("remembered");
    }
    if (url.pathname === "/ok/call-remembered") {
      return new Response(await env.THIEF.getByName("t").callRemembered());
    }
    if (url.pathname === "/ok/hold") return Response.json(await env.VAULT.getByName("v").hold());
    if (url.pathname === "/ok/transact") return Response.json(await env.VAULT.getByName("v").transact());
    if (url.pathname === "/ok/arm") {
      await env.VAULT.getByName("v").arm();
      return new Response("armed");
    }
    if (url.pathname === "/ok/alarmed") return Response.json(await env.VAULT.getByName("v").alarmed());
    if (url.pathname === "/ok/facet") {
      const host = env.HOST.getByName("h");
      await host.fetch("http://h/");
      return new Response(await (await host.fetch("http://h/")).text());
    }
    if (url.pathname === "/vault/leak-cursors") {
      await env.VAULT.getByName("v").leakCursors();
      return new Response("leaked");
    }
    if (url.pathname === "/thief/read-leaked-sql") {
      return Response.json(await env.THIEF.getByName("t").readLeakedSql());
    }
    if (url.pathname === "/thief/read-leaked-list") {
      return Response.json(await env.THIEF.getByName("t").readLeakedList());
    }
    if (url.pathname === "/thief/read-kv") {
      return Response.json(await env.THIEF.getByName("t").readKv(arg));
    }
    if (url.pathname === "/thief/read-sql") {
      return Response.json(await env.THIEF.getByName("t").readSql(arg));
    }
    if (url.pathname === "/thief/write-kv") {
      return Response.json(await env.THIEF.getByName("t").writeKv(arg, "overwritten"));
    }

    // The binding, for comparison.
    if (url.pathname === "/d1/own") {
      await env.DB.exec("CREATE TABLE IF NOT EXISTS t (x)");
      return Response.json({ n: await env.DB.prepare("SELECT count(*) AS n FROM t").first("n") });
    }
    // A database this script never declared, reached by building the
    // namespace the binding is made from.
    if (url.pathname === "/d1/fabricated") {
      try {
        const Namespace = env.VAULT.constructor;
        const namespace = new Namespace("__D1Database", "cells:v1:d1:__D1Database");
        const response = await namespace.getByName(arg).fetch(
          "http://d1/",
          post({ exec: { sql: "CREATE TABLE IF NOT EXISTS t (x); SELECT 1 AS reached" } }),
        );
        return Response.json({ status: response.status, body: await response.text() });
      } catch (error) {
        return Response.json({ error: String(error) });
      }
    }
    // A cell of a class this script declared, under a namespace key of
    // another script. Only the namespace the class belongs to derives its ids.
    if (url.pathname === "/vault/foreign-key") {
      try {
        const Namespace = env.VAULT.constructor;
        const namespace = new Namespace("Vault", "cells:v1:5:other:Vault");
        // Without the name, so nothing checks the id against the class's own key.
        const id = namespace.idFromString(namespace.idFromName("x").toString());
        const response = await namespace.get(id).fetch("http://v/");
        return Response.json({ status: response.status, body: await response.text() });
      } catch (error) {
        return Response.json({ error: String(error) });
      }
    }
    // The same database, through a namespace built by hand: allowed, because
    // the binding names it.
    if (url.pathname === "/d1/fabricated-declared") {
      const Namespace = env.VAULT.constructor;
      const namespace = new Namespace("__D1Database", "cells:v1:d1:__D1Database");
      const response = await namespace.getByName("ledger-id").fetch(
        "http://d1/",
        post({ exec: { sql: "CREATE TABLE IF NOT EXISTS t (x); SELECT 1 AS reached" } }),
      );
      return Response.json({ status: response.status });
    }
    return new Response("not found", { status: 404 });
  },
};
