import { DurableObject } from "cloudflare:workers";

// Loaded code that tries every route out of its isolate. It answers what each
// attempt did, so the test can read the refusal of each one.
const OUTBOUND_CODE = `
  import { connect } from "cloudflare:sockets";
  const failure = (error) => ({
    ok: false,
    type: error instanceof TypeError,
    message: String(error?.message ?? error),
  });
  export default {
    async fetch(request) {
      const params = new URL(request.url).searchParams;
      const target = params.get("url");
      const op = params.get("op");
      try {
        if (op === "fetch") {
          const response = await fetch(target);
          return Response.json({ ok: true, status: response.status, body: await response.text() });
        }
        if (op === "upgrade") {
          const response = await fetch(target, { headers: { Upgrade: "websocket" } });
          return Response.json({ ok: true, status: response.status });
        }
        if (op === "websocket") {
          const socket = new WebSocket(target.replace(/^http/, "ws"));
          await new Promise((resolve, reject) => {
            socket.addEventListener("open", resolve, { once: true });
            socket.addEventListener("error", () => reject(new Error("websocket error event")), { once: true });
            socket.addEventListener("close", () => reject(new Error("websocket closed")), { once: true });
          });
          return Response.json({ ok: true });
        }
        if (op === "connect") {
          const socket = connect({ hostname: params.get("host"), port: Number(params.get("port")) });
          await socket.opened;
          await socket.close();
          return Response.json({ ok: true });
        }
      } catch (error) {
        return Response.json(failure(error));
      }
      return new Response("unknown op", { status: 400 });
    },
  };
`;

// A facet with data of its own. "reach" tries the routes it has to another
// object's storage and reports what each one answered.
const FACET_CODE = `
  import { DurableObject } from "cloudflare:workers";
  const attempt = async (run) => {
    try {
      return { value: await run() };
    } catch (error) {
      return { error: String(error?.message ?? error) };
    }
  };
  export class App extends DurableObject {
    async fetch(request) {
      const params = new URL(request.url).searchParams;
      const op = params.get("op");
      if (op === "seed") {
        this.ctx.storage.kv.put("secret", params.get("v"));
        this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS s (v TEXT)");
        this.ctx.storage.sql.exec("INSERT INTO s VALUES (?)", params.get("v"));
        return Response.json({ id: this.ctx.id.toString() });
      }
      if (op === "dump") {
        return Response.json(this.dump());
      }
      if (op === "reach") {
        const names = (params.get("names") ?? "").split(",").filter(Boolean);
        const out = {
          own: this.dump(),
          env: Object.keys(this.env),
          exports: Object.keys(this.ctx.exports ?? {}),
          facets: {},
        };
        for (const name of names) {
          out.facets[name] = await attempt(async () => {
            const facet = this.ctx.facets.get(name, () => ({ class: this.ctx.exports.App ?? this.constructor }));
            return await (await facet.fetch("http://facet/?op=dump")).json();
          });
        }
        out.sibling = await attempt(async () => {
          const stub = this.ctx.exports.Host.get(params.get("sibling"));
          return await (await stub.fetch("http://host/")).text();
        });
        return Response.json(out);
      }
      return new Response("unknown op", { status: 400 });
    }
    dump() {
      return {
        kv: [...this.ctx.storage.kv.list()],
        sql: this.ctx.storage.sql.exec("SELECT v FROM s").toArray(),
      };
    }
  }
`;

// An object with private storage and facets of its own.
export class Host extends DurableObject {
  facet(name) {
    const worker = this.env.LOADER.get("app-v1", () => ({
      compatibilityDate: "2025-01-01",
      mainModule: "app.js",
      modules: { "app.js": FACET_CODE },
    }));
    return this.ctx.facets.get(name, () => ({
      class: worker.getDurableObjectClass("App"),
    }));
  }

  async fetch(request) {
    const url = new URL(request.url);
    const facet = url.searchParams.get("facet");
    if (facet) {
      return this.facet(facet).fetch(request);
    }
    if (url.searchParams.get("op") === "seed") {
      const value = url.searchParams.get("v");
      this.ctx.storage.kv.put("secret", value);
      this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS s (v TEXT)");
      this.ctx.storage.sql.exec("INSERT INTO s VALUES (?)", value);
    }
    return Response.json({
      id: this.ctx.id.toString(),
      secret: this.ctx.storage.kv.get("secret") ?? null,
    });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    // /host/<name>: a request for the object of that name.
    if (url.pathname.startsWith("/host/")) {
      const name = url.pathname.slice("/host/".length);
      return env.HOST.getByName(name).fetch(`http://host/${url.search}`);
    }
    // /loaded/<mode>: loaded code with `globalOutbound: null` or the default.
    if (url.pathname.startsWith("/loaded/")) {
      const mode = url.pathname.slice("/loaded/".length);
      const worker = env.LOADER.get(`outbound-${mode}`, () => ({
        compatibilityDate: "2025-01-01",
        mainModule: "outbound.js",
        modules: { "outbound.js": OUTBOUND_CODE },
        ...(mode === "null" ? { globalOutbound: null } : {}),
      }));
      return worker.getEntrypoint().fetch(`http://loaded/${url.search}`);
    }
    return new Response("not found", { status: 404 });
  },
};
