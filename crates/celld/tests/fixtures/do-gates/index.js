import { DurableObject } from "cloudflare:workers";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export class Cell extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.init = "pending";
    ctx.blockConcurrencyWhile(async () => {
      await sleep(1200);
      this.init = "done";
    });
  }

  async fetch(request) {
    const url = new URL(request.url);
    const storage = this.ctx.storage;
    if (url.pathname === "/init") {
      return Response.json({ init: this.init });
    }
    if (url.pathname === "/incr") {
      // Read, await, then write: a storage round trip sits between the two.
      const value = (await storage.get("n")) ?? 0;
      await storage.put("n", value + 1);
      return Response.json({ n: value + 1 });
    }
    return Response.json({ n: (await storage.get("n")) ?? 0 });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    const stub = env.OBJ.get(env.OBJ.idFromName(url.searchParams.get("o") ?? "default"));
    const call = (path) => stub.fetch("http://object" + path).then((r) => r.json());
    if (url.pathname === "/race") {
      const count = Number(url.searchParams.get("count") ?? 20);
      await Promise.all(Array.from({ length: count }, () => call("/incr")));
      return Response.json(await call("/get"));
    }
    if (url.pathname === "/inits") {
      return Response.json(await Promise.all([call("/init"), call("/init"), call("/init")]));
    }
    return stub.fetch(request);
  },
};
