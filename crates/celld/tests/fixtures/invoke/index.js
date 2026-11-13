import { DurableObject } from "cloudflare:workers";

// The object keeps one counter. `/inc` raises it and any other path reads it.
export class Counter extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    let count = (await this.ctx.storage.get("count")) ?? 0;
    if (url.pathname === "/inc") {
      count += 1;
      await this.ctx.storage.put("count", count);
    }
    return new Response(String(count));
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    const stub = env.COUNTER.get(env.COUNTER.idFromName("main"));
    const count = Number(await (await stub.fetch("http://counter" + url.pathname)).text());
    return Response.json({
      code: "v1",
      greeting: env.GREETING,
      count,
      method: request.method,
      key: request.headers.get("x-api-key"),
      body: request.method === "GET" ? null : await request.text(),
    });
  },
};
