import { DurableObject } from "cloudflare:workers";

export class Counter extends DurableObject {
  async next() {
    const value = ((await this.ctx.storage.get("n")) ?? 0) + 1;
    await this.ctx.storage.put("n", value);
    return value;
  }
}

// Reached only through the primary's service binding.
const VERSION = "v1";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    if (url.pathname === "/key") {
      return new Response(await env.KEY.get());
    }
    if (url.pathname === "/count") {
      return new Response(String(await env.COUNTER.getByName("c").next()));
    }
    if (url.pathname === "/version") {
      return new Response(VERSION);
    }
    return new Response(env.GREETING ?? "no greeting");
  },
};
