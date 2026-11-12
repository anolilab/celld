import { DurableObject } from "cloudflare:workers";

export class Cell extends DurableObject {
  async fetch() {
    const n = ((await this.ctx.storage.get("n")) ?? 0) + 1;
    await this.ctx.storage.put("n", n);
    return new Response(String(n));
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    return env.CELL.get(env.CELL.idFromName("counter")).fetch(request);
  },
};
