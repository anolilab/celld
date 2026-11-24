import { DurableObject } from "cloudflare:workers";

export class Cell extends DurableObject {
  async bump() {
    const n = ((await this.ctx.storage.get("n")) ?? 0) + 1;
    await this.ctx.storage.put("n", n);
    return n;
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    // A new cell per request, written once before the answer.
    if (url.pathname === "/new") {
      const id = env.OBJ.newUniqueId();
      const n = await env.OBJ.get(id).bump();
      return Response.json({ id: id.toString(), n });
    }
    return new Response("unknown", { status: 404 });
  },
};
