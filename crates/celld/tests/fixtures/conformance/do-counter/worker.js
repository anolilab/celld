// A Durable Object per name keeps a counter in its storage.
export class Counter {
  constructor(ctx) {
    this.ctx = ctx;
  }

  async fetch(request) {
    const url = new URL(request.url);
    let count = (await this.ctx.storage.get("count")) ?? 0;
    if (url.pathname === "/inc") count += Number(url.searchParams.get("by") ?? 1);
    if (url.pathname === "/reset") count = 0;
    await this.ctx.storage.put("count", count);
    const keys = [...(await this.ctx.storage.list()).keys()];
    return Response.json({ count, keys, name: this.ctx.id.name });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") return new Response("ready");
    const name = url.searchParams.get("name") ?? "default";
    return env.COUNTER.get(env.COUNTER.idFromName(name)).fetch(request);
  },
};
