import { DurableObject } from "cloudflare:workers";

// Keeps the bodies of the batches this script's queue handler received.
export class InboxP extends DurableObject {
  async add(bodies) {
    const seen = (await this.ctx.storage.get("seen")) ?? [];
    await this.ctx.storage.put("seen", seen.concat(bodies));
  }
  async seen() {
    return (await this.ctx.storage.get("seen")) ?? [];
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    if (url.pathname.startsWith("/send/")) {
      await env.JOBS.send(url.pathname.slice("/send/".length));
      return new Response("sent");
    }
    if (url.pathname === "/seen") {
      return new Response((await env.INBOX.getByName("i").seen()).join(","));
    }
    return new Response("not found", { status: 404 });
  },
  async queue(batch, env) {
    await env.INBOX.getByName("i").add(batch.messages.map((message) => message.body));
  },
};
