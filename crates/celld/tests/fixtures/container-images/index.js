import { DurableObject } from "cloudflare:workers";

export class Box extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    const container = this.ctx.container;
    if (url.pathname === "/images") {
      return Response.json(container.images);
    }
    if (url.pathname === "/start") {
      const image = url.searchParams.get("image");
      try {
        container.start(image === null ? undefined : { image });
        return Response.json({ started: true, running: container.running });
      } catch (error) {
        return Response.json({ started: false, error: error.message });
      }
    }
    return new Response("not found", { status: 404 });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    const name = url.searchParams.get("name") ?? "one";
    const box = env.BOX.get(env.BOX.idFromName(name));
    return box.fetch(request);
  },
};
