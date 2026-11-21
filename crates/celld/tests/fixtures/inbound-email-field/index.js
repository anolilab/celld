// A class entrypoint that gives each instance its email() handler in the
// constructor, not on the prototype.
import { WorkerEntrypoint } from "cloudflare:workers";

export default class extends WorkerEntrypoint {
  constructor(ctx, env) {
    super(ctx, env);
    this.email = async (message) => {
      message.setReject("from an instance");
    };
  }

  async fetch(request) {
    if (new URL(request.url).pathname === "/__ready") return new Response("ready");
    return new Response("fetch");
  }
}
