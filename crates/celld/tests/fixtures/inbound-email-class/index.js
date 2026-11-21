// A class entrypoint whose email() method rejects every message.
import { WorkerEntrypoint } from "cloudflare:workers";

export default class extends WorkerEntrypoint {
  async fetch(request) {
    if (new URL(request.url).pathname === "/__ready") return new Response("ready");
    return new Response("fetch");
  }

  async email(message) {
    message.setReject("from a class");
  }
}
