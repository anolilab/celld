import { DurableObject } from "cloudflare:workers";

// Module scope runs once per isolate, so the id tells which isolate hosts a cell.
const isolate = crypto.randomUUID().slice(0, 8);

export class Cell extends DurableObject {
  fetch() {
    return new Response(isolate);
  }
}

export default {
  fetch(request, env) {
    const name = new URL(request.url).pathname.slice(1) || "a";
    if (name === "__ready") {
      return new Response("ready");
    }
    return env.CELL.get(env.CELL.idFromName(name)).fetch(request);
  },
};
