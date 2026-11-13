import { DurableObject } from "cloudflare:workers";

// The object keeps no state, so the workload measures routing across many
// objects, not storage.
export class Cell extends DurableObject {
  fetch() {
    return new Response("ok");
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    return env.CELL.get(env.CELL.idFromName(url.searchParams.get("name") ?? "a")).fetch(request);
  },
};
