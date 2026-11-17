import { DurableObject } from "cloudflare:workers";

export class Cell extends DurableObject {
  fetch() {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS t (x INTEGER)");
    this.ctx.storage.sql.exec("INSERT INTO t (x) VALUES (1)");
    return new Response("ok");
  }
}

// `/minted` draws an id with `newUniqueId` and uses it; `/named/<name>` uses an
// id derived from a name. Both answer the id they used.
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    const id = url.pathname.startsWith("/named/")
      ? env.OBJ.idFromName(url.pathname.slice("/named/".length))
      : env.OBJ.newUniqueId();
    await env.OBJ.get(id).fetch(request);
    return new Response(id.toString());
  },
};
