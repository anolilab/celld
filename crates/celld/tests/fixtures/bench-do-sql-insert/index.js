import { DurableObject } from "cloudflare:workers";

export class Cell extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS rows (id INTEGER PRIMARY KEY, at INTEGER NOT NULL)",
    );
  }
  fetch() {
    this.ctx.storage.sql.exec("INSERT INTO rows (at) VALUES (?)", Date.now());
    return new Response("ok");
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    return env.CELL.get(env.CELL.idFromName("sql")).fetch(request);
  },
};
