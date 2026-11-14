import { DurableObject } from "cloudflare:workers";

export class Cell extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS rows (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
    );
  }
  fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/add") {
      this.ctx.storage.sql.exec("INSERT INTO rows (label) VALUES (?)", url.searchParams.get("label"));
      return new Response("ok");
    }
    const labels = [...this.ctx.storage.sql.exec("SELECT label FROM rows ORDER BY id")].map(
      (row) => row.label,
    );
    return new Response(labels.join(","));
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    return env.CELL.get(env.CELL.idFromName("rows")).fetch(request);
  },
};
