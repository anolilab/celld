import { DurableObject } from "cloudflare:workers";

// One object per agent; each request appends one conversation turn.
export class Agent extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS turns (id INTEGER PRIMARY KEY, at INTEGER NOT NULL, body TEXT NOT NULL)",
    );
  }
  fetch() {
    this.ctx.storage.sql.exec(
      "INSERT INTO turns (at, body) VALUES (?, ?)",
      Date.now(),
      "x".repeat(512),
    );
    return new Response("ok");
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    const name = url.pathname.slice("/agent/".length);
    return env.AGENT.get(env.AGENT.idFromName(name)).fetch(request);
  },
};
