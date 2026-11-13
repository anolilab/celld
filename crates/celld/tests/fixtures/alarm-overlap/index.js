import { DurableObject } from "cloudflare:workers";

const log = [];
let running = 0;
let n = 0;

export class Cell extends DurableObject {
  async alarm() {
    const id = ++n;
    const ms = (await this.ctx.storage.get("ms")) ?? 0;
    running++;
    log.push({ id, event: "start", at: Date.now(), concurrent: running });
    try {
      await new Promise((resolve) => setTimeout(resolve, ms));
    } finally {
      running--;
      log.push({ id, event: "end", at: Date.now() });
    }
  }
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/arm") {
      await this.ctx.storage.put("ms", Number(url.searchParams.get("ms")));
      await this.ctx.storage.setAlarm(Date.now() + 10);
      return new Response("armed\n");
    }
    if (url.pathname === "/alarm") {
      return Response.json(await this.ctx.storage.getAlarm());
    }
    return Response.json(log);
  }
}

export default {
  fetch(request, env) {
    if (new URL(request.url).pathname === "/__ready") {
      return new Response("ready");
    }
    return env.CELL.get(env.CELL.idFromName("a")).fetch(request);
  },
};
