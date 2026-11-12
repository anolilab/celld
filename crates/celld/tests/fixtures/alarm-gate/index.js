import { DurableObject } from "cloudflare:workers";

const log = [];
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export class Cell extends DurableObject {
  async alarm() {
    log.push({ event: "start", at: Date.now() });
    await sleep(1000);
    log.push({ event: "end", at: Date.now() });
  }
  async fetch(request) {
    if (new URL(request.url).pathname === "/hold") {
      await this.ctx.storage.setAlarm(Date.now() + 100);
      this.ctx.blockConcurrencyWhile(() => sleep(2000));
      log.push({ event: "hold", at: Date.now() });
      return new Response("holding\n");
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
