import { DurableObject } from "cloudflare:workers";
import fs from "node:fs";

// Module scope is shared by every cell in the isolate, so two cells can wait
// on each other and the continuation of one runs during the turn of the other.
const isolate = crypto.randomUUID().slice(0, 8);
let parked = false;
let release;
const gate = new Promise((resolve) => {
  release = resolve;
});

export class Cell extends DurableObject {
  // Parks until the other cell lets go, then writes to its own `/tmp`.
  async park() {
    parked = true;
    await gate;
    fs.mkdirSync("/tmp/from-parked");
    return { isolate, own: fs.existsSync("/tmp/from-parked") };
  }

  // Lets the parked cell continue while this request is the one running, then
  // looks for what the parked cell wrote.
  async release() {
    release();
    await new Promise((resolve) => setTimeout(resolve, 100));
    return { isolate, foreign: fs.existsSync("/tmp/from-parked") };
  }

  fetch(request) {
    const op = new URL(request.url).pathname.split("/")[2];
    return op === "park" ? this.park().then(Response.json) : op === "release"
      ? this.release().then(Response.json)
      : new Response(String(parked));
  }
}

export default {
  fetch(request, env) {
    const [, name] = new URL(request.url).pathname.split("/");
    if (name === "__ready") {
      return new Response("ready");
    }
    return env.CELL.get(env.CELL.idFromName(name)).fetch(request);
  },
};
