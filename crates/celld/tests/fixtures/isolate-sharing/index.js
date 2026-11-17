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

// A reaction a finished event left behind: the event that registers it has
// ended, a later event of another cell settles the promise, and V8 runs the
// reaction inside that later event's microtask checkpoint.
let fireStale;
const staleGate = new Promise((resolve) => {
  fireStale = resolve;
});
let staleOutcome = "did not run";

export class Cell extends DurableObject {
  // Leaves a reaction on the module-scope promise and returns at once.
  arm() {
    staleGate.then(() => {
      let write;
      try {
        fs.writeFileSync("/tmp/stale", "x");
        write = "written";
      } catch (error) {
        write = error.code;
      }
      staleOutcome = { write, sees: fs.existsSync("/tmp/planted") };
    });
    return { armed: true };
  }

  // Plants a file in this request's `/tmp`, then lets the stale reaction run
  // during this request.
  async fire() {
    fs.writeFileSync("/tmp/planted", "from the running event");
    fireStale();
    await new Promise((resolve) => setTimeout(resolve, 100));
    return staleOutcome;
  }

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
    switch (op) {
      case "park":
        return this.park().then(Response.json);
      case "release":
        return this.release().then(Response.json);
      case "arm":
        return Response.json(this.arm());
      case "fire":
        return this.fire().then(Response.json);
      default:
        return new Response(String(parked));
    }
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
