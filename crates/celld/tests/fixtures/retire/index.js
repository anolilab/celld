import { DurableObject } from "cloudflare:workers";

const HOUR = 60 * 60 * 1000;

// `pin` answers at once and leaves a referenced interval behind it, the way an
// event that never awaits its own timer does. Each tick is stored, so a
// count that moves later shows JavaScript still running.
export class Pinned extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/ticks") {
      return new Response(String((await this.ctx.storage.get("ticks")) ?? 0));
    }
    let ticks = 0;
    setInterval(() => this.ctx.storage.put("ticks", ++ticks), 100);
    return new Response("pinned");
  }
}

// `/pin` answers at once and leaves a timer behind it in `waitUntil`. `/hang`
// never answers. Either one keeps its isolate in use for an hour, longer than
// any test runs.
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    switch (url.pathname) {
      case "/__ready":
        return new Response("ready");
      case "/version":
        return new Response("v1");
      case "/pin":
        ctx.waitUntil(new Promise((resolve) => setTimeout(resolve, HOUR)));
        return new Response("pinned");
      case "/do-pin":
        return env.PIN.get(env.PIN.idFromName("a")).fetch(request);
      case "/do-ticks":
        return env.PIN.get(env.PIN.idFromName("a")).fetch(
          new Request("http://do/ticks"),
        );
      case "/hang":
        await new Promise((resolve) => setTimeout(resolve, HOUR));
        return new Response("late");
      default:
        return new Response("not found", { status: 404 });
    }
  },
};
