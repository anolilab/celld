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
    // Still running when the object's move to a new deployment ends it.
    if (url.pathname === "/stay") {
      await new Promise((resolve) => setTimeout(resolve, 5000));
      return new Response("stayed");
    }
    // Starts storage work after its handler has returned, from a leftover
    // timer, and keeps the outcome for `/late-outcome`.
    if (url.pathname === "/late") {
      this.late = "pending";
      setTimeout(async () => {
        try {
          await this.ctx.storage.transaction(async (txn) => {
            await txn.put("late", 1);
            await new Promise((resolve) => setTimeout(resolve, 300));
            await txn.put("late2", 1);
          });
          this.late = "committed";
        } catch (error) {
          this.late = `rejected: ${error.message}`;
        }
      }, 100);
      return new Response("started");
    }
    if (url.pathname === "/late-outcome") {
      return new Response(this.late ?? "none");
    }
    if (url.pathname === "/late-keys") {
      const keys = [...(await this.ctx.storage.list()).keys()];
      return new Response(keys.filter((key) => key.startsWith("late")).join(","));
    }
    if (url.pathname === "/nap") {
      await new Promise((resolve) => setTimeout(resolve, 200));
      return new Response("napped");
    }
    // Answers, then leaves work behind that is still running when its
    // deployment is retired.
    if (url.pathname === "/hang") {
      this.ctx.waitUntil(new Promise((resolve) => setTimeout(resolve, HOUR)));
      return new Response("hung");
    }
    let ticks = 0;
    setInterval(() => this.ctx.storage.put("ticks", ++ticks), 100);
    return new Response("pinned");
  }
}

// `/pin` answers at once and leaves a timer behind it in `waitUntil`. `/spin`
// does so after a long first turn. Either one keeps its isolate in use for an
// hour, longer than any test runs.
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
      // A promise for work another event started stays reachable after that
      // event ended. `/stash` leaves one behind and waits on it, so a client
      // that hangs up ends the event; `/await-stash` waits on it again.
      case "/stash":
        globalThis.stashed = env.PIN.get(env.PIN.idFromName("stay")).fetch(
          new Request("http://do/stay"),
        );
        return await globalThis.stashed;
      case "/await-stash":
        try {
          return new Response(await (await globalThis.stashed).text());
        } catch (error) {
          return new Response(`rejected: ${error.message}`);
        }
      case "/do-late":
      case "/do-late-outcome":
      case "/do-late-keys":
        return env.PIN.get(env.PIN.idFromName("late")).fetch(
          new Request(`http://do/${url.pathname.slice("/do-".length)}`),
        );
      case "/do-pin":
        return env.PIN.get(env.PIN.idFromName("a")).fetch(request);
      case "/do-hang":
        return env.PIN.get(
          env.PIN.idFromName(`hang-${url.searchParams.get("n")}`),
        ).fetch(new Request("http://do/hang"));
      case "/do-nap":
        return env.PIN.get(
          env.PIN.idFromName(`hang-${url.searchParams.get("n")}`),
        ).fetch(new Request("http://do/nap"));
      case "/do-stay":
        return env.PIN.get(env.PIN.idFromName("stay")).fetch(
          new Request("http://do/stay"),
        );
      case "/do-ticks":
        return env.PIN.get(env.PIN.idFromName("a")).fetch(
          new Request("http://do/ticks"),
        );
      // Still in its first turn when the retire deadline passes, then
      // leaves work behind it.
      case "/spin": {
        // The clock stands still inside a turn, so count instead: several
        // seconds of work.
        let n = 0;
        for (let i = 0; i < 6e9; i++) n += i;
        if (n < 0) return new Response("never");
        ctx.waitUntil(new Promise((resolve) => setTimeout(resolve, HOUR)));
        return new Response("spun");
      }
      default:
        return new Response("not found", { status: 404 });
    }
  },
};
