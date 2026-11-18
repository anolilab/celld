import { DurableObject } from "cloudflare:workers";

const HOUR = 60 * 60 * 1000;

// `pin` answers at once and leaves a referenced interval behind it, the way an
// event that never awaits its own timer does. Each tick is stored, so a
// count that moves later shows JavaScript still running.
export class Pinned extends DurableObject {
  signal(name) {
    return this.env.PIN.get(this.env.PIN.idFromName(`signal-${name}`)).fetch(
      "http://do/mark",
    );
  }
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
            await this.signal("late");
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
    // A client that hangs up ends this event while it waits. The handlers its
    // rejected wait runs must not write or start anything.
    if (url.pathname === "/do-guarded") {
      this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS guarded (k TEXT)");
      this.outcome = {};
      await this.signal("guarded");
      try {
        await this.env.PIN.get(this.env.PIN.idFromName("stay")).fetch(
          "http://do/stay",
        );
      } catch (error) {
        this.outcome.ended = error.message;
        try {
          this.ctx.storage.sql.exec("INSERT INTO guarded VALUES ('caught')");
          this.outcome.write = "written";
        } catch (refused) {
          this.outcome.write = refused.message;
        }
        try {
          await fetch("http://127.0.0.1:1/");
          this.outcome.fetch = "answered";
        } catch (refused) {
          this.outcome.fetch = refused.message;
        }
      }
      return new Response("guarded");
    }
    if (url.pathname === "/guarded-outcome") {
      const rows = this.ctx.storage.sql
        .exec("SELECT count(*) AS n FROM guarded")
        .one().n;
      return new Response(JSON.stringify({ ...this.outcome, rows }));
    }
    // Holds a transaction open, waiting for ever. The object's move to a new
    // deployment ends the event inside it, and the rejection that unwinds it
    // must be able to roll the transaction back.
    if (url.pathname === "/tx-hold") {
      await this.ctx.storage.transaction(async (txn) => {
        await txn.put("tx", 1);
        await this.signal("tx");
        await new Promise((resolve) => setTimeout(resolve, HOUR));
      });
      return new Response("committed");
    }
    if (url.pathname === "/tx-value") {
      return new Response(String((await this.ctx.storage.get("tx")) ?? "none"));
    }
    // A transaction shuts the object's input gate, so an event that entered
    // one is reported through an object of its own.
    if (url.pathname === "/mark") {
      this.marked = true;
      return new Response("marked");
    }
    if (url.pathname === "/marked") {
      return new Response(this.marked ? "entered" : "waiting");
    }
    if (url.pathname === "/tx-value") {
      return new Response(String((await this.ctx.storage.get("tx")) ?? "none"));
    }
    // Waits for `/late-release`, which runs in another event. When the client
    // has hung up by then, this handler's event is over and its context gone.
    if (url.pathname === "/do-late-write") {
      this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS late (k TEXT)");
      this.lateWrite = {};
      request.signal.addEventListener("abort", () => {
        this.lateAborted = true;
      });
      await this.signal("lw");
      await new Promise((resolve) => {
        this.release = resolve;
      });
      try {
        this.ctx.storage.sql.exec("INSERT INTO late VALUES ('after')");
        this.lateWrite.write = "written";
      } catch (refused) {
        this.lateWrite.write = refused.message;
      }
      return new Response("late");
    }
    if (url.pathname === "/late-aborted") {
      return new Response(this.lateAborted ? "aborted" : "running");
    }
    if (url.pathname === "/late-release") {
      this.release?.();
      await new Promise((resolve) => setTimeout(resolve, 100));
      const rows = this.ctx.storage.sql
        .exec("SELECT count(*) AS n FROM late")
        .one().n;
      return new Response(JSON.stringify({ ...this.lateWrite, rows }));
    }
    // Answers at once and leaves a continuation behind that writes once
    // `/orphan-release` runs in another event. The event that started it has
    // completed normally, and its context is gone by then.
    if (url.pathname === "/orphan") {
      this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS orphan (k TEXT)");
      this.orphan = {};
      (async () => {
        await new Promise((resolve) => {
          this.orphanRelease = resolve;
        });
        try {
          this.ctx.storage.sql.exec("INSERT INTO orphan VALUES ('after')");
          this.orphan.write = "written";
        } catch (refused) {
          this.orphan.write = refused.message;
        }
      })();
      return new Response("started");
    }
    if (url.pathname === "/orphan-release") {
      this.orphanRelease?.();
      await new Promise((resolve) => setTimeout(resolve, 100));
      const rows = this.ctx.storage.sql
        .exec("SELECT count(*) AS n FROM orphan")
        .one().n;
      return new Response(JSON.stringify({ ...this.orphan, rows }));
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
        request.signal.addEventListener("abort", () => {
          globalThis.stashAborted = true;
        });
        globalThis.stashed = env.PIN.get(env.PIN.idFromName("stay")).fetch(
          new Request("http://do/stay"),
        );
        return await globalThis.stashed;
      case "/stash-state":
        return new Response(
          globalThis.stashAborted ? "aborted" : globalThis.stashed ? "waiting" : "none",
        );
      case "/await-stash":
        if (!globalThis.stashed) return new Response("nothing stashed");
        try {
          return new Response(await (await globalThis.stashed).text());
        } catch (error) {
          return new Response(`rejected: ${error.message}`);
        }
      // The object's event ends when the client of this request hangs up.
      case "/do-guarded":
        return env.PIN.get(env.PIN.idFromName("guarded")).fetch(request);
      case "/do-guarded-outcome":
        return env.PIN.get(env.PIN.idFromName("guarded")).fetch(
          new Request("http://do/guarded-outcome"),
        );
      // A stateless Worker, with no object behind it, ended the same way. Its
      // catch block writes through D1.
      case "/d1-guarded": {
        await env.DB.exec("CREATE TABLE IF NOT EXISTS guarded (k TEXT)");
        globalThis.d1Outcome = {};
        request.signal.addEventListener("abort", () => {
          globalThis.d1Aborted = true;
        });
        globalThis.d1Started = true;
        try {
          await env.PIN.get(env.PIN.idFromName("stay")).fetch("http://do/stay");
        } catch (error) {
          globalThis.d1Outcome.ended = error.message;
          try {
            await env.DB.prepare("INSERT INTO guarded VALUES ('caught')").run();
            globalThis.d1Outcome.write = "written";
          } catch (refused) {
            globalThis.d1Outcome.write = refused.message;
          }
        }
        return new Response("guarded");
      }
      case "/d1-state":
        return new Response(
          globalThis.d1Aborted ? "aborted" : globalThis.d1Started ? "waiting" : "none",
        );
      case "/d1-outcome": {
        const rows = await env.DB.prepare("SELECT count(*) AS n FROM guarded").first("n");
        return new Response(JSON.stringify({ ...globalThis.d1Outcome, rows }));
      }
      case "/do-lw-entered":
        return env.PIN.get(env.PIN.idFromName("signal-lw")).fetch(
          new Request("http://do/marked"),
        );
      case "/do-late-aborted":
        return env.PIN.get(env.PIN.idFromName("late-write")).fetch(
          new Request("http://do/late-aborted"),
        );
      case "/do-guarded-entered":
        return env.PIN.get(env.PIN.idFromName("signal-guarded")).fetch(
          new Request("http://do/marked"),
        );
      case "/do-late-write":
        return env.PIN.get(env.PIN.idFromName("late-write")).fetch(request);
      case "/do-orphan":
        return env.PIN.get(env.PIN.idFromName("orphan")).fetch(
          new Request("http://do/orphan"),
        );
      case "/do-orphan-release":
        return env.PIN.get(env.PIN.idFromName("orphan")).fetch(
          new Request("http://do/orphan-release"),
        );
      case "/do-late-release":
        return env.PIN.get(env.PIN.idFromName("late-write")).fetch(
          new Request("http://do/late-release"),
        );
      case "/do-tx-entered":
      case "/do-late-entered":
        return env.PIN.get(
          env.PIN.idFromName(`signal-${url.pathname.split("-")[1]}`),
        ).fetch(new Request("http://do/marked"));
      case "/do-tx-hold":
      case "/do-tx-value":
        return env.PIN.get(env.PIN.idFromName("tx")).fetch(
          new Request(`http://do/${url.pathname.slice("/do-".length)}`),
        );
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
