import { DurableObject, WorkflowEntrypoint } from "cloudflare:workers";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Counts what the workflows do outside their own ledger, so a test can see a
// callback that ran twice. `max` keeps the largest value it was given.
export class Counter extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    const storage = this.ctx.storage;
    const value = Number(url.searchParams.get("value") ?? 0);
    let n = (await storage.get("n")) ?? 0;
    if (url.pathname === "/inc") n += 1;
    if (url.pathname === "/max") n = Math.max(n, value);
    await storage.put("n", n);
    return new Response(String(n));
  }
}

const count = async (env, key, path = "/read") =>
  Number(await (await env.COUNTER.getByName(key).fetch("http://c" + path)).text());

// Many short steps: no step comes near the handler budget, but together they
// run past it.
export class Slow extends WorkflowEntrypoint {
  async run(event, step) {
    let done = 0;
    for (let i = 1; i <= event.payload.steps; i++) {
      done = await step.do(`step ${i}`, async () => {
        await sleep(event.payload.stepMs);
        return i;
      });
    }
    return { done };
  }
}

// One step that alone outlives the handler budget.
export class Stuck extends WorkflowEntrypoint {
  async run(event, step) {
    await step.do("stuck", { retries: { limit: 0, delay: 1 } }, async () => {
      await sleep(event.payload.stepMs);
      return "late";
    });
  }
}

// A step with a rollback handler, which celld does not implement.
export class Undo extends WorkflowEntrypoint {
  async run(event, step) {
    await step.do(
      "paid",
      { rollbackConfig: { retries: { limit: 0, delay: 1 } } },
      async () => "ok",
      async () => {},
    );
  }
}

// A step that needs its full timeout, started after another step used part of
// the turn. Its attempt must not start in a turn that cannot give it that
// timeout, or the handler ends it and the callback runs a second time.
export class Late extends WorkflowEntrypoint {
  async run(event, step) {
    await step.do("first", async () => {
      await sleep(500);
    });
    return await step.do(
      "second",
      { timeout: 1700, retries: { limit: 0, delay: 1 } },
      async () => {
        await count(this.env, "late", "/inc");
        await sleep(1650);
        return "ok";
      },
    );
  }
}

// Sleeps that complete at once. The time between them is spent in run(), and
// the longest turn is recorded: a turn must hand over before it uses the budget.
export class Naps extends WorkflowEntrypoint {
  async run(event, step) {
    const started = Date.now();
    const seen = (globalThis.__napsSeen ??= new Set());
    for (let i = 1; i <= event.payload.steps; i++) {
      if (!seen.has(i)) {
        seen.add(i);
        await sleep(event.payload.stepMs);
      }
      await step.sleep(`nap ${i}`, 0);
      await count(this.env, "naps", `/max?value=${Date.now() - started}`);
    }
    return { done: event.payload.steps };
  }
}

// Held by the first drive's retry delay callback until the replacement drive
// lets it go, in the same isolate.
let releaseDelay;
const delayGate = new Promise((resolve) => {
  releaseDelay = resolve;
});

// A step whose retry delay callback outlives the drive that started it. The
// drive is fenced while that callback is pending; when it resolves it must not
// write its retry record over the record of the drive that took over.
export class Fence extends WorkflowEntrypoint {
  async run(event, step) {
    const value = await step.do(
      "work",
      {
        timeout: 1000,
        retries: {
          limit: 3,
          delay: async () => {
            await delayGate;
            return 1;
          },
        },
      },
      async () => {
        if ((await count(this.env, "fence", "/inc")) === 1) {
          await new Promise(() => {});
        }
        return "ok";
      },
    );
    // The work record is complete by now. Letting the fenced drive go writes
    // its retry record after it.
    await step.do("release", async () => {
      releaseDelay();
    });
    await step.sleep("after", "1 second");
    return { value };
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") return new Response("ready");
    const id = url.searchParams.get("id") ?? "a";
    const flow = {
      stuck: env.STUCK,
      undo: env.UNDO,
      late: env.LATE,
      naps: env.NAPS,
      fence: env.FENCE,
    }[url.searchParams.get("flow")] ?? env.SLOW;
    if (url.pathname === "/count") {
      return Response.json({ n: await count(env, url.searchParams.get("key")) });
    }
    if (url.pathname === "/poke") {
      await (await flow.get(id)).sendEvent({ type: "poke", payload: {} });
      return new Response("poked");
    }
    if (url.pathname === "/start") {
      await flow.create({
        id,
        params: {
          steps: Number(url.searchParams.get("steps") ?? 1),
          stepMs: Number(url.searchParams.get("stepMs") ?? 100),
        },
      });
      return new Response("started");
    }
    return Response.json(await (await flow.get(id)).status());
  },
};
