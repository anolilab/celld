import { WorkflowEntrypoint } from "cloudflare:workers";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") return new Response("ready");
    const id = url.searchParams.get("id") ?? "a";
    const flow = { stuck: env.STUCK, undo: env.UNDO }[url.searchParams.get("flow")] ??
      env.SLOW;
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
