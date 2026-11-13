import { DurableObject } from "cloudflare:workers";

const text = (bytes) => new TextDecoder().decode(bytes);

// Run `fn` and report what it returned or how it failed, so a test can assert
// on an error message as it asserts on a value.
const attempt = async (fn) => {
  try {
    return { ok: await fn() };
  } catch (error) {
    return { error: error?.message ?? String(error) };
  }
};

export class Runner extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    const container = this.ctx.container;
    const run = async (fn) => Response.json(await attempt(fn));
    switch (url.pathname) {
      // exec() right after start(), in one event, with nothing awaited between.
      case "/start-exec":
        return run(async () => {
          container.start();
          const process = await container.exec(["hello"]);
          const output = await process.output();
          return { stdout: text(output.stdout), exitCode: output.exitCode };
        });
      // exec() pending while destroy() ends the start.
      case "/start-destroy-exec":
        return run(async () => {
          container.start();
          const pending = attempt(() => container.exec(["hello"]));
          await container.destroy();
          return await pending;
        });
      // destroy() ends a start whose container the engine is still creating.
      case "/destroy-during-start":
        return run(async () => {
          container.start();
          await container.destroy();
          // Long enough for the create that was in flight to finish.
          await new Promise((resolve) => setTimeout(resolve, 1500));
          return { running: container.running };
        });
      default:
        return new Response("not found", { status: 404 });
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    const name = url.searchParams.get("name") ?? "one";
    return env.RUNNER.get(env.RUNNER.idFromName(name)).fetch(request);
  },
};
