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
      // A pty exec of `size`: it prints the terminal size after one line of stdin.
      case "/pty": {
        const pty = JSON.parse(url.searchParams.get("pty") ?? "true");
        const resize = url.searchParams.get("resize");
        return run(async () => {
          container.start();
          const process = await container.exec(["size"], { pty, stdin: "pipe" });
          if (resize !== null) {
            const [cols, rows] = resize.split("x").map(Number);
            await process.resize(cols, rows);
          }
          const writer = process.stdin.getWriter();
          await writer.write(new TextEncoder().encode("\n"));
          const out = await new Response(process.stdout).text();
          return { isPty: process.isPty, out, exitCode: await process.exitCode };
        });
      }
      // A pty exec that ends before it can be resized.
      case "/pty-short":
        return run(async () => {
          container.start();
          const process = await container.exec(["hello"], { pty: { cols: 100, rows: 30 } });
          const out = await new Response(process.stdout).text();
          return { out, exitCode: await process.exitCode };
        });
      case "/pty-state":
        return run(async () => {
          container.start();
          const plain = await container.exec(["hello"]);
          const piped = await container.exec(["hello"], { pty: true });
          let resizeError = null;
          try {
            await plain.resize(80, 24);
          } catch (error) {
            resizeError = error.message;
          }
          const bad = [];
          for (const pty of ["yes", { cols: 0 }, { cols: 1.5, rows: 24 }, { cols: 80, rows: 70000 }]) {
            bad.push(await attempt(() => container.exec(["hello"], { pty })));
          }
          let rangeError = null;
          try {
            await piped.resize(0, 24);
          } catch (error) {
            rangeError = error.name;
          }
          return { plain: plain.isPty, piped: piped.isPty, resizeError, rangeError, bad };
        });
      // A pty exec that never exits, for the case where sizing it fails.
      case "/pty-hold":
        return run(async () => {
          container.start();
          return await container.exec(["hold"], { pty: true }).then(() => "started");
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
