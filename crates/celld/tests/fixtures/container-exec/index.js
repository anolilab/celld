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
      // A monitor() waiting while destroy() ends the start it follows.
      case "/monitor-destroyed-start":
        return run(async () => {
          container.start();
          const monitored = attempt(() => container.monitor());
          await container.destroy();
          const hung = new Promise((resolve) => setTimeout(() => resolve("hung"), 2500));
          return await Promise.race([monitored, hung]);
        });
      // A destroy() the engine refuses.
      case "/destroy-refused":
        return run(async () => {
          container.start();
          await (await container.exec(["hello"])).output();
          return await attempt(() => container.destroy());
        });
      // A start that fails, then a second start, in one object.
      case "/restart-after-failed-start":
        return run(async () => {
          container.start();
          for (let waited = 0; container.running && waited < 2000; waited += 20) {
            await new Promise((resolve) => setTimeout(resolve, 20));
          }
          const failed = !container.running;
          container.start();
          // Long enough for the first start's cleanup, held by the engine, to
          // reach it.
          await new Promise((resolve) => setTimeout(resolve, 1500));
          return { failed, running: container.running };
        });
      // The container of this run is up.
      case "/up":
        return run(async () => {
          container.start();
          await (await container.exec(["hello"])).output();
          return { running: container.running };
        });
      // A destroy() of the first run that reaches the engine after the object
      // has started a second one.
      case "/late-destroy":
        return run(async () => {
          container.start();
          await (await container.exec(["hello"])).output();
          const destroying = attempt(() => container.destroy());
          // The kill ends the first container, and the removal is held.
          for (let waited = 0; container.running && waited < 2000; waited += 20) {
            await new Promise((resolve) => setTimeout(resolve, 20));
          }
          container.start();
          await (await container.exec(["hello"])).output();
          const destroyed = await destroying;
          return { destroyed, running: container.running };
        });
      // An exec issued for the first run, held by the engine while the object
      // destroys that run and starts a second one.
      case "/exec-through-restart":
        return run(async () => {
          const stale = attempt(() => container.exec(["hello"]));
          await container.destroy();
          container.start();
          const fresh = await attempt(async () => (await (await container.exec(["hello"])).output()).exitCode);
          const outcome = await stale;
          return { stale: outcome.error ?? "ran", fresh };
        });
      // An exec issued while the first start is in flight, which the object
      // destroys and replaces with a second start before the exec is answered.
      case "/start-exec-restart":
        return run(async () => {
          container.start();
          const pending = attempt(() => container.exec(["hello"]));
          await container.destroy();
          container.start();
          const fresh = await attempt(async () => (await (await container.exec(["hello"])).output()).exitCode);
          const outcome = await pending;
          return { pending: outcome.error ?? "ran", fresh };
        });
      // A destroy() the engine answers with a removal already in progress.
      case "/destroy-plain":
        return run(async () => {
          const destroyed = await attempt(() => container.destroy());
          return { destroyed, running: container.running };
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
      // An exec whose signal is aborted before it spawns.
      case "/abort-before": {
        const controller = new AbortController();
        controller.abort(new Error("stop now"));
        return run(async () => {
          container.start();
          await container.exec(["hold"], { signal: controller.signal });
        });
      }
      // An exec aborted after it spawned: SIGKILL, in the same event.
      case "/abort-after":
        return run(async () => {
          container.start();
          const controller = new AbortController();
          const process = await container.exec(["hold"], { signal: controller.signal });
          controller.abort();
          return { exitCode: await process.exitCode };
        });
      // The abort listener is gone once the process has exited.
      case "/abort-listener":
        return run(async () => {
          container.start();
          const controller = new AbortController();
          const signal = controller.signal;
          const counts = { added: 0, removed: 0 };
          const add = signal.addEventListener.bind(signal);
          const remove = signal.removeEventListener.bind(signal);
          signal.addEventListener = (type, ...rest) => {
            if (type === "abort") counts.added++;
            return add(type, ...rest);
          };
          signal.removeEventListener = (type, ...rest) => {
            if (type === "abort") counts.removed++;
            return remove(type, ...rest);
          };
          const process = await container.exec(["hello"], { signal });
          const output = await process.output();
          return { exitCode: output.exitCode, ...counts };
        });
      // A process that exits after the event that spawned it has ended, and
      // that nothing awaits: the listener it put on the signal must go anyway.
      case "/abort-leak-spawn":
        return run(async () => {
          container.start();
          const controller = new AbortController();
          const signal = controller.signal;
          const counts = { added: 0, removed: 0 };
          const add = signal.addEventListener.bind(signal);
          const remove = signal.removeEventListener.bind(signal);
          signal.addEventListener = (type, ...rest) => {
            if (type === "abort") counts.added++;
            return add(type, ...rest);
          };
          signal.removeEventListener = (type, ...rest) => {
            if (type === "abort") counts.removed++;
            return remove(type, ...rest);
          };
          this.leak = { signal, counts };
          await container.exec(["hello"], { signal });
          return "spawned";
        });
      // The same signal again, from a later event, after the first process
      // has exited: its listener is gone before the new one is added.
      case "/abort-leak-sweep":
        return run(async () => {
          const { signal, counts } = this.leak;
          const before = { ...counts };
          const process = await container.exec(["hello"], { signal });
          // Read before the new process can have exited and taken its own
          // listener off, so only the first process's is counted.
          const after = { ...counts };
          await process.output();
          return { before, after };
        });
      // An exec kept across events: this event starts it, others consume it.
      case "/spawn-late":
        return run(async () => {
          container.start();
          this.process = await container.exec(["late"]);
          // The promise is taken here, in this event, and awaited in another.
          this.exit = this.process.exitCode;
          return "spawned";
        });
      case "/exit-stored":
        return run(() => this.exit);
      case "/exit-getter":
        return run(() => this.process.exitCode);
      // A read issued here and left pending when this event ends.
      case "/read-start":
        return run(async () => {
          container.start();
          this.process = await container.exec(["late"]);
          this.reader = this.process.stdout.getReader();
          this.pending = this.reader.read();
          return "reading";
        });
      case "/read-rest":
        return run(async () => {
          const decode = (result) =>
            result.done ? null : new TextDecoder().decode(result.value);
          const first = decode(await this.pending);
          const second = decode(await this.reader.read());
          const third = decode(await this.reader.read());
          return { first, second, third };
        });
      // The reader's next read comes first; the read this object left pending
      // in the other event is consumed after it.
      case "/read-fresh":
        return run(async () => {
          const decode = (result) =>
            result.done ? null : new TextDecoder().decode(result.value);
          const fresh = this.reader.read();
          const first = decode(await this.pending);
          const second = decode(await fresh);
          return { first, second };
        });
      // Many reads queued on a process that is silent: each gets its chunk,
      // in order, and the host is not asked once per read.
      case "/quiet-reads":
        return run(async () => {
          container.start();
          const process = await container.exec(["late"]);
          const reader = process.stdout.getReader();
          // A poller that races each read against a timer leaves one more
          // read pending per round, each from its own turn.
          const reads = [];
          for (let round = 0; round < 30; round++) {
            reads.push(reader.read());
            await new Promise((resolve) => setTimeout(resolve, 5));
          }
          const results = await Promise.all(reads);
          return results.map((result) =>
            result.done ? null : new TextDecoder().decode(result.value));
        });
      // Signals, kills and stdin closes that the event does not wait for.
      case "/signal-return":
        return run(async () => {
          container.start();
          const process = await container.exec(["hello"]);
          await process.exitCode;
          container.signal(15);
          return "signaled";
        });
      case "/kill-return":
        return run(async () => {
          container.start();
          const process = await container.exec(["hold"]);
          process.kill(9);
          return "killed";
        });
      // `eof` exits only when its stdin ends, and this exec has no stdin.
      case "/stdin-none":
        return run(async () => {
          container.start();
          await container.exec(["eof"]);
          return "spawned";
        });
      case "/kill-and-wait":
        return run(async () => {
          container.start();
          const process = await container.exec(["hold"]);
          process.kill(15);
          return { exitCode: await process.exitCode };
        });
      case "/exec-reserved":
        return run(async () => {
          container.start();
          await container.exec(["hello"], { env: { CELLD_EXEC: "spoofed" } });
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
