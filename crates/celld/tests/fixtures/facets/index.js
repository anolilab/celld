import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";

// The code of the facet. A real application would load it from a bundle,
// a database, or a request; here it is a string so the example is one file.
const CODE = `
  import { DurableObject } from "cloudflare:workers";
  export class App extends DurableObject {
    async fetch(request) {
      const path = new URL(request.url).pathname;
      const sql = this.ctx.storage.sql;
      // /fill?mb=N grows the database by N MiB, one row per MiB.
      if (path.endsWith("/fill")) {
        const mb = Number(new URL(request.url).searchParams.get("mb"));
        sql.exec("CREATE TABLE IF NOT EXISTS blobs (b BLOB)");
        for (let i = 0; i < mb; i++)
          sql.exec("INSERT INTO blobs VALUES (zeroblob(1048576))");
        return new Response("filled");
      }
      if (path.endsWith("/rows"))
        return new Response(String(
          sql.exec("SELECT count(*) AS c FROM blobs").one().c));
      const n = this.ctx.storage.kv.get("n") ?? 0;
      this.ctx.storage.kv.put("n", n + 1);
      const greeting = await this.env.GREETER.greet("facet");
      return new Response(String(n) + ":" + greeting);
    }
  }
`;

export class Greeter extends WorkerEntrypoint {
  greet(name) {
    return `${this.ctx.props.greeting}, ${name}`;
  }
}

export class Supervisor extends DurableObject {
  facet(name) {
    const worker = this.env.LOADER.get("app-v1", () => ({
      compatibilityDate: "2025-01-01",
      mainModule: "app.js",
      modules: { "app.js": CODE },
      env: {
        GREETER: this.ctx.exports.Greeter({
          props: { greeting: "hello" },
        }),
      },
    }));
    return this.ctx.facets.get(name, () => ({
      class: worker.getDurableObjectClass("App"),
    }));
  }

  async fetch(request) {
    // `/clone` copies the facet's storage into a second facet, `/copy...` is
    // that second facet, and any other path is the original.
    const url = new URL(request.url);
    const path = url.pathname;
    if (path === "/clone") {
      await this.ctx.facets.clone("app", "copy");
      return new Response("cloned");
    }
    // A delete and a clone of one target, and two clones of one target, in
    // one turn: each must run after the one before it.
    if (path === "/delete-clone") {
      this.ctx.facets.delete("copy");
      await this.ctx.facets.clone("app", "copy");
      return new Response("cloned");
    }
    if (path === "/clone-twice") {
      await Promise.all([
        this.ctx.facets.clone("app", "copy"),
        this.ctx.facets.clone("app", "copy"),
      ]);
      return new Response("cloned");
    }
    // A delete of the source that follows a clone without waiting for it
    // runs after the copy, so the copy holds the source's data.
    if (path === "/clone-then-delete-source") {
      const cloned = this.ctx.facets.clone("app", "copy");
      this.ctx.facets.delete("app");
      await cloned;
      return new Response("cloned");
    }
    if (path === "/clone-self") {
      try {
        await this.ctx.facets.clone("app", "app");
      } catch (error) {
        return new Response(error.message);
      }
      return new Response("cloned");
    }
    // A stub taken before a clone is the target's old one: it throws after.
    if (path === "/old-stub") {
      const old = this.facet("copy");
      await old.fetch(request);
      await this.ctx.facets.clone("app", "copy");
      try {
        await old.fetch(request);
      } catch (error) {
        return new Response("threw: " + error.message);
      }
      return new Response("still answers");
    }
    // A clone refused for size says so, and its target answers afterwards.
    if (path === "/clone-refused") {
      let message;
      try {
        await this.ctx.facets.clone("app", "copy");
      } catch (error) {
        message = error.message;
      }
      const after = await this.facet("copy").fetch(request);
      return new Response(message + " | " + (await after.text()));
    }
    if (path === "/clone-timed") {
      const started = Date.now();
      await this.ctx.facets.clone("app", "copy");
      return new Response(String(Date.now() - started));
    }
    // A clone while the source takes writes: how long it took, and how many
    // writes ran during it and failed.
    if (path === "/clone-busy") {
      const started = Date.now();
      let cloning = true;
      const clone = this.ctx.facets.clone("app", "copy")
        .finally(() => { cloning = false; });
      let writes = 0;
      let failed = 0;
      while (cloning) {
        try {
          await this.facet("app").fetch(new Request("http://facet/"));
          writes++;
        } catch (error) {
          failed++;
        }
      }
      await clone;
      return new Response(JSON.stringify({
        ms: Date.now() - started, writes, failed,
      }));
    }
    return this.facet(path.startsWith("/copy") ? "copy" : "app").fetch(request);
  }
}

export default {
  fetch(request, env) {
    // The harness probes this path for readiness; it must not bump the count.
    if (new URL(request.url).pathname === "/__ready") {
      return new Response("ready");
    }
    return env.SUPERVISOR.getByName("only").fetch(request);
  },
};
