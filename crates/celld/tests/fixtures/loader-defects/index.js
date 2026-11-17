// Loaded code whose modules each import a different name from the same
// builtin. The main module also imports from it.
const IMPORTS_MAIN = `
  import { WorkerEntrypoint } from "cloudflare:workers";
  import { siblingKind } from "./sibling.js";
  export default {
    fetch() {
      return Response.json({
        main: typeof WorkerEntrypoint,
        sibling: siblingKind,
      });
    },
  };
`;
const IMPORTS_SIBLING = `
  import { DurableObject } from "cloudflare:workers";
  export const siblingKind = typeof DurableObject;
`;

// Loaded code that holds one provider request open until the request's own
// signal aborts it.
const PROVIDER_CODE = `
  export default {
    async fetch(request) {
      const provider = new URL(request.url).searchParams.get("provider");
      try {
        await fetch(provider, { signal: request.signal });
        return new Response("provider answered");
      } catch (error) {
        return new Response("provider request failed: " + error.name);
      }
    },
  };
`;

// Loaded code that upgrades to a WebSocket and echoes.
const SOCKET_CODE = `
  export default {
    fetch() {
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      server.addEventListener("message", (event) => server.send("echo:" + event.data));
      return new Response(null, { status: 101, webSocket: client });
    },
  };
`;

const load = (env, id, code, main = "main.js", extra = {}) =>
  env.LOADER.get(id, () => ({
    compatibilityDate: "2025-01-01",
    mainModule: main,
    modules: { [main]: code, ...extra },
  }));

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    if (url.pathname === "/imports") {
      const worker = load(env, "imports", IMPORTS_MAIN, "main.js", {
        "sibling.js": IMPORTS_SIBLING,
      });
      return worker.getEntrypoint().fetch("http://loaded/");
    }
    if (url.pathname === "/abort") {
      const worker = load(env, "provider", PROVIDER_CODE);
      const controller = new AbortController();
      const provider = encodeURIComponent(url.searchParams.get("provider"));
      const pending = worker
        .getEntrypoint()
        .fetch(`http://loaded/?provider=${provider}`, { signal: controller.signal })
        .then(
          (response) => response.text(),
          (error) => "aborted: " + error.name,
        );
      await new Promise((resolve) => setTimeout(resolve, 300));
      controller.abort();
      return new Response(await pending);
    }
    if (url.pathname === "/direct") {
      const controller = new AbortController();
      setTimeout(() => controller.abort(), 300);
      try {
        await fetch(url.searchParams.get("provider"), { signal: controller.signal });
      } catch (error) {
        return new Response("aborted: " + error.name);
      }
      return new Response("provider answered");
    }
    if (url.pathname === "/socket") {
      const worker = load(env, "socket", SOCKET_CODE);
      const response = await worker.getEntrypoint().fetch("http://loaded/", {
        headers: { Upgrade: "websocket" },
      });
      const socket = response.webSocket;
      const out = { status: response.status, socket: socket !== null && socket !== undefined };
      if (socket) {
        socket.accept();
        const echoed = new Promise((resolve) =>
          socket.addEventListener("message", (event) => resolve(event.data)),
        );
        socket.send("hi");
        out.echo = await Promise.race([
          echoed,
          new Promise((resolve) => setTimeout(() => resolve("timeout"), 2000)),
        ]);
      }
      return Response.json(out);
    }
    return new Response("not found", { status: 404 });
  },
};
