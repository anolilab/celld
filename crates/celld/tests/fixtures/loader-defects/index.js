import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";

// What the outbound Fetcher saw of the requests the loaded Worker made.
const outbound = { started: 0, aborted: 0, socketCloses: [] };

// A Fetcher for the loaded Worker's global `fetch()`. It holds each request
// open until the request's own signal aborts it.
export class Outbound extends WorkerEntrypoint {
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (path === "/state") return Response.json(outbound);
    // Answers with a socket, which a plain `fetch()` cannot use.
    if (path === "/upgrade") {
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      server.addEventListener("close", (event) => outbound.socketCloses.push(event.code));
      return new Response(null, { status: 101, webSocket: client });
    }
    outbound.started++;
    await new Promise((resolve) => request.signal.addEventListener("abort", resolve));
    outbound.aborted++;
    return new Response("aborted");
  }
}

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

// Loaded code that upgrades to a WebSocket and echoes. It closes when told to
// and keeps what its own end observed, for `/socket-state` to read back.
const SOCKET_CODE = `
  const seen = { closes: [], errors: 0, received: 0 };
  export default {
    fetch(request) {
      if (new URL(request.url).pathname === "/state") return Response.json(seen);
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      server.addEventListener("message", (event) => {
        seen.received++;
        if (event.data === "close") server.close(3002, "from worker");
        else server.send("echo:" + event.data);
      });
      server.addEventListener("close", (event) =>
        seen.closes.push({ code: event.code, reason: event.reason, wasClean: event.wasClean }));
      server.addEventListener("error", () => seen.errors++);
      return new Response(null, { status: 101, webSocket: client });
    },
  };
`;

// Loaded code that sends without end to a socket nobody holds the other end of.
const FLOOD_CODE = `
  const seen = { sent: 0, closes: [] };
  export default {
    fetch(request) {
      if (new URL(request.url).pathname === "/state") return Response.json(seen);
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      server.addEventListener("close", (event) => seen.closes.push(event.code));
      const chunk = "x".repeat(64 * 1024);
      const flood = () => {
        if (server.readyState !== 1 || seen.sent >= 4096) return;
        server.send(chunk);
        seen.sent++;
        setTimeout(flood, 2);
      };
      setTimeout(flood, 0);
      return new Response(null, { status: 101, webSocket: client });
    },
  };
`;

// Loaded code that starts a `fetch()` with no signal and answers without it.
const DROP_CODE = `
  export default {
    async fetch() {
      fetch("http://outbound.test/").catch(() => {});
      await new Promise((resolve) => setTimeout(resolve, 200));
      return new Response("answered");
    },
  };
`;

// A Durable Object that answers with the socket a loaded Worker upgraded.
export class Relay extends DurableObject {
  fetch(request) {
    const worker = load(this.env, "socket", SOCKET_CODE);
    return worker.getEntrypoint().fetch(request);
  }
}

// Loaded code whose `fetch()` is answered with a 101 it has no use for.
const UPGRADED_CODE = `
  export default {
    async fetch() {
      const response = await fetch("http://outbound.test/upgrade");
      await new Promise((resolve) => setTimeout(resolve, 300));
      const state = await (await fetch("http://outbound.test/state")).json();
      return Response.json({ status: response.status, state });
    },
  };
`;

const load = (env, id, code, main = "main.js", extra = {}, options = {}) =>
  env.LOADER.get(id, () => ({
    compatibilityDate: "2025-01-01",
    mainModule: main,
    modules: { [main]: code, ...extra },
    ...options,
  }));

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    if (url.pathname === "/outbound-state") return Response.json(outbound);
    // A Fetcher answers the loaded Worker's `fetch()` with a socket.
    if (url.pathname === "/outbound-upgraded") {
      const worker = load(env, "upgraded", UPGRADED_CODE, "main.js", {}, {
        globalOutbound: ctx.exports.Outbound,
      });
      return worker.getEntrypoint().fetch("http://loaded/");
    }
    // The loaded Worker's request ends with a service call still pending that
    // had no signal to cancel it.
    if (url.pathname === "/outbound-drop") {
      const worker = load(env, "drop", DROP_CODE, "main.js", {}, {
        globalOutbound: ctx.exports.Outbound,
      });
      const answer = await (await worker.getEntrypoint().fetch("http://loaded/")).text();
      return new Response(answer);
    }
    // A Durable Object returns a loaded Worker's socket to its caller.
    if (url.pathname === "/relay") {
      const stub = env.RELAY.get(env.RELAY.idFromName("relay"));
      const upgrade = url.searchParams.get("upgrade") !== "no";
      let response;
      try {
        response = await stub.fetch("http://relay/", upgrade ? { headers: { Upgrade: "websocket" } } : {});
      } catch (error) {
        return Response.json({ threw: String(error?.message ?? error) });
      }
      const socket = response.webSocket;
      const out = { status: response.status, socket: !!socket };
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
    // The caller opens the loaded Worker's socket, then one side closes it
    // with a code of its own. Reports what each end saw.
    if (url.pathname === "/socket-close") {
      const worker = load(env, "socket", SOCKET_CODE);
      const response = await worker.getEntrypoint().fetch("http://loaded/", {
        headers: { Upgrade: "websocket" },
      });
      const socket = response.webSocket;
      socket.accept();
      const closed = new Promise((resolve) =>
        socket.addEventListener("close", (event) =>
          resolve({ code: event.code, reason: event.reason, wasClean: event.wasClean })),
      );
      const echoed = new Promise((resolve) =>
        socket.addEventListener("message", (event) => resolve(event.data)),
      );
      socket.send("hi");
      await echoed;
      if (url.searchParams.get("who") === "worker") socket.send("close");
      else socket.close(3001, "from caller");
      const caller = await Promise.race([
        closed,
        new Promise((resolve) => setTimeout(() => resolve("timeout"), 2000)),
      ]);
      await new Promise((resolve) => setTimeout(resolve, 300));
      const loaded = await (await worker.getEntrypoint().fetch("http://loaded/state")).json();
      return Response.json({ caller, loaded });
    }
    // The caller sends a message and closes in the same turn, so the loaded
    // Worker's isolate has not read either yet when the close is forwarded.
    if (url.pathname === "/socket-close-unread") {
      const worker = load(env, "socket", SOCKET_CODE);
      const response = await worker.getEntrypoint().fetch("http://loaded/", {
        headers: { Upgrade: "websocket" },
      });
      const socket = response.webSocket;
      socket.accept();
      socket.send("hi");
      socket.close(3001, "from caller");
      await new Promise((resolve) => setTimeout(resolve, 500));
      const loaded = await (await worker.getEntrypoint().fetch("http://loaded/state")).json();
      return Response.json({ loaded });
    }
    // The caller sends far more than the loaded Worker's queue holds, in one
    // turn, so the Worker reads none of it while it is sent.
    if (url.pathname === "/socket-caller-flood") {
      const worker = load(env, "socket", SOCKET_CODE);
      const response = await worker.getEntrypoint().fetch("http://loaded/", {
        headers: { Upgrade: "websocket" },
      });
      const socket = response.webSocket;
      socket.accept();
      const closed = new Promise((resolve) =>
        socket.addEventListener("close", (event) => resolve(event.code)),
      );
      const chunk = "x".repeat(64 * 1024);
      for (let sent = 0; sent < 200; sent++) socket.send(chunk);
      const caller = await Promise.race([
        closed,
        new Promise((resolve) => setTimeout(() => resolve("timeout"), 3000)),
      ]);
      await new Promise((resolve) => setTimeout(resolve, 300));
      const loaded = await (await worker.getEntrypoint().fetch("http://loaded/state")).json();
      return Response.json({ caller, loaded });
    }
    // The caller is handed a socket and never accepts it, while the loaded
    // Worker keeps sending.
    if (url.pathname === "/socket-flood") {
      const worker = load(env, "flood", FLOOD_CODE);
      await worker.getEntrypoint().fetch("http://loaded/", { headers: { Upgrade: "websocket" } });
      await new Promise((resolve) => setTimeout(resolve, 1500));
      const loaded = await (await worker.getEntrypoint().fetch("http://loaded/state")).json();
      return Response.json(loaded);
    }
    return new Response("not found", { status: 404 });
  },
};
