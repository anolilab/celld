import { DurableObject } from "cloudflare:workers";
import { connect } from "cloudflare:sockets";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const opened = (ws) =>
  new Promise((resolve, reject) => {
    ws.addEventListener("open", resolve, { once: true });
    ws.addEventListener("error", reject, { once: true });
  });

const nextMessage = (ws) =>
  new Promise((resolve) => {
    ws.addEventListener("message", (event) => resolve(String(event.data)), {
      once: true,
    });
  });

const wsUrl = (url, path) => {
  const target = new URL(url);
  return `ws://${target.host}${path}`;
};

// A Durable Object that keeps its sockets hibernatable.
export class Chat extends DurableObject {
  async fetch() {
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(ws, message) {
    if (message === "id") {
      ws.send(`id:${ws._id}`);
      return;
    }
    ws.send(`chat:${message}:${this.ctx.getWebSockets().length}`);
  }

  // Tries to take the socket `id` for this object, as a script can by writing
  // the id on a socket of its own. Answers whether the host allowed it.
  claim(id) {
    const [, server] = Object.values(new WebSocketPair());
    server._id = id;
    try {
      this.ctx.acceptWebSocket(server);
      return "claimed";
    } catch {
      return "refused";
    }
  }

  // Tries every id after the one this object allocated. One socket is reused
  // so the object allocates nothing in between: each id it tries belongs to
  // no event yet.
  claimAhead(count) {
    const [, server] = Object.values(new WebSocketPair());
    const base = server._id;
    const answers = [];
    for (let id = base + 1; id <= base + count; id++) {
      server._id = id;
      try {
        this.ctx.acceptWebSocket(server);
        answers.push("claimed");
      } catch {
        answers.push("refused");
      }
    }
    return answers;
  }

  // Accepts a socket inside an RPC method, which has no request of its own.
  adopt() {
    const [, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    return this.ctx.getWebSockets().length;
  }
}

// A Durable Object that serves a regular, non-hibernatable socket.
export class Plain extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    // An object that dials out and reads a socket of its own.
    if (url.pathname === "/dial") {
      const ws = new WebSocket(`ws://${url.host}/echo`);
      await opened(ws);
      const reply = nextMessage(ws);
      ws.send("ping");
      const text = await reply;
      ws.close();
      return new Response(text);
    }
    if (url.pathname === "/tcp") {
      const socket = connect({
        hostname: "127.0.0.1",
        port: Number(url.searchParams.get("port")),
      });
      const writer = socket.writable.getWriter();
      const reader = socket.readable.getReader();
      const greeting = new TextDecoder().decode((await reader.read()).value);
      await writer.write(new TextEncoder().encode("pong\n"));
      const reply = new TextDecoder().decode((await reader.read()).value);
      await socket.close();
      return Response.json({ greeting, reply });
    }
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    server.addEventListener("message", (event) => {
      server.send(`plain:${event.data}`);
    });
    return new Response(null, { status: 101, webSocket: client });
  }
}

const FACET_CODE = `
  import { DurableObject } from "cloudflare:workers";
  export class App extends DurableObject {
    async fetch() {
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      server.addEventListener("message", (event) => {
        server.send("facet:" + event.data);
      });
      return new Response(null, { status: 101, webSocket: client });
    }
  }
`;

// An object whose socket lives in a facet it loaded.
export class Host extends DurableObject {
  fetch(request) {
    const worker = this.env.LOADER.get("app-v1", () => ({
      compatibilityDate: "2025-01-01",
      mainModule: "app.js",
      modules: { "app.js": FACET_CODE },
    }));
    const facet = this.ctx.facets.get("app", () => ({
      class: worker.getDurableObjectClass("App"),
    }));
    return facet.fetch(request);
  }
}

// What a request leaves for another one to find: the socket it holds open
// until `/release`, the frames that reached it, and the way to let it go.
const held = new Map();

const hold = async (name, make) => {
  const entry = { id: null, log: [], released: false, ready: false };
  held.set(name, entry);
  await make(entry);
  entry.ready = true;
  // Polled, because a promise settled by another request does not wake this one.
  while (!entry.released) await sleep(20);
  return Response.json({ id: entry.id, log: entry.log });
};

const settle = async (entry) => {
  for (let i = 0; i < 200 && entry.log.length === 0; i++) await sleep(10);
};

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const name = url.searchParams.get("name") ?? "a";
    const port = Number(url.searchParams.get("port"));

    if (url.pathname === "/__ready") return new Response("ready");

    // ---- a Worker's own server socket, answered after the 101 returned ----
    if (url.pathname === "/echo") {
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      server.addEventListener("message", (event) => {
        server.send(`echo:${event.data}`);
      });
      return new Response(null, { status: 101, webSocket: client });
    }

    // ---- ordinary use, which restricting ids must not break ----
    if (url.pathname === "/ws/selftest") {
      const ws = new WebSocket(wsUrl(request.url, "/echo"));
      await opened(ws);
      const reply = nextMessage(ws);
      ws.send("ping");
      const text = await reply;
      ws.close();
      return new Response(text);
    }
    if (url.pathname === "/ws/fetch-selftest") {
      const response = await fetch(wsUrl(request.url, "/echo").replace("ws:", "http:"), {
        headers: { Upgrade: "websocket" },
      });
      const ws = response.webSocket;
      ws.accept();
      const reply = nextMessage(ws);
      ws.send("ping");
      const text = await reply;
      ws.close();
      return new Response(text);
    }
    if (url.pathname === "/do/selftest") {
      const kind = url.searchParams.get("kind");
      const namespace = kind === "plain" ? env.PLAIN : kind === "facet" ? env.HOST : env.CHAT;
      const stub = namespace.getByName("room");
      const response = await stub.fetch(request.url, {
        headers: { Upgrade: "websocket" },
      });
      const ws = response.webSocket;
      ws.accept();
      const first = nextMessage(ws);
      ws.send("one");
      const a = await first;
      const second = nextMessage(ws);
      ws.send("two");
      const b = await second;
      ws.close();
      return Response.json([a, b]);
    }
    if (url.pathname === "/proxy/chat") {
      return env.CHAT.getByName("room").fetch(request);
    }
    if (url.pathname === "/ws/proxy-selftest") {
      const ws = new WebSocket(wsUrl(request.url, "/proxy/chat"));
      await opened(ws);
      const reply = nextMessage(ws);
      ws.send("one");
      const text = await reply;
      ws.close();
      return new Response(text);
    }
    if (url.pathname === "/do/rpc-accept") {
      const stub = env.CHAT.getByName("rpc");
      return Response.json([await stub.adopt(), await stub.adopt()]);
    }
    if (url.pathname === "/do/dial") {
      return env.PLAIN.getByName("dialer").fetch(new URL("/dial", request.url));
    }
    if (url.pathname === "/do/tcp") {
      return env.PLAIN.getByName("dialer").fetch(new URL(`/tcp?port=${port}`, request.url));
    }
    if (url.pathname === "/do/claim-ahead") {
      return Response.json(await env.CHAT.getByName("claimer").claimAhead(30));
    }
    if (url.pathname === "/ws/claim-while-connecting") {
      // A Durable Object tries to take the id of a socket still connecting.
      const ws = new WebSocket(wsUrl(request.url, "/echo"));
      const ready = opened(ws);
      const claim = await env.CHAT.getByName("claimer").claim(ws._id);
      await ready;
      const reply = nextMessage(ws);
      ws.send("ping");
      const text = await reply;
      ws.close();
      return Response.json({ claim, text });
    }
    if (url.pathname === "/ws/forge-scope") {
      // One real grant, with the scope swapped for another cell's.
      const stub = env.CHAT.getByName("room");
      const response = await stub.fetch(request.url, {
        headers: { Upgrade: "websocket" },
      });
      const target = response._wsTarget;
      let error = null;
      try {
        const forged = new WebSocket(undefined, [], {
          ...target,
          scope: `Plain:${env.PLAIN.idFromName("room")}`,
        });
        forged.accept();
      } catch (caught) {
        error = String(caught);
      }
      const ws = response.webSocket;
      ws.accept();
      const reply = nextMessage(ws);
      ws.send("one");
      const text = await reply;
      ws.close();
      return Response.json({ error, text });
    }
    if (url.pathname === "/tcp/selftest") {
      const socket = connect({ hostname: "127.0.0.1", port });
      const writer = socket.writable.getWriter();
      const reader = socket.readable.getReader();
      const greeting = new TextDecoder().decode((await reader.read()).value);
      await writer.write(new TextEncoder().encode("ping\n"));
      await sleep(20);
      await writer.write(new TextEncoder().encode("pong\n"));
      const reply = new TextDecoder().decode((await reader.read()).value);
      await socket.close();
      return Response.json({ greeting, reply });
    }

    // ---- another request's socket ----
    if (url.pathname === "/ws/hold") {
      return hold(name, async (entry) => {
        const ws = new WebSocket(wsUrl(request.url, "/echo"));
        entry.id = ws._id;
        ws.addEventListener("message", (event) => entry.log.push(String(event.data)));
        await opened(ws);
        ws.send("mine");
        await settle(entry);
      });
    }
    if (url.pathname === "/tcp/hold") {
      return hold(name, async (entry) => {
        const socket = connect({ hostname: "127.0.0.1", port });
        entry.id = socket._id;
        await socket.opened;
        entry.id = socket._id;
        const writer = socket.writable.getWriter();
        const reader = socket.readable.getReader();
        entry.log.push(new TextDecoder().decode((await reader.read()).value));
        await writer.write(new TextEncoder().encode("mine\n"));
      });
    }
    if (url.pathname === "/ws/hold-chat") {
      // A client of a Durable Object's socket, kept open by this request.
      const entry = { id: null, scope: null, log: [], released: false, ready: false, poke: false };
      held.set(name, entry);
      const ws = new WebSocket(wsUrl(request.url, "/proxy/chat"));
      ws.addEventListener("message", (event) => entry.log.push(String(event.data)));
      await opened(ws);
      ws.send("id");
      for (let i = 0; i < 200 && entry.log.length === 0; i++) await sleep(10);
      entry.id = Number(entry.log[0].split(":")[1]);
      entry.scope = `Chat:${env.CHAT.idFromName("room")}`;
      entry.ready = true;
      while (!entry.released) {
        if (entry.poke) {
          entry.poke = false;
          ws.send("again");
        }
        await sleep(20);
      }
      return Response.json({ log: entry.log });
    }
    if (url.pathname === "/poke") {
      const entry = held.get(name);
      if (entry) entry.poke = true;
      return new Response("poked");
    }
    if (url.pathname === "/info") {
      const entry = held.get(name);
      return Response.json({
        ready: !!entry?.ready,
        id: entry?.id ?? null,
        log: entry?.log ?? [],
      });
    }
    if (url.pathname === "/ws/forge-bind") {
      // Binds this request to a Durable Object's socket it was never handed.
      const victim = held.get(name);
      let error = null;
      const seen = [];
      try {
        const forged = new WebSocket(undefined, [], { id: victim.id, scope: victim.scope });
        forged.addEventListener("message", (event) => seen.push(String(event.data)));
        forged.accept();
        forged.send("forged");
        await sleep(300);
      } catch (caught) {
        error = String(caught);
      }
      return Response.json({ error, seen });
    }
    if (url.pathname === "/forge-return") {
      // Answers an upgrade with a Durable Object's socket it was never handed.
      const victim = held.get(name);
      const [client] = Object.values(new WebSocketPair());
      return new Response(null, {
        status: 101,
        webSocket: client,
        __wsTarget: { id: victim.id, scope: victim.scope },
      });
    }
    if (url.pathname === "/ws/forge-return-selftest") {
      let opened_ = false;
      try {
        const ws = new WebSocket(wsUrl(request.url, `/forge-return?name=${name}`));
        await opened(ws);
        opened_ = true;
        ws.close();
      } catch {
        opened_ = false;
      }
      return Response.json({ opened: opened_ });
    }
    if (url.pathname === "/release") {
      const entry = held.get(name);
      if (entry) entry.released = true;
      return new Response("released");
    }
    if (url.pathname === "/ws/steal") {
      const victim = held.get(name);
      const mine = new WebSocket(wsUrl(request.url, "/echo"));
      await opened(mine);
      const own = mine._id;
      mine._id = victim.id;
      let error = null;
      try {
        mine.send("stolen");
      } catch (caught) {
        error = String(caught);
      }
      await sleep(300);
      mine._id = own;
      mine.close();
      return Response.json({ error });
    }
    if (url.pathname === "/ws/send-on-cell") {
      // A Worker request sends on a Durable Object's socket.
      const victim = held.get(name);
      const mine = new WebSocket(wsUrl(request.url, "/echo"));
      await opened(mine);
      const own = mine._id;
      mine._id = victim.id;
      let error = null;
      try {
        mine.send("injected");
      } catch (caught) {
        error = String(caught);
      }
      await sleep(300);
      mine._id = own;
      mine.close();
      return Response.json({ error });
    }
    if (url.pathname === "/tcp/steal-write") {
      const victim = held.get(name);
      const mine = connect({ hostname: "127.0.0.1", port });
      await mine.opened;
      const own = mine._id;
      mine._id = victim.id;
      let error = null;
      try {
        const writer = mine.writable.getWriter();
        await writer.write(new TextEncoder().encode("stolen\n"));
      } catch (caught) {
        error = String(caught);
      }
      mine._id = own;
      await mine.close();
      return Response.json({ error });
    }
    if (url.pathname === "/tcp/steal-read") {
      const victim = held.get(name);
      const mine = connect({ hostname: "127.0.0.1", port });
      await mine.opened;
      const own = mine._id;
      mine._id = victim.id;
      let data = null;
      let error = null;
      try {
        const reader = mine.readable.getReader();
        const { value } = await reader.read();
        data = value ? new TextDecoder().decode(value) : "";
      } catch (caught) {
        error = String(caught);
      }
      mine._id = own;
      await mine.close();
      return Response.json({ data, error });
    }
    return new Response("not found", { status: 404 });
  },
};
