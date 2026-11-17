import { DurableObject } from "cloudflare:workers";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// A server-sent event stream that ends after `events` events.
const sse = (events) => {
  const encoder = new TextEncoder();
  return new Response(
    new ReadableStream({
      async start(controller) {
        for (let index = 0; index < events; index++) {
          controller.enqueue(encoder.encode(`data: event ${index}\n\n`));
          await sleep(20);
        }
        controller.close();
      },
    }),
    { headers: { "content-type": "text/event-stream" } },
  );
};

// A WebSocket the handler closes in the way `when` names.
const socket = (when) => {
  const [client, server] = Object.values(new WebSocketPair());
  server.accept();
  server.send("hello");
  if (when === "message") {
    server.addEventListener("message", () => server.close(4001, "done"));
  } else if (when === "timer") {
    setTimeout(() => server.close(4001, "done"), 100);
  } else if (when === "bare") {
    server.close();
  } else {
    server.close(4001, "done");
  }
  return new Response(null, { status: 101, webSocket: client });
};

// A WebSocket that refuses the first message it gets with `code`; with `feed`
// it also sends a tick every 100 ms until it closes.
const refuse = (feed, code) => {
  const [client, server] = Object.values(new WebSocketPair());
  server.accept();
  let ticker;
  if (feed) {
    ticker = setInterval(() => server.send("tick"), 100);
  }
  server.addEventListener("message", () => {
    clearInterval(ticker);
    server.close(code, "refused");
  });
  server.addEventListener("close", () => clearInterval(ticker));
  return new Response(null, { status: 101, webSocket: client });
};

// A WebSocket whose first message listener and close listener throw: the
// second message listener must still answer, and so must later messages.
const throwing = (tag) => {
  const [client, server] = Object.values(new WebSocketPair());
  server.accept();
  server.addEventListener("message", () => {
    throw new Error(`message boom ${tag}`);
  });
  server.addEventListener("message", (event) => server.send(`echo:${event.data}`));
  server.addEventListener("close", () => {
    throw new Error(`close boom ${tag}`);
  });
  return new Response(null, { status: 101, webSocket: client });
};

// Which close codes `close()` takes: the error name for each code, or "ok".
const closeCodes = () => {
  const outcome = {};
  for (const code of [
    999, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011,
    1012, 1013, 1014, 1015, 1016, 2999, 3000, 4999, 5000,
  ]) {
    const [, server] = Object.values(new WebSocketPair());
    server.accept();
    try {
      server.close(code, "r");
      outcome[code] = "ok";
    } catch (error) {
      outcome[code] = error.name;
    }
  }
  const [, server] = Object.values(new WebSocketPair());
  server.accept();
  try {
    server.close(1008, "x".repeat(124));
  } catch (error) {
    outcome.longReason = error.name;
  }
  return Response.json(outcome);
};

export class Streams extends DurableObject {
  member = null;
  pending = new Map();
  next = 0;

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/refuse") {
      return refuse(url.searchParams.has("feed"), Number(url.searchParams.get("code") ?? 1008));
    }
    if (url.pathname === "/close-codes") {
      return closeCodes();
    }
    if (url.pathname === "/throwing") {
      return throwing("object");
    }
    if (url.pathname === "/member") {
      // The member relays answers to the requests this object forwards to it.
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      this.member = server;
      server.addEventListener("message", (event) => {
        const { id, done, delta } = JSON.parse(event.data);
        const controller = this.pending.get(id);
        if (!controller) return;
        if (done) {
          this.pending.delete(id);
          controller.close();
        } else {
          controller.enqueue(new TextEncoder().encode(`data: ${delta}\n\n`));
        }
      });
      return new Response(null, { status: 101, webSocket: client });
    }
    if (url.pathname === "/stream") {
      const id = this.next++;
      const stream = new ReadableStream({
        start: (controller) => {
          this.pending.set(id, controller);
          this.member.send(JSON.stringify({ id }));
        },
      });
      return new Response(stream, { headers: { "content-type": "text/event-stream" } });
    }
    if (url.pathname === "/ws-close") {
      return socket(url.searchParams.get("when"));
    }
    if (url.pathname === "/written") {
      return new Response((await this.ctx.storage.get("written")) ?? "none");
    }
    if (url.pathname === "/close-all") {
      for (const ws of this.ctx.getWebSockets()) ws.close(4001, "done");
      return new Response("closed");
    }
    if (url.pathname === "/ws-hibernate") {
      const [client, server] = Object.values(new WebSocketPair());
      this.ctx.acceptWebSocket(server);
      return new Response(null, { status: 101, webSocket: client });
    }
    return sse(Number(url.searchParams.get("events") ?? "3"));
  }

  // The object closes the socket from its message handler, in the way the
  // message names.
  async webSocketMessage(ws, message) {
    if (message === "throw") {
      throw new Error("hibernation boom");
    }
    if (message === "throw-write") {
      await this.ctx.storage.put("written", "yes");
      throw new Error("hibernation write boom");
    }
    if (message === "echo") {
      ws.send("echoed");
      return;
    }
    if (message === "codes") {
      // The hibernation socket takes the same codes; only the refused ones are
      // tried here, because a taken one closes the socket.
      const outcome = {};
      for (const code of [999, 1004, 1005, 1006, 1015, 1016, 2999, 5000]) {
        try {
          ws.close(code, "r");
          outcome[code] = "ok";
        } catch (error) {
          outcome[code] = error.name;
        }
      }
      ws.send(JSON.stringify(outcome));
      return;
    }
    if (message === "later") {
      await sleep(100);
    }
    ws.close(4001, "done");
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    if (url.pathname === "/ws-close") {
      return socket(url.searchParams.get("when"));
    }
    if (url.pathname === "/refuse") {
      return refuse(url.searchParams.has("feed"), Number(url.searchParams.get("code") ?? 1008));
    }
    if (url.pathname === "/close-codes") {
      return closeCodes();
    }
    if (url.pathname === "/throwing") {
      return throwing("worker");
    }
    if (url.pathname === "/sse") {
      return sse(Number(url.searchParams.get("events") ?? "3"));
    }
    if (url.pathname.startsWith("/proxy/")) {
      // Forwards to this Worker's own listener, as a Worker in front of an origin.
      url.pathname = url.pathname.slice("/proxy".length);
      return fetch(new Request(url, request));
    }
    if (url.pathname.startsWith("/relay/")) {
      // One object relays every request through the member's socket.
      const stub = env.STREAMS.get(env.STREAMS.idFromName("relay"));
      url.pathname = url.pathname.slice("/relay".length);
      return stub.fetch(new Request(url, request));
    }
    if (url.pathname.startsWith("/object/")) {
      const name = url.searchParams.get("name") ?? "shared";
      const stub = env.STREAMS.get(env.STREAMS.idFromName(name));
      url.pathname = url.pathname.slice("/object".length);
      return stub.fetch(new Request(url, request));
    }
    return new Response("not found", { status: 404 });
  },
};
