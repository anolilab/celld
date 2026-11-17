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

// A WebSocket that refuses the first message it gets (a handler may close with
// 1000 or 3000-4999 only; `close(1008)` throws); with `feed` it also
// sends a tick every 100 ms until it closes.
const refuse = (feed) => {
  const [client, server] = Object.values(new WebSocketPair());
  server.accept();
  let ticker;
  if (feed) {
    ticker = setInterval(() => server.send("tick"), 100);
  }
  server.addEventListener("message", () => {
    clearInterval(ticker);
    server.close(4008, "refused");
  });
  server.addEventListener("close", () => clearInterval(ticker));
  return new Response(null, { status: 101, webSocket: client });
};

export class Streams extends DurableObject {
  member = null;
  pending = new Map();
  next = 0;

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/refuse") {
      return refuse(url.searchParams.has("feed"));
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
      return refuse(url.searchParams.has("feed"));
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
