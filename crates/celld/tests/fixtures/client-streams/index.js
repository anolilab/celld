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

export class Streams extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
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
    if (url.pathname === "/sse") {
      return sse(Number(url.searchParams.get("events") ?? "3"));
    }
    if (url.pathname.startsWith("/proxy/")) {
      // Forwards to this Worker's own listener, as a Worker in front of an origin.
      url.pathname = url.pathname.slice("/proxy".length);
      return fetch(new Request(url, request));
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
