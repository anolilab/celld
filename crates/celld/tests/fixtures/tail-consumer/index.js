// Receives the trace items of `tail-producer` and prints each batch.
export default {
  async fetch(request) {
    return new Response(new URL(request.url).pathname === "/__ready" ? "ready" : "consumer");
  },
  async tail(events, env, ctx) {
    console.log("TAIL " + JSON.stringify(events));
    for (const event of events) {
      const request = event.event.request;
      if (request) console.log("RAW " + JSON.stringify(request.getUnredacted()));
    }
    if (events.some((event) => event.logs.some((log) => log.message.includes("make-the-consumer-hang")))) {
      await new Promise(() => {});
    }
    if (events.some((event) => event.logs.some((log) => log.message.includes("make-the-consumer-linger")))) {
      // Returns at once, but leaves work that stays pending: a timer that
      // would fire in an hour.
      console.log("LINGER started");
      ctx.waitUntil(new Promise((resolve) => setTimeout(resolve, 3_600_000)));
    }
    if (events.some((event) => event.logs.some((log) => log.message.includes("make-the-consumer-open-a-socket")))) {
      // Returns with a socket still open to the producer.
      console.log("SOCKET started");
      const url = new URL(events[0].event.request.url);
      const socket = new WebSocket(`ws://${url.host}/__ws`);
      socket.addEventListener("message", () => {});
      await new Promise((resolve, reject) => {
        socket.addEventListener("open", resolve);
        socket.addEventListener("error", () => reject(new Error("socket failed")));
      });
      // The handler returns, and `waitUntil` work keeps the event running up
      // to the budget while the socket is open.
      ctx.waitUntil(new Promise((resolve) => setTimeout(resolve, 3_600_000)));
      console.log("SOCKET open");
    }
    if (events.some((event) => event.logs.some((log) => log.message.includes("make-the-consumer-throw")))) {
      throw new Error("consumer exploded");
    }
  },
};
