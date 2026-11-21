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
    if (events.some((event) => event.logs.some((log) => log.message.includes("make-the-consumer-throw")))) {
      throw new Error("consumer exploded");
    }
  },
};
