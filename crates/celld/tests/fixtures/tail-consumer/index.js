// Receives the trace items of `tail-producer` and prints each batch.
export default {
  async fetch(request) {
    return new Response(new URL(request.url).pathname === "/__ready" ? "ready" : "consumer");
  },
  async tail(events) {
    console.log("TAIL " + JSON.stringify(events));
    for (const event of events) {
      const request = event.event.request;
      if (request) console.log("RAW " + JSON.stringify(request.getUnredacted()));
    }
    if (events.some((event) => event.logs.some((log) => log.message.includes("make-the-consumer-hang")))) {
      await new Promise(() => {});
    }
    if (events.some((event) => event.logs.some((log) => log.message.includes("make-the-consumer-throw")))) {
      throw new Error("consumer exploded");
    }
  },
};
