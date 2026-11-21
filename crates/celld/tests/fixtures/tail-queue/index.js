// A Worker that sends to its own queue; its queue handler is traced.
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    await env.JOBS.send(url.pathname.slice(1));
    return new Response("sent");
  },
  async queue(batch) {
    console.log("handled", batch.messages.map((message) => message.body).join(","));
    if (batch.messages.some((message) => message.body === "bad")) {
      throw new Error("job failed");
    }
  },
};
