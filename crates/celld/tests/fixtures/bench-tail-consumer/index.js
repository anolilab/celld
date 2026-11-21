// Takes the trace items of `bench-hello-tail` and does nothing with them.
export default {
  fetch(request) {
    return new Response(new URL(request.url).pathname === "/__ready" ? "ready" : "consumer");
  },
  tail() {},
};
