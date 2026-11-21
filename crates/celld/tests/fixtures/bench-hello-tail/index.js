// `bench-hello` with a tail consumer: every request produces a trace item.
export default {
  fetch(request) {
    if (new URL(request.url).pathname === "/__ready") {
      return new Response("ready");
    }
    return new Response("hello\n");
  },
};
