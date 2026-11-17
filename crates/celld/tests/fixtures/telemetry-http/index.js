export default {
  fetch(request) {
    if (new URL(request.url).pathname === "/__ready") {
      return new Response("ready");
    }
    return new Response("teapot", { status: request.method === "PATCH" ? 418 : 202 });
  },
};
