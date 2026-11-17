const HOUR = 60 * 60 * 1000;

// `/pin` answers at once and leaves a timer behind it in `waitUntil`. `/hang`
// never answers. Either one keeps its isolate in use for an hour, longer than
// any test runs.
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    switch (url.pathname) {
      case "/__ready":
        return new Response("ready");
      case "/version":
        return new Response("v1");
      case "/pin":
        ctx.waitUntil(new Promise((resolve) => setTimeout(resolve, HOUR)));
        return new Response("pinned");
      case "/hang":
        await new Promise((resolve) => setTimeout(resolve, HOUR));
        return new Response("late");
      default:
        return new Response("not found", { status: 404 });
    }
  },
};
