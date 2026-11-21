export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname === "/__ready") {
      return new Response("ready");
    }
    return Response.json({
      store: await env.STORE_KEY.get(),
      listed: env.LISTED ?? null,
      unlisted: env.UNLISTED ?? null,
    });
  },
};
