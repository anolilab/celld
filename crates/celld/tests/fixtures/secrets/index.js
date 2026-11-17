export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname === "/__ready") {
      return new Response("ready");
    }
    return Response.json({
      plain: env.PLAIN,
      token: env.API_TOKEN ?? null,
      type: typeof env.API_TOKEN,
    });
  },
};
