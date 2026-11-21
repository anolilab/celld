export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname === "/__ready") {
      return new Response("ready");
    }
    return Response.json({
      listed: env.LISTED ?? null,
      unlisted: env.UNLISTED ?? null,
      absent: env.ABSENT ?? null,
    });
  },
};
