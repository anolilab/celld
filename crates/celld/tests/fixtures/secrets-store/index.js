export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname === "/__ready") {
      return new Response("ready");
    }
    let value = null;
    let error = null;
    try {
      value = await env.STORE_KEY.get();
    } catch (e) {
      error = String(e.message ?? e);
    }
    return Response.json({
      value,
      error,
      plain: env.PLAIN,
      bindingKeys: Object.keys(env.STORE_KEY),
      json: JSON.stringify(env.STORE_KEY),
      processEnv: process.env.STORE_KEY ?? null,
    });
  },
};
