// Rate limit bindings driven over HTTP.
//   /limit/<binding>/<key>      one limit() call, answers { success }
//   /raw/<binding>?options=json limit() with arbitrary options, answers
//                               { success } or { error, message }
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") return new Response("ready");
    const [, route, binding, key] = url.pathname.split("/");
    if (route === "limit") {
      return Response.json(await env[binding].limit({ key: decodeURIComponent(key) }));
    }
    if (route === "raw") {
      try {
        const options = JSON.parse(url.searchParams.get("options") ?? "null");
        return Response.json(await env[binding].limit(options));
      } catch (error) {
        return Response.json({ error: error.name, message: error.message });
      }
    }
    return new Response("ok");
  },
};
