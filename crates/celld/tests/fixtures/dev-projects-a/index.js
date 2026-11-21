// The primary: serves requests and calls the other project through its binding.
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    if (url.pathname === "/own") {
      return Response.json({ greeting: env.GREETING ?? null, key: await env.KEY.get() });
    }
    const rest = url.pathname === "/" ? "" : url.pathname.slice(1);
    const answer = await env.B.fetch(new Request(`http://b/${rest}`));
    return new Response(`a:${await answer.text()}`);
  },
};
