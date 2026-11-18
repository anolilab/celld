// Echoes what the Worker sees of a request, and sets response headers of its own.
export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") return new Response("ready");
    if (url.pathname === "/missing") {
      return new Response("no such page", { status: 404, headers: { "content-type": "text/plain;charset=UTF-8" } });
    }
    if (url.pathname === "/redirect") {
      return Response.redirect(new URL("/echo?from=redirect", request.url).toString(), 302);
    }
    const lines = [
      `method=${request.method}`,
      `path=${url.pathname}`,
      `query=${url.search}`,
      `probe=${request.headers.get("x-probe")}`,
      `type=${request.headers.get("content-type")}`,
      `body=${await request.text()}`,
    ];
    const headers = new Headers({ "content-type": "text/plain; charset=utf-8", "x-echo-method": request.method });
    headers.append("x-multi", "a");
    headers.append("x-multi", "b");
    headers.set("cache-control", "no-store");
    headers.set("x-request-id", crypto.randomUUID());
    return new Response(lines.join("\n"), { headers });
  },
};
