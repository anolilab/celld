// Reports how URL and URLSearchParams parse and serialise what the request carries.
export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") return new Response("ready");
    const target = url.searchParams.get("u") ?? "";
    let parsed;
    try {
      const u = new URL(target, "https://base.example/dir/page");
      u.searchParams.sort();
      parsed = {
        href: u.href,
        origin: u.origin,
        protocol: u.protocol,
        host: u.host,
        hostname: u.hostname,
        port: u.port,
        pathname: u.pathname,
        search: u.search,
        hash: u.hash,
        username: u.username,
        pairs: [...u.searchParams],
      };
    } catch (error) {
      parsed = { error: error.name };
    }
    const params = new URLSearchParams("a=1&a=2&b=%20x+y&c");
    return Response.json({
      parsed,
      params: { all_a: params.getAll("a"), b: params.get("b"), c: params.get("c"), string: params.toString(), has_d: params.has("d") },
      can_parse: [URL.canParse("http://x"), URL.canParse("nope")],
      pattern: new URLPattern({ pathname: "/books/:id" }).exec("https://x.example/books/42")?.pathname.groups,
    });
  },
};
