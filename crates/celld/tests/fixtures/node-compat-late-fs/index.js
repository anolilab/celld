// The Worker never imports `node:fs` statically. The first request loads it
// halfway through, so it began before the filesystem existed and has no
// request-local `/tmp`; the next request does.
let loaded = false;

const tryWrite = (fs, name) => {
  try {
    fs.writeFileSync(`/tmp/${name}`, "x");
    return "written";
  } catch (error) {
    return `${error.code}: ${error.message}`;
  }
};

export default {
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (path === "/__ready") return new Response("ready");
    if (path !== "/write") return new Response("not found", { status: 404 });
    const first = !loaded;
    loaded = true;
    const fs = await import("node:fs");
    const write = tryWrite(fs, "late");
    return Response.json({
      first,
      write,
      sees: fs.existsSync("/tmp/late"),
      listed: fs.readdirSync("/tmp"),
    });
  },
};
