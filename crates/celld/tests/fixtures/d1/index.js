// A D1 database driven through its binding, so a test can compare what the
// binding returns with what the operator route returns for the same SQL.

const SCHEMA = `
CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL);
`;

const count = async (env) =>
  await env.DB.prepare("SELECT count(*) AS n FROM notes").first("n");

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") return new Response("ready");
    await env.DB.exec(SCHEMA);

    // The binding's own batch(), for the operator route to be compared with.
    if (url.pathname === "/binding-batch") {
      const results = await env.DB.batch([
        env.DB.prepare("INSERT INTO notes (body) VALUES (?)").bind("bound"),
        env.DB.prepare("SELECT id, body FROM notes WHERE body = ?").bind("bound"),
      ]);
      return Response.json({ results });
    }
    if (url.pathname === "/count") return Response.json({ count: await count(env) });
    return new Response("not found", { status: 404 });
  },
};
