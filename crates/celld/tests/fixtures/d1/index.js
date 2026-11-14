// D1 databases driven through their bindings, so a test can compare what a
// binding returns with what the operator route returns for the same SQL, and
// can try as a Worker what only the operator may do.

const SCHEMA = `
CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL);
`;

const SEED = `
CREATE TABLE export_notes (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT, raw BLOB);
CREATE TABLE export_tags (id INTEGER PRIMARY KEY AUTOINCREMENT, note_id INTEGER, tag TEXT);
CREATE TABLE export_area (
  w INTEGER, h INTEGER,
  virt INTEGER GENERATED ALWAYS AS (w * h) VIRTUAL,
  stored INTEGER GENERATED ALWAYS AS (w + h) STORED
);
CREATE TABLE export_loose (a TEXT, b INTEGER);
CREATE VIRTUAL TABLE export_docs USING fts5(title, body);
CREATE INDEX export_notes_body ON export_notes (body);
CREATE VIEW export_bodies AS SELECT id, body FROM export_notes;
CREATE TRIGGER export_note_tag AFTER INSERT ON export_notes
BEGIN
  INSERT INTO export_tags (note_id, tag) VALUES (new.id, 'auto');
  INSERT INTO export_tags (note_id, tag) VALUES (new.id, 'seen');
END;
INSERT INTO export_notes (body, raw) VALUES ('it''s', X'00ff'), (CAST(X'6100620063' AS TEXT), NULL), ('gone', NULL), ('two
lines;', NULL);
DELETE FROM export_notes WHERE body = 'gone';
DELETE FROM export_tags;
INSERT INTO export_tags (note_id, tag) VALUES (1, 'a'), (2, 'b');
INSERT INTO export_area (w, h) VALUES (3, 4);
INSERT INTO export_loose VALUES ('x', 1), ('y', 2), ('z', 3);
DELETE FROM export_loose WHERE a = 'y';
INSERT INTO export_docs (title, body) VALUES ('Moby', 'call me ishmael');
`;

// Every value as bytes, so a NUL or a bad byte that JSON would hide is seen.
const FINGERPRINT = [
  "SELECT id, hex(CAST(body AS BLOB)) AS body, hex(raw) AS raw FROM export_notes ORDER BY id",
  "SELECT id, note_id, tag FROM export_tags ORDER BY id",
  "SELECT w, h, virt, stored FROM export_area ORDER BY w",
  "SELECT rowid, a, b FROM export_loose ORDER BY rowid",
  "SELECT name, seq FROM sqlite_sequence ORDER BY name",
  "SELECT rowid FROM export_docs WHERE export_docs MATCH 'ishmael'",
  "SELECT type, name FROM sqlite_schema ORDER BY name",
  "SELECT id, hex(CAST(body AS BLOB)) FROM export_bodies ORDER BY id",
  "SELECT name, sql FROM sqlite_schema WHERE type IN ('trigger', 'view') ORDER BY name",
];

const fingerprint = async (db) => {
  const rows = [];
  for (const sql of FINGERPRINT) rows.push(await db.prepare(sql).raw());
  return rows;
};

const count = async (env) =>
  await env.DB.prepare("SELECT count(*) AS n FROM notes").first("n");

const STUB_BODIES = {
  export: { export: { schema: true, data: true } },
  migrate: { migrate: { name: "stub", sql: "select 1;", table: "d1_migrations" } },
  "exec-rows": { exec: { sql: "select 1", rows: true } },
  "exec-count": { exec: { sql: "select 1", rows: false } },
  statements: { statements: [{ sql: "select 1 as one", params: [] }] },
};

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

    if (url.pathname === "/seed") {
      await env.DB.exec(SEED);
      return Response.json({ seeded: true });
    }
    // Replay a dump into the second database, as `celld d1 execute --file`
    // would: through the same exec, under the same authorizer and limits.
    if (url.pathname === "/replay") {
      const result = await env.COPY.exec(await request.text());
      return Response.json({ count: result.count });
    }
    if (url.pathname === "/fingerprints") {
      return Response.json({
        source: await fingerprint(env.DB),
        copy: await fingerprint(env.COPY),
      });
    }

    // What a Worker holding a stub can reach. The forged header must not
    // matter: the operator mark is not something a request carries.
    if (url.pathname === "/stub-fetch") {
      const body = STUB_BODIES[url.searchParams.get("kind")];
      const headers = url.searchParams.has("forge")
        ? { "x-cells-operator-ingress": "1" }
        : {};
      const response = await env.DB._stub.fetch("http://d1/", {
        method: "POST",
        headers,
        body: JSON.stringify(body),
      });
      return Response.json({
        status: response.status,
        body: (await response.text()).slice(0, 200),
      });
    }
    if (url.pathname === "/stub-rpc") {
      const method = url.searchParams.get("method");
      const args = {
        __d1Query: [[{ sql: "select 1 as one", params: [] }]],
        __d1Batch: [[{ sql: "select 1 as one", params: [] }]],
        __d1Exec: ["select 1"],
        __d1Migrate: ["stub", "select 1;", "d1_migrations"],
        __d1Export: [{}],
      }[method];
      try {
        await env.DB._stub[method](...args);
        return Response.json({ ok: true });
      } catch (error) {
        return Response.json({ ok: false, message: String(error && error.message) });
      }
    }
    return new Response("not found", { status: 404 });
  },
};
