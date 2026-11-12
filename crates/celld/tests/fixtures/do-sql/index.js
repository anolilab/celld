import { DurableObject } from "cloudflare:workers";

const fails = (fn) => {
  try {
    fn();
    return false;
  } catch {
    return true;
  }
};

const seed = (sql) => {
  sql.exec("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
  sql.exec(
    "INSERT INTO people (name, age) VALUES (?, ?), (?, ?), (?, ?)",
    "ann", 30, "bob", 25, "cy", 41,
  );
};

const scenarios = {
  bindings(sql) {
    seed(sql);
    const rows = sql.exec("SELECT name, age FROM people WHERE age > ? ORDER BY age", 28).toArray();
    const none = sql.exec("SELECT * FROM people WHERE name = ?", "zed").toArray();
    return { rows, none };
  },
  one(sql) {
    seed(sql);
    return {
      row: sql.exec("SELECT name FROM people WHERE id = ?", 1).one(),
      zeroThrows: fails(() => sql.exec("SELECT name FROM people WHERE id = ?", 99).one()),
      manyThrows: fails(() => sql.exec("SELECT name FROM people").one()),
    };
  },
  raw(sql) {
    seed(sql);
    const cursor = sql.exec("SELECT id, name FROM people ORDER BY id");
    const names = [...cursor.columnNames];
    return { names, raw: [...cursor.raw()] };
  },
  counters(sql) {
    sql.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    const insert = sql.exec("INSERT INTO t (v) VALUES ('a'), ('b'), ('c')");
    insert.toArray();
    const select = sql.exec("SELECT * FROM t");
    select.toArray();
    return {
      rowsWritten: insert.rowsWritten,
      rowsRead: select.rowsRead,
      selectWritten: select.rowsWritten,
    };
  },
  size(sql) {
    return { type: typeof sql.databaseSize, nonNegative: sql.databaseSize >= 0 };
  },
  reserved(sql) {
    return {
      readThrows: fails(() => sql.exec("SELECT * FROM _cf_KV").toArray()),
      createThrows: fails(() => sql.exec("CREATE TABLE _cf_mine (x)").toArray()),
    };
  },
  rollback(sql, storage) {
    sql.exec("CREATE TABLE t (v INTEGER)");
    sql.exec("INSERT INTO t VALUES (1)");
    let message = null;
    try {
      storage.transactionSync(() => {
        sql.exec("INSERT INTO t VALUES (2)");
        throw new Error("boom");
      });
    } catch (error) {
      message = error.message;
    }
    const returned = storage.transactionSync(() => {
      sql.exec("INSERT INTO t VALUES (3)");
      return "returned";
    });
    const values = sql.exec("SELECT v FROM t ORDER BY v").toArray().map((row) => row.v);
    return { message, values, returned };
  },
  multi(sql) {
    const cursor = sql.exec(
      "CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (1), (2); SELECT v FROM t ORDER BY v",
    );
    return { columnNames: [...cursor.columnNames], rows: cursor.toArray() };
  },
  write(sql) {
    seed(sql);
    return { written: true };
  },
  read(sql) {
    return sql.exec("SELECT name, age FROM people ORDER BY id").toArray();
  },
};

export class Cell extends DurableObject {
  async fetch(request) {
    const name = new URL(request.url).pathname.slice(1);
    const scenario = scenarios[name];
    if (!scenario) return new Response("unknown scenario " + name, { status: 404 });
    return Response.json(scenario(this.ctx.storage.sql, this.ctx.storage));
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    // One object per scenario unless the test names one with ?o=.
    const name = url.searchParams.get("o") ?? url.pathname;
    return env.OBJ.get(env.OBJ.idFromName(name)).fetch(request);
  },
};
