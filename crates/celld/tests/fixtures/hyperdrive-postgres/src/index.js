// The `postgres` driver, unchanged, over the Hyperdrive binding: the
// connection string alone tells it where the database is.
import postgres from "postgres";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") return new Response("ready");
    const sql = postgres(env.HD.connectionString, {
      max: 1,
      fetch_types: false,
      ssl: false,
    });
    try {
      await sql`create table if not exists hd_items (id serial primary key, name text not null)`;
      await sql`truncate hd_items`;
      await sql`insert into hd_items (name) values (${"alpha"}), (${"beta"})`;
      const rows = await sql`select id, name from hd_items order by id`;
      const [who] = await sql`select current_user as user, current_database() as database`;
      return Response.json({ rows: [...rows], who });
    } catch (error) {
      return Response.json({ error: String(error?.message ?? error) }, { status: 500 });
    } finally {
      await sql.end({ timeout: 1 });
    }
  },
};
