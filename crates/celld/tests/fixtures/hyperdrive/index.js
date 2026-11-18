import { connect } from "cloudflare:sockets";

// Writes `text` to the socket and returns the first chunk that comes back.
async function exchange(socket, text) {
  const writer = socket.writable.getWriter();
  await writer.write(new TextEncoder().encode(text));
  writer.releaseLock();
  const reader = socket.readable.getReader();
  const { value } = await reader.read();
  reader.releaseLock();
  await socket.close();
  return new TextDecoder().decode(value);
}

const failure = (error) => ({
  ok: false,
  message: String(error?.message ?? error),
});

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") return new Response("ready");
    if (url.pathname === "/info") {
      const hd = env.HD;
      return Response.json({
        connectionString: hd.connectionString,
        host: hd.host,
        port: hd.port,
        user: hd.user,
        password: hd.password,
        database: hd.database,
        bindingKeys: Object.keys(hd),
        json: JSON.stringify(hd),
        plain: env.PLAIN,
        processEnv: process.env.HD ?? null,
        secretVar: env.HYPERDRIVE_HD ?? null,
        localVar:
          env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HD ?? null,
      });
    }
    if (url.pathname === "/via-binding") {
      try {
        return Response.json({ ok: true, reply: await exchange(env.HD.connect(), "ping") });
      } catch (error) {
        return Response.json(failure(error));
      }
    }
    if (url.pathname === "/direct") {
      try {
        const socket = connect({
          hostname: url.searchParams.get("host"),
          port: Number(url.searchParams.get("port")),
        });
        return Response.json({ ok: true, reply: await exchange(socket, "ping") });
      } catch (error) {
        return Response.json(failure(error));
      }
    }
    return new Response("not found", { status: 404 });
  },
};
