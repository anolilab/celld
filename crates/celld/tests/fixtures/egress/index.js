import { connect } from "cloudflare:sockets";

const failure = (error) => ({
  ok: false,
  type: error instanceof TypeError,
  message: String(error?.message ?? error),
});

export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    const target = url.searchParams.get("url");
    if (url.pathname === "/fetch") {
      try {
        const response = await fetch(target, {
          redirect: url.searchParams.get("redirect") ?? "follow",
        });
        return Response.json({
          ok: true,
          status: response.status,
          location: response.headers.get("location"),
          body: await response.text(),
        });
      } catch (error) {
        return Response.json(failure(error));
      }
    }
    if (url.pathname === "/upgrade") {
      try {
        const response = await fetch(target, { headers: { Upgrade: "websocket" } });
        return Response.json({ ok: true, status: response.status });
      } catch (error) {
        return Response.json(failure(error));
      }
    }
    if (url.pathname === "/connect") {
      try {
        const socket = connect({
          hostname: url.searchParams.get("host"),
          port: Number(url.searchParams.get("port")),
        });
        await socket.opened;
        await socket.close();
        return Response.json({ ok: true });
      } catch (error) {
        return Response.json(failure(error));
      }
    }
    return new Response("not found", { status: 404 });
  },
};
