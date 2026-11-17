import { connect } from "cloudflare:sockets";
import { DurableObject, RpcTarget } from "cloudflare:workers";

class Probe extends RpcTarget {
  ping() {
    return "pong";
  }
}

export class Cell extends DurableObject {
  plain() {
    return { a: 1 };
  }
  list() {
    return [1, 2, 3];
  }
  scalar() {
    return 7;
  }
  nested() {
    return { inner: { b: 2 }, items: [{ c: 3 }] };
  }
  // A plain result forwarded beside an RpcTarget, and the target forwarded
  // alone.
  async forwardWithPlain(name) {
    const plain = await this.env.CELL.getByName(name).plain();
    return { plain, target: new Probe() };
  }
  entries() {
    return globalThis.__celld_stub_entries();
  }
  async forwardStubOnly() {
    return { target: new Probe() };
  }
}

const failure = (error) => ({
  ok: false,
  type: error instanceof TypeError,
  message: String(error?.message ?? error),
});

const hex = (bytes) =>
  [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    const target = url.searchParams.get("url");
    if (url.pathname === "/fetch") {
      try {
        const init = {};
        const mode = url.searchParams.get("encode");
        if (mode) init.encodeResponseBody = mode;
        const method = url.searchParams.get("method");
        if (method) init.method = method;
        const response = await fetch(target, init);
        const bytes = new Uint8Array(await response.arrayBuffer());
        return Response.json({
          ok: true,
          status: response.status,
          contentEncoding: response.headers.get("content-encoding"),
          contentLength: response.headers.get("content-length"),
          size: bytes.length,
          head: hex(bytes.slice(0, 2)),
          text: new TextDecoder().decode(bytes.slice(0, 64)),
        });
      } catch (error) {
        return Response.json(failure(error));
      }
    }
    if (url.pathname === "/connect") {
      try {
        const socket = connect(
          { hostname: "localhost", port: Number(url.searchParams.get("port")) },
          { secureTransport: "on" },
        );
        await socket.opened;
        const writer = socket.writable.getWriter();
        await writer.write(new TextEncoder().encode("ping\n"));
        const reader = socket.readable.getReader();
        const { value } = await reader.read();
        await socket.close();
        return Response.json({ ok: true, reply: new TextDecoder().decode(value) });
      } catch (error) {
        return Response.json(failure(error));
      }
    }
    if (url.pathname === "/dispose") {
      const stub = env.CELL.getByName("rpc");
      const plain = await stub.plain();
      const again = await stub.plain();
      const list = await stub.list();
      const scalar = await stub.scalar();
      const nested = await stub.nested();
      const descriptor = Object.getOwnPropertyDescriptor(plain, Symbol.dispose);
      const out = {
        plain: typeof plain[Symbol.dispose],
        list: typeof list[Symbol.dispose],
        nested: typeof nested[Symbol.dispose],
        scalar: typeof scalar?.[Symbol.dispose],
        enumerable: descriptor?.enumerable,
        keys: Object.keys(plain),
        json: JSON.stringify(plain),
        shared: plain[Symbol.dispose] === again[Symbol.dispose],
        listKeys: Object.keys(list),
      };
      plain[Symbol.dispose]();
      list[Symbol.dispose]();
      return Response.json(out);
    }
    if (url.pathname === "/forward") {
      const stub = env.CELL.getByName("outer");
      // The entries the callee holds after each call, while the caller still
      // holds the result.
      const held = [];
      const results = [];
      for (const method of ["forwardStubOnly", "forwardWithPlain"]) {
        const before = await stub.entries();
        results.push(await stub[method]("inner"));
        held.push((await stub.entries()) - before);
      }
      for (const result of results) result[Symbol.dispose]?.();
      return Response.json({ stubOnly: held[0], withPlain: held[1] });
    }
    if (url.pathname === "/probe") {
      return Response.json({
        probe: typeof globalThis.__celld_stub_entries,
        names: Object.getOwnPropertyNames(globalThis).filter((name) =>
          name.startsWith("__celld")),
      });
    }
    return new Response("not found", { status: 404 });
  },
};
