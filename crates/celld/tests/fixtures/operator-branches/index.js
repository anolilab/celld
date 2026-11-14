// The KV namespace and the queue are cells of runtime classes. A Worker holds a
// stub of each through its binding, and the cell's `fetch` is the operator's
// route, so what a stub may ask there is what these routes try.

const post = (body) => ({
  method: "POST",
  headers: { "content-type": "application/json", "x-cells-operator-ingress": "1" },
  body: JSON.stringify(body),
});

const KV_BODIES = {
  info: { op: "info" },
  get: { op: "get", key: "k" },
  put: { op: "put", key: "k", value: [1, 2, 3] },
  list: { op: "list", limit: 10 },
  delete: { op: "delete", keys: ["k"] },
};

const QUEUE_BODIES = {
  info: { op: "info" },
  peek: { op: "peek", limit: 5 },
  purge: { op: "purge" },
  pause: { op: "pause" },
  resume: { op: "resume" },
  redrive: { op: "redrive" },
  settle: { msgId: "x" },
};

const attempt = async (stub, url, body) => {
  const response = await stub.fetch(url, post(body));
  return { status: response.status, body: (await response.text()).slice(0, 160) };
};

const rpc = async (stub, method, args) => {
  try {
    await stub[method](...args);
    return { ok: true };
  } catch (error) {
    return { ok: false, message: String(error && error.message) };
  }
};

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = url.searchParams.get("k") ?? "k";
    const value = url.searchParams.get("v") ?? "v";
    if (url.pathname === "/__ready") return new Response("ready");

    // ---- through the bindings ----
    if (url.pathname === "/kv/put") {
      await env.KV.put(key, value);
      return new Response("ok");
    }
    if (url.pathname === "/kv/get") return Response.json(await env.KV.get(key));
    if (url.pathname === "/kv/list") {
      return Response.json((await env.KV.list()).keys.map((entry) => entry.name));
    }
    if (url.pathname === "/queue/send") {
      await env.JOBS.send({ id: key });
      return new Response("sent");
    }

    // ---- through a stub ----
    if (url.pathname === "/kv/stub-fetch") {
      return Response.json(
        await attempt(env.KV._stub, "http://kv/", KV_BODIES[url.searchParams.get("op")]),
      );
    }
    if (url.pathname === "/queue/stub-fetch") {
      const path = url.searchParams.get("op") === "settle" ? "/__qSettle" : "/";
      return Response.json(
        await attempt(env.JOBS._stub, `http://queue${path}`, QUEUE_BODIES[url.searchParams.get("op")]),
      );
    }
    if (url.pathname === "/kv/stub-rpc") {
      const method = url.searchParams.get("method");
      const args = {
        __kvGet: [{ keys: ["k"], withMetadata: false, now: Date.now() }],
        __kvPut: [{ entries: [{ key: "r", value: new Uint8Array([1]), tag: "bytes", metadata: null, expiresAt: null }] }],
        __kvDelete: [{ keys: ["r"] }],
        __kvList: [{ prefix: "", limit: 5, after: "", now: Date.now() }],
        __kvMetrics: [{ now: Date.now() }],
        __kvSweep: [{ now: Date.now(), limit: 5 }],
        _open: [],
        alarm: [],
      }[method];
      return Response.json(await rpc(env.KV._stub, method, args));
    }
    if (url.pathname === "/queue/stub-rpc") {
      const method = url.searchParams.get("method");
      const args = {
        __queueMetrics: [],
        _purge: [],
        _setPaused: [true],
        _redrive: [10],
        _peek: [5],
        alarm: [],
      }[method];
      return Response.json(await rpc(env.JOBS._stub, method, args));
    }
    return new Response("not found", { status: 404 });
  },

  // The consumer: records what it received, through the KV binding.
  async queue(batch, env) {
    for (const message of batch.messages) {
      await env.KV.put(`done:${message.body.id}`, "1");
      message.ack();
    }
  },
};
