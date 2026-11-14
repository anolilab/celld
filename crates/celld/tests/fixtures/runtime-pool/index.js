import { DurableObject } from "cloudflare:workers";

// Tenant code that watches the language's own entry points. Every isolate that
// loads this module runs the hooks, so what they catch says which cells share
// that isolate with tenant code.
const caught = [];
const note = (hook, text) => {
  if (typeof text === "string" && text.includes("CANARY")) {
    caught.push(`${hook}: ${text.slice(0, 100)}`);
  }
};
const parse = JSON.parse;
JSON.parse = function (text, ...rest) {
  note("JSON.parse", text);
  return parse.call(this, text, ...rest);
};
const stringify = JSON.stringify;
JSON.stringify = function (...args) {
  const text = stringify.apply(this, args);
  note("JSON.stringify", text);
  return text;
};
const decode = TextDecoder.prototype.decode;
TextDecoder.prototype.decode = function (...args) {
  const text = decode.apply(this, args);
  note("TextDecoder.decode", text);
  return text;
};
globalThis.__tenantCaught = caught;

// The tenant's own cell, in the isolates the tenant's cells run in.
export class Spy extends DurableObject {
  captured() {
    return globalThis.__tenantCaught.slice();
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") return new Response("ready");

    if (url.pathname === "/d1") {
      await env.DB.exec("CREATE TABLE IF NOT EXISTS t (v TEXT)");
      await env.DB.prepare("INSERT INTO t (v) VALUES (?)").bind("CANARY-d1-row").run();
      const row = await env.DB.prepare("SELECT v FROM t").first();
      return Response.json(row);
    }
    if (url.pathname === "/kv") {
      await env.KV.put("CANARY-kv-key", "CANARY-kv-value");
      return Response.json({
        value: await env.KV.get("CANARY-kv-key"),
        keys: (await env.KV.list()).keys.map((entry) => entry.name),
      });
    }
    if (url.pathname === "/queue") {
      await env.JOBS.send({ id: "m1", note: "CANARY-queue-body" });
      return new Response("sent");
    }
    if (url.pathname === "/consumed") {
      return Response.json(await env.KV.get("consumed:m1"));
    }
    if (url.pathname === "/spy") {
      return Response.json(await env.SPY.getByName("s").captured());
    }
    return new Response("not found", { status: 404 });
  },

  async queue(batch, env) {
    for (const message of batch.messages) {
      await env.KV.put(`consumed:${message.body.id}`, "yes");
      message.ack();
    }
  },
};
