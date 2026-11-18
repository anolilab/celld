// send_email bindings driven over HTTP.
//   POST /send   { binding, kind: "raw", from, to, raw, pad?, stream? }
//                { binding, kind: "builder", message }
//                answers { messageId } or { error, code, message }
//   GET  /env    what a Worker can see of its environment
import { EmailMessage } from "cloudflare:email";
import process from "node:process";

const streamOf = (text) =>
  new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(text));
      controller.close();
    },
  });

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") return new Response("ready");
    if (url.pathname === "/env") {
      return Response.json({
        keys: Object.keys(env),
        env: JSON.stringify(env),
        process: JSON.stringify(process.env),
        binding: JSON.stringify(env.EMAIL),
        members: [
          ...Object.getOwnPropertyNames(env.EMAIL),
          ...Object.getOwnPropertyNames(Object.getPrototypeOf(env.EMAIL)),
        ],
      });
    }
    if (url.pathname === "/send") {
      const spec = await request.json();
      try {
        let message;
        if (spec.kind === "builder") {
          message = spec.message;
        } else {
          let raw = spec.raw ?? "";
          if (spec.pad) raw += "x".repeat(spec.pad);
          message = new EmailMessage(spec.from, spec.to, spec.stream ? streamOf(raw) : raw);
        }
        return Response.json(await env[spec.binding].send(message));
      } catch (error) {
        return Response.json({ error: error.name, code: error.code, message: error.message });
      }
    }
    return new Response("ok");
  },
};
