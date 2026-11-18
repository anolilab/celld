// send_email bindings driven over HTTP.
//   POST /send   { binding, kind: "raw", from, to, raw, pad?, stream? }
//                { binding, kind: "builder", message }
//                answers { messageId } or { error, code, message }
//   POST /burst  { count, binding? } sends count builder messages at once and
//                answers one { ok } or { code } for each
//   POST /streams { count } sends count raw stream messages at once and
//                answers { results, reads }: one { ok } or { code } for each, and
//                how many streams were read from at all
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

// `size` characters: lines of 100 by default, or `size` of one wide
// character, whose UTF-8 form is longer than its length says.
const padding = (size, wide) => {
  if (wide) return "\u00e9".repeat(size);
  const line = "x".repeat(99) + "\n";
  return line.repeat(Math.floor(size / 100)) + "x".repeat(size % 100);
};

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
    if (url.pathname === "/burst") {
      const { count, binding = "EMAIL" } = await request.json();
      const results = await Promise.allSettled(
        Array.from({ length: count }, (_, index) =>
          env[binding].send({ from: "ann@example.com", to: "bob@example.com", subject: `m${index}`, text: "t" }),
        ),
      );
      return Response.json(
        results.map((result) => (result.status === "fulfilled" ? { ok: true } : { code: result.reason.code })),
      );
    }
    if (url.pathname === "/streams") {
      const { count } = await request.json();
      let reads = 0;
      const results = await Promise.allSettled(
        Array.from({ length: count }, () => {
          // A zero high-water mark: nothing is pulled until a reader asks.
          const stream = new ReadableStream(
            {
              pull(controller) {
                reads += 1;
                controller.enqueue(new TextEncoder().encode("From: ann@example.com\n\nbody\n"));
                controller.close();
              },
            },
            { highWaterMark: 0 },
          );
          return env.EMAIL.send(new EmailMessage("ann@example.com", "bob@example.com", stream));
        }),
      );
      return Response.json({
        results: results.map((result) => (result.status === "fulfilled" ? { ok: true } : { code: result.reason.code })),
        reads,
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
          if (spec.pad) raw += padding(spec.pad, spec.wide);
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
