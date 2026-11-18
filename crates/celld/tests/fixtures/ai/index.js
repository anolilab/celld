const CHAT = "@cf/meta/llama-3.1-8b-instruct";
const EMBED = "@cf/baai/bge-base-en-v1.5";

async function attempt(work) {
  try {
    return { ok: true, value: await work() };
  } catch (error) {
    return { ok: false, name: error.name, message: String(error.message) };
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    switch (url.pathname) {
      case "/__ready":
        return new Response("ready");
      case "/messages":
        return Response.json(await env.AI.run(CHAT, {
          messages: [
            { role: "system", content: "be brief" },
            { role: "user", content: "hi" },
          ],
          max_tokens: 7,
          temperature: 0.5,
        }));
      case "/prompt":
        return Response.json(await env.AI.run(CHAT, { prompt: "once upon" }));
      case "/embed-one":
        return Response.json(await env.AI.run(EMBED, { text: "alpha" }));
      case "/embed-many":
        return Response.json(await env.AI.run(EMBED, { text: ["a", "bb", "ccc"] }));
      case "/stream": {
        const stream = await env.AI.run(CHAT, { prompt: "stream", stream: true });
        return new Response(stream, {
          headers: { "content-type": "text/event-stream" },
        });
      }
      case "/stream-multiline":
      case "/stream-cr":
      case "/stream-too-big":
      case "/stream-echo-key":
      case "/stream-error":
      case "/stream-cut":
      case "/stream-bad-json":
      case "/stream-long-line": {
        // Read the stream to its end, keeping what arrived before it failed.
        let text = "";
        const result = await attempt(async () => {
          const stream = await env.AI.run(CHAT, {
            prompt: url.pathname.slice(1),
            stream: true,
          });
          const decoder = new TextDecoder();
          for await (const chunk of stream) text += decoder.decode(chunk);
        });
        return Response.json({ ...result, text });
      }
      case "/echo-key":
        return Response.json(await attempt(() => env.AI.run(CHAT, { prompt: "echo-key" })));
      case "/bad-shape":
        return Response.json(await attempt(() => env.AI.run(CHAT, { prompt: "bad-shape" })));
      case "/bad-embedding":
        return Response.json(await attempt(() => env.AI.run(EMBED, { text: "bad-shape" })));
      case "/big-answer":
        return Response.json(await attempt(() => env.AI.run(CHAT, { prompt: "big" })));
      case "/big-request":
        return Response.json(
          await attempt(() => env.AI.run(CHAT, { prompt: "x".repeat(33 << 20) })));
      case "/unmapped":
        return Response.json(await attempt(() => env.AI.run("@cf/nope/none", { prompt: "x" })));
      case "/badinput":
        return Response.json(await attempt(() => env.AI.run(CHAT, { image: [1] })));
      case "/upstream-error":
        return Response.json(await attempt(() => env.AI.run(CHAT, { prompt: "fail" })));
      case "/binding":
        return Response.json({
          keys: Object.keys(env.AI),
          dump: JSON.stringify(env),
        });
      default:
        return new Response("not found", { status: 404 });
    }
  },
};
