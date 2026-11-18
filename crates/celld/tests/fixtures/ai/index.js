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
