const revision = "one";

export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname === "/__ready") {
      return new Response("ready");
    }
    const meta = env.CF_VERSION_METADATA;
    return Response.json({
      revision,
      id: meta.id,
      tag: meta.tag,
      timestamp: meta.timestamp,
      frozen: Object.isFrozen(meta),
    });
  },
};
