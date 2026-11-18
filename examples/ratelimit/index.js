export default {
  async fetch(request, env) {
    const key = new URL(request.url).pathname;
    const { success } = await env.LIMITER.limit({ key });
    return success
      ? new Response(`Allowed ${key}.`)
      : new Response("Too many requests.", { status: 429 });
  },
};
