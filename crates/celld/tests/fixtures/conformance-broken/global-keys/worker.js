// Deliberately differs between runtimes: the number of own properties of
// globalThis is a property of the runtime, not of the program. Used to prove
// that the harness reports a difference. Not part of the seed corpus.
export default {
  async fetch(request) {
    if (new URL(request.url).pathname === "/__ready") return new Response("ready");
    return Response.json({ keys: Object.getOwnPropertyNames(globalThis).length });
  },
};
