// Parses a JSON request body and answers with a derived JSON document.
export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") return new Response("ready");
    let input;
    try {
      input = await request.json();
    } catch (error) {
      return Response.json({ error: "invalid json", name: error.name }, { status: 400 });
    }
    const numbers = Array.isArray(input.numbers) ? input.numbers : [];
    return Response.json({
      count: numbers.length,
      sum: numbers.reduce((a, b) => a + b, 0),
      sorted: [...numbers].sort((a, b) => a - b),
      upper: String(input.name ?? "").toUpperCase(),
      nested: { keys: Object.keys(input).sort(), big: 2 ** 53, neg_zero: Object.is(-0, input.zero) },
      id: crypto.randomUUID(),
      received_at: new Date().toISOString(),
      took_ms: performance.now(),
    });
  },
};
