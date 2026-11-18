// Pushes request bytes through TextDecoder, a TransformStream and TextEncoder.
export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") return new Response("ready");
    if (url.pathname === "/stream") {
      const source = new ReadableStream({
        start(controller) {
          for (const word of ["alpha", "beta", "gamma"]) controller.enqueue(new TextEncoder().encode(word + "\n"));
          controller.close();
        },
      });
      const upper = new TransformStream({
        transform(chunk, controller) {
          controller.enqueue(new TextEncoder().encode(new TextDecoder().decode(chunk).toUpperCase()));
        },
      });
      return new Response(source.pipeThrough(upper), { headers: { "content-type": "text/plain" } });
    }
    const bytes = new Uint8Array(await request.arrayBuffer());
    const text = new TextDecoder().decode(bytes);
    const encoded = new TextEncoder().encode(text);
    const into = new Uint8Array(4);
    const result = new TextEncoder().encodeInto(text, into);
    const tee = new Response(text).body.tee();
    const [a, b] = await Promise.all(tee.map((s) => new Response(s).text()));
    return Response.json({
      byte_length: bytes.length,
      code_points: [...text].length,
      utf16_length: text.length,
      reencoded_equal: encoded.length === bytes.length,
      encoded: [...encoded].map((b) => b.toString(16).padStart(2, "0")).join(""),
      encode_into: { read: result.read, written: result.written },
      latin1: new TextDecoder("latin1").decode(new Uint8Array([0xe9, 0x41])),
      fatal_error: (() => { try { new TextDecoder("utf-8", { fatal: true }).decode(new Uint8Array([0xff])); return null; } catch (e) { return e.name; } })(),
      tee_equal: a === b && a === text,
    });
  },
};
