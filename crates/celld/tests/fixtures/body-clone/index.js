// Request and Response clone() over streaming bodies. The same checks run
// under Node (`node index.js`), which is the reference for each one.
const encoder = new TextEncoder();

function stream(...chunks) {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  });
}

function equal(actual, expected, what) {
  if (actual !== expected) throw new Error(`${what}: got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)}`);
}

async function readAll(body) {
  const reader = body.getReader();
  const parts = [];
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    parts.push(new TextDecoder().decode(value));
  }
  return parts.join("");
}

async function refused(run) {
  try {
    await run();
  } catch (error) {
    return error instanceof TypeError ? "TypeError" : `${error?.name}: ${error?.message}`;
  }
  return "no error";
}

const makers = {
  Response: (body, init) => new Response(body, init),
  Request: (body, init) => new Request("https://example.com/", { method: "POST", ...init, body, duplex: "half" }),
};

const checks = {};
for (const [kind, make] of Object.entries(makers)) {
  checks[`${kind}: a clone of a streaming body reads the same bytes`] = async () => {
    const original = make(stream("hello ", "streaming ", "world"));
    const copy = original.clone();
    equal(await copy.text(), "hello streaming world", "clone text");
    equal(await original.text(), "hello streaming world", "original text");
  };
  checks[`${kind}: the original still streams after a clone`] = async () => {
    const original = make(stream("one", "two"));
    const copy = original.clone();
    equal(await readAll(original.body), "onetwo", "original stream");
    equal(await readAll(copy.body), "onetwo", "clone stream");
  };
  checks[`${kind}: a clone keeps the headers and does not share them`] = async () => {
    const original = make(stream("x"), { headers: { "x-one": "1" } });
    const copy = original.clone();
    equal(copy.headers.get("x-one"), "1", "cloned header");
    copy.headers.set("x-one", "2");
    equal(original.headers.get("x-one"), "1", "original header");
    await copy.text();
    await original.text();
  };
  checks[`${kind}: a clone of a clone`] = async () => {
    const original = make(stream("a", "b", "c"));
    const first = original.clone();
    const second = first.clone();
    equal(await second.text(), "abc", "second");
    equal(await first.text(), "abc", "first");
    equal(await original.text(), "abc", "original");
  };
  checks[`${kind}: the branches are read in either order`] = async () => {
    const original = make(stream("late", "read"));
    const copy = original.clone();
    equal(await original.text(), "lateread", "original first");
    equal(await copy.text(), "lateread", "clone second");
  };
  checks[`${kind}: a clone is refused once the body has been read`] = async () => {
    const original = make(stream("gone"));
    await original.text();
    equal(await refused(() => original.clone()), "TypeError", "clone after text()");
  };
  checks[`${kind}: a clone is refused while a reader holds the body`] = async () => {
    const original = make(stream("held"));
    const reader = original.body.getReader();
    equal(await refused(() => original.clone()), "TypeError", "clone with a reader");
    reader.releaseLock();
  };
  checks[`${kind}: a clone is refused once a reader has read from the body`] = async () => {
    const original = make(stream("part", "ial"));
    const reader = original.body.getReader();
    await reader.read();
    reader.releaseLock();
    equal(original.bodyUsed, true, "bodyUsed after a read");
    equal(await refused(() => original.clone()), "TypeError", "clone after a read");
  };
  checks[`${kind}: a clone is refused once a reader has finished the body`] = async () => {
    const original = make(stream("all"));
    await readAll(original.body);
    equal(original.bodyUsed, true, "bodyUsed after reading everything");
    equal(await refused(() => original.clone()), "TypeError", "clone after the read");
  };
  checks[`${kind}: a clone without a body has none`] = async () => {
    const original = kind === "Response" ? new Response(null) : new Request("https://example.com/");
    const copy = original.clone();
    equal(copy.body, null, "clone body");
    equal(await copy.text(), "", "clone text");
    equal(original.bodyUsed, false, "bodyUsed");
  };
  checks[`${kind}: bodyUsed follows a read of a buffered body`] = async () => {
    const original = make("buffered");
    equal(original.bodyUsed, false, "before");
    await readAll(original.body);
    equal(original.bodyUsed, true, "after a reader read it");
    equal(await refused(() => original.clone()), "TypeError", "clone after the read");
  };
  checks[`${kind}: a clone of a buffered body still works`] = async () => {
    const original = make("buffered");
    const copy = original.clone();
    equal(await copy.text(), "buffered", "clone");
    equal(await original.text(), "buffered", "original");
  };
}

export default {
  async fetch(request) {
    if (new URL(request.url).pathname === "/__ready") return new Response("ready");
    const results = {};
    for (const [name, check] of Object.entries(checks)) {
      try {
        await check();
        results[name] = "ok";
      } catch (error) {
        results[name] = `fail: ${error?.message ?? error}`;
      }
    }
    return Response.json(results);
  },
};
