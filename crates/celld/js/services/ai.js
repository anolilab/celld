// ---- Workers AI ----------------------------------------------------------
// `env.AI.run(model, input, options?)` for text generation and embeddings.
// The host owns the endpoint, the key and the model map (`__ai_run`); this
// side only turns a Workers AI input into an OpenAI-shaped body and the
// upstream answer into the Workers AI shape. The route is chosen by the input,
// not by the model: `messages` or `prompt` generate text, `text` embeds.

const __AI_SAMPLING = [
  "max_tokens", "temperature", "top_p", "seed",
  "frequency_penalty", "presence_penalty",
];
// The most of one upstream event a stream holds while it waits for the
// event's end; a longer one errors the stream.
const __AI_MAX_LINE = 1 << 20;
// The most of an upstream stream a call reads, as for a whole answer.
const __AI_MAX_STREAM = 32 << 20;
const __aiEncoder = new TextEncoder();
const __aiError = (message) => new Error("AiError: " + message);

function __aiChatBody(model, inputs) {
  let messages;
  if (Array.isArray(inputs.messages)) {
    messages = inputs.messages.map((message) => {
      if (
        message === null || typeof message !== "object" ||
        typeof message.role !== "string" || typeof message.content !== "string"
      ) {
        throw new TypeError(
          `AI.run(${model}): each message needs a string role and a string content`);
      }
      return { role: message.role, content: message.content };
    });
  } else {
    messages = [{ role: "user", content: inputs.prompt }];
  }
  const body = { messages };
  for (const name of __AI_SAMPLING) {
    if (inputs[name] !== undefined) body[name] = inputs[name];
  }
  if (inputs.stream === true) body.stream = true;
  return body;
}

// One upstream server-sent event in Workers AI's shape, or null when it
// carries nothing the Worker reads (a role-only first chunk, a keep-alive).
function __aiEventLine(event) {
  const delta = event?.choices?.[0]?.delta?.content;
  const text = typeof delta === "string" ? delta : "";
  if (text === "" && !event?.usage) return null;
  // The node's key may be written with escapes the raw text did not match.
  const out = { response: __ai_redact(text) };
  if (event.usage) out.usage = event.usage;
  return `data: ${JSON.stringify(out)}\n\n`;
}

// The message of an error event the upstream put in the stream, without the
// node's key and at most a short line, as a pre-stream error is.
function __aiStreamError(event) {
  const error = event.error;
  const message = typeof error === "string" ? error : error?.message ?? JSON.stringify(error);
  return __aiError(
    "the upstream reported an error: " + __ai_redact(String(message)).slice(0, 2048));
}

// The upstream's OpenAI stream as Workers AI's: `data: {"response":"..."}`
// events, then `data: [DONE]`. It reads upstream only as the Worker reads it.
// The upstream's events are read as server-sent events: lines end in CRLF, LF
// or CR, an event is its `data:` lines up to a blank line joined with LF, and
// comments and other fields are ignored. An error event, an event that is not
// JSON, an event past 1 MiB, a stream past 32 MiB in all, or an upstream that
// ends without `[DONE]` errors the stream: a Worker must not take a damaged or
// cut-off answer for a finished one. The node's key is removed from each
// event before it is read.
function __aiEventStream(streamId) {
  const reader = new CelldHttpBodyStream(streamId).getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = "";
  let ended = false;
  let received = 0;
  let dataLines = [];
  let dataSize = 0;
  const finish = (controller) => {
    controller.enqueue(__aiEncoder.encode("data: [DONE]\n\n"));
    controller.close();
    reader.cancel().catch(() => {});
  };
  const fail = (controller, error) => {
    controller.error(error);
    reader.cancel().catch(() => {});
  };
  // Reads one complete event; true when the pull is over (an event went out,
  // the stream ended or it failed).
  const dispatch = (controller) => {
    const data = __ai_redact(dataLines.join("\n")).trim();
    dataLines = [];
    dataSize = 0;
    // An event with no data, such as a bare `data:` line, carries nothing.
    if (data === "") return false;
    if (data === "[DONE]") {
      finish(controller);
      return true;
    }
    let event;
    try {
      event = JSON.parse(data);
    } catch {
      fail(controller, __aiError("the upstream sent a stream event that is not JSON"));
      return true;
    }
    if (event?.error !== undefined && event.error !== null) {
      fail(controller, __aiStreamError(event));
      return true;
    }
    const out = __aiEventLine(event);
    if (out === null) return false;
    controller.enqueue(__aiEncoder.encode(out));
    return true;
  };
  const tooLarge = (controller) =>
    fail(controller, __aiError("the upstream sent an event larger than 1 MiB"));
  return new ReadableStream({
    async pull(controller) {
      for (;;) {
        const end = /\r\n|\n|\r/.exec(buffer);
        // A CR at the end of the buffer may be half of a CRLF.
        if (end !== null && (ended || end.index + end[0].length < buffer.length)) {
          if (end.index > __AI_MAX_LINE) return tooLarge(controller);
          const line = buffer.slice(0, end.index);
          buffer = buffer.slice(end.index + end[0].length);
          if (line === "") {
            if (dataLines.length > 0 && dispatch(controller)) return;
            continue;
          }
          const colon = line.indexOf(":");
          if (colon === 0 || (colon < 0 ? line : line.slice(0, colon)) !== "data") continue;
          let value = colon < 0 ? "" : line.slice(colon + 1);
          if (value.startsWith(" ")) value = value.slice(1);
          dataSize += value.length + 1;
          if (dataSize > __AI_MAX_LINE) return tooLarge(controller);
          dataLines.push(value);
          continue;
        }
        if (buffer.length > __AI_MAX_LINE) return tooLarge(controller);
        if (ended) {
          // The last event may end with the stream and not with a blank line.
          if (dataLines.length > 0 && dispatch(controller)) return;
          return fail(controller, __aiError("the upstream stream ended before [DONE]"));
        }
        let done, value;
        try {
          ({ done, value } = await reader.read());
        } catch {
          // The read error names the request URL, whose path may hold a credential.
          return fail(controller, __aiError("reading the upstream stream failed"));
        }
        try {
          if (done) {
            ended = true;
            buffer += decoder.decode();
            if (buffer !== "") buffer += "\n";
          } else {
            received += value.byteLength;
            if (received > __AI_MAX_STREAM) {
              return fail(
                controller,
                __aiError("the upstream stream is larger than 32 MiB"));
            }
            buffer += decoder.decode(value, { stream: true });
          }
        } catch {
          return fail(controller, __aiError("the upstream stream is not valid UTF-8"));
        }
      }
    },
    cancel(reason) {
      return reader.cancel(reason);
    },
  }, { highWaterMark: 0 });
}

__celld.__makeAi = () => ({
  async run(model, inputs, options) {
    if (typeof model !== "string" || model === "") {
      throw new TypeError("AI.run: the model name must be a non-empty string");
    }
    if (inputs === null || typeof inputs !== "object") {
      throw new TypeError(`AI.run(${model}): the input must be an object`);
    }
    if (Array.isArray(inputs.messages) || typeof inputs.prompt === "string") {
      const body = __aiChatBody(model, inputs);
      if (body.stream) {
        const id = await __ai_run("chat", model, JSON.stringify(body), true);
        return __aiEventStream(id);
      }
      const answer = __aiJson(
        await __ai_run("chat", model, JSON.stringify(body), false));
      const content = answer?.choices?.[0]?.message?.content;
      if (typeof content !== "string") {
        throw __aiError(
          "the upstream answer has no choices[0].message.content text");
      }
      // The node's key may be written with escapes the raw text did not match.
      const out = { response: __ai_redact(content) };
      if (answer.usage) out.usage = answer.usage;
      return out;
    }
    const text = inputs.text;
    if (
      typeof text === "string" ||
      (Array.isArray(text) && text.every((item) => typeof item === "string"))
    ) {
      const answer = __aiJson(
        await __ai_run("embeddings", model, JSON.stringify({ input: text }), false));
      if (
        !Array.isArray(answer?.data) ||
        !answer.data.every((item) => Array.isArray(item?.embedding))
      ) {
        throw __aiError("the upstream answer has no data[].embedding vectors");
      }
      const data = [...answer.data]
        .sort((a, b) => (a.index ?? 0) - (b.index ?? 0))
        .map((item) => item.embedding);
      if (
        !data.every((vector) =>
          vector.length === data[0].length &&
          vector.every((value) => typeof value === "number" && Number.isFinite(value)))
      ) {
        throw __aiError(
          "the upstream embeddings are not finite numbers of one length");
      }
      return { shape: [data.length, data[0]?.length ?? 0], data };
    }
    throw new TypeError(
      `AI.run(${model}): the input needs \`messages\` or \`prompt\` to generate text, or \`text\` to embed`);
  },
});

function __aiJson(text) {
  try {
    return JSON.parse(text);
  } catch {
    throw __aiError("the upstream did not answer JSON");
  }
}
