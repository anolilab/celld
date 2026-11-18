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
  const out = { response: text };
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
// An error event, or an upstream that ends without `[DONE]`, errors the
// stream: a Worker must not take a cut-off answer for a finished one.
function __aiEventStream(streamId) {
  const reader = new CelldHttpBodyStream(streamId).getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let ended = false;
  const finish = (controller) => {
    controller.enqueue(__aiEncoder.encode("data: [DONE]\n\n"));
    controller.close();
    reader.cancel().catch(() => {});
  };
  const fail = (controller, error) => {
    controller.error(error);
    reader.cancel().catch(() => {});
  };
  return new ReadableStream({
    async pull(controller) {
      for (;;) {
        const newline = buffer.indexOf("\n");
        if (newline < 0) {
          if (ended) {
            return fail(
              controller, __aiError("the upstream stream ended before [DONE]"));
          }
          const { done, value } = await reader.read();
          if (done) {
            ended = true;
            buffer += decoder.decode();
            if (buffer !== "") buffer += "\n";
          } else {
            buffer += decoder.decode(value, { stream: true });
          }
          continue;
        }
        const line = buffer.slice(0, newline).replace(/\r$/, "");
        buffer = buffer.slice(newline + 1);
        if (!line.startsWith("data:")) continue;
        const data = line.slice(5).trim();
        if (data === "[DONE]") return finish(controller);
        let event;
        try {
          event = JSON.parse(data);
        } catch {
          continue;
        }
        if (event?.error !== undefined && event.error !== null) {
          return fail(controller, __aiStreamError(event));
        }
        const out = __aiEventLine(event);
        if (out !== null) {
          controller.enqueue(__aiEncoder.encode(out));
          return;
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
      const out = { response: answer.choices?.[0]?.message?.content ?? "" };
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
      if (!Array.isArray(answer.data)) {
        throw __aiError("the upstream answered no embeddings");
      }
      const data = [...answer.data]
        .sort((a, b) => (a.index ?? 0) - (b.index ?? 0))
        .map((item) => item.embedding);
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
