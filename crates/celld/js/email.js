// `cloudflare:email` and the `send_email` binding. The binding only shapes the
// arguments; the host op `__email_send` checks them against the deployment's
// restrictions and sends the message through the node's SMTP relay.
(function () {
  const MAX_MESSAGE_BYTES = 25 * 1024 * 1024;
  const raws = new WeakMap();

  const fail = (code, message) => {
    const error = new Error(message);
    error.code = code;
    return error;
  };

  class EmailMessage {
    constructor(from, to, raw) {
      if (typeof from !== "string") {
        throw new TypeError("Incorrect type for the 'from' argument: the provided value is not of type 'string'.");
      }
      if (typeof to !== "string") {
        throw new TypeError("Incorrect type for the 'to' argument: the provided value is not of type 'string'.");
      }
      if (typeof raw !== "string" && !(raw instanceof ReadableStream)) {
        throw new TypeError("Incorrect type for the 'raw' argument: the provided value is not of type 'ReadableStream or string'.");
      }
      Object.defineProperty(this, "from", { value: from, enumerable: true });
      Object.defineProperty(this, "to", { value: to, enumerable: true });
      raws.set(this, raw);
    }
  }

  // The bytes of a raw message. A stream is read up to the limit and no
  // further, so a Worker cannot make the node buffer more than that.
  const readRaw = async (raw) => {
    if (typeof raw === "string") {
      if (raw.length > MAX_MESSAGE_BYTES) {
        throw fail("E_CONTENT_TOO_LARGE", "the message is too large");
      }
      // The limit is in bytes, and a character is up to three of them.
      const bytes = new TextEncoder().encode(raw);
      if (bytes.byteLength > MAX_MESSAGE_BYTES) {
        throw fail("E_CONTENT_TOO_LARGE", "the message is too large");
      }
      return bytes;
    }
    const reader = raw.getReader();
    const chunks = [];
    let total = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!(value instanceof Uint8Array)) {
        await reader.cancel();
        throw new TypeError("the raw stream of an EmailMessage must yield Uint8Array chunks");
      }
      total += value.byteLength;
      if (total > MAX_MESSAGE_BYTES) {
        await reader.cancel();
        throw fail("E_CONTENT_TOO_LARGE", "the message is too large");
      }
      chunks.push(value);
    }
    const bytes = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return bytes;
  };

  const base64 = (bytes) => {
    let text = "";
    for (let i = 0; i < bytes.length; i += 0x8000) {
      text += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
    }
    return btoa(text);
  };

  // The builder as plain data. Attachment content given as bytes becomes
  // base64, a string is taken as base64 already, and what is not an
  // attachment is left for the host to refuse.
  const shapeBuilder = (message) => {
    const shaped = { ...message };
    let total = 0;
    for (const field of ["text", "html"]) {
      if (typeof message[field] === "string") total += message[field].length;
    }
    if (total > MAX_MESSAGE_BYTES) {
      throw fail("E_CONTENT_TOO_LARGE", "the message is too large");
    }
    if (Array.isArray(message.attachments)) {
      shaped.attachments = message.attachments.map((attachment) => {
        if (attachment === null || typeof attachment !== "object") return attachment;
        const { content } = attachment;
        const copy = { ...attachment };
        if (typeof content === "string") {
          total += content.length;
        } else if (content instanceof ArrayBuffer || ArrayBuffer.isView(content)) {
          const bytes = content instanceof ArrayBuffer
            ? new Uint8Array(content)
            : new Uint8Array(content.buffer, content.byteOffset, content.byteLength);
          total += bytes.byteLength;
          if (total > MAX_MESSAGE_BYTES) {
            throw fail("E_CONTENT_TOO_LARGE", "the message is too large");
          }
          copy.content = base64(bytes);
        }
        if (total > MAX_MESSAGE_BYTES) {
          throw fail("E_CONTENT_TOO_LARGE", "the message is too large");
        }
        return copy;
      });
    }
    return shaped;
  };

  class SendEmail {
    #name;

    constructor(name) {
      this.#name = name;
    }

    async send(message) {
      let answer;
      if (message instanceof EmailMessage) {
        const bytes = await readRaw(raws.get(message));
        answer = await __email_send(
          JSON.stringify({ binding: this.#name, kind: "raw", from: message.from, to: message.to }),
          bytes,
        );
      } else if (message !== null && typeof message === "object") {
        answer = await __email_send(
          JSON.stringify({ binding: this.#name, kind: "builder", message: shapeBuilder(message) }),
          undefined,
        );
      } else {
        throw new TypeError("send() needs an EmailMessage or an object with from, to, subject and text or html");
      }
      const result = JSON.parse(answer);
      if (!result.ok) throw fail(result.code, result.message);
      return { messageId: result.messageId };
    }
  }

  __celld.__cfEmail = { EmailMessage };
  __celld.__makeSendEmail = (name) => new SendEmail(name);
})();
