// `cloudflare:email` and the `send_email` binding. The binding only shapes the
// arguments; the host op `__email_send` checks them against the deployment's
// restrictions and sends the message through the node's SMTP relay.
(function () {
  const MAX_MESSAGE_BYTES = 25 * 1024 * 1024;
  // How long a raw stream may take to yield its whole message. A stream that
  // stalls past it is cancelled and send() rejects, so it cannot hold the
  // isolate's send slot for good.
  const READ_DEADLINE_MS = 60 * 1000;
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
    let timer;
    const expired = new Promise((_, reject) => {
      timer = setTimeout(() => {
        // Not awaited: a stalled stream may never settle its cancel.
        reader.cancel().catch(() => {});
        reject(fail("E_TIMEOUT", "the raw stream did not finish within 60 seconds"));
      }, READ_DEADLINE_MS);
    });
    try {
      await Promise.race([readChunks(reader, chunks), expired]);
    } finally {
      clearTimeout(timer);
    }
    const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
    const bytes = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return bytes;
  };

  const readChunks = async (reader, chunks) => {
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

  // Hand a message to a host op, which checks it and sends it: `rawHead` and
  // `builderHead` make the op's JSON for each form of a message. A raw stream
  // is read after its slot and subrequest are taken, so sends past the limit
  // never read theirs.
  const submit = async (message, op, rawHead, builderHead, usage) => {
    let answer;
    if (message instanceof EmailMessage) {
      const raw = raws.get(message);
      let token;
      if (typeof raw !== "string") {
        const reserved = JSON.parse(__email_reserve());
        if (!reserved.ok) throw fail(reserved.code, reserved.message);
        token = reserved.token;
      }
      let handedOver = false;
      try {
        const bytes = await readRaw(raw);
        handedOver = token !== undefined;
        answer = await op(rawHead(message), bytes, token);
      } finally {
        if (token !== undefined && !handedOver) __email_release(token);
      }
    } else if (message !== null && typeof message === "object") {
      answer = await op(builderHead(shapeBuilder(message)), undefined);
    } else {
      throw new TypeError(usage);
    }
    const result = JSON.parse(answer);
    if (!result.ok) throw fail(result.code, result.message);
    return { messageId: result.messageId };
  };

  class SendEmail {
    #name;

    constructor(name) {
      this.#name = name;
    }

    send(message) {
      return submit(
        message,
        __email_send,
        (raw) => JSON.stringify({ binding: this.#name, kind: "raw", from: raw.from, to: raw.to }),
        (shaped) => JSON.stringify({ binding: this.#name, kind: "builder", message: shaped }),
        "send() needs an EmailMessage or an object with from, to, subject and text or html",
      );
    }
  }

  // The `message` of an `email()` handler. The envelope, the received bytes
  // and the thread stay with the host: `forward()` and `reply()` name an
  // action, and the host decides what it may do. `setReject()` and the actions
  // end with the handler, so a message kept past it cannot bounce a mail that
  // was accepted or send one later.
  const makeInbound = (meta, bytes) => {
    let active = true;
    let rejection = null;
    const checkActive = () => {
      if (!active) throw new Error("Email message methods cannot be called after the handler returns.");
    };
    const headers = new Headers();
    // A header a Headers cannot hold (a name with a separator, a value past
    // Latin-1) is left out rather than failing the whole message; `raw` has it.
    for (const [name, value] of meta.headers) {
      try {
        headers.append(name, value);
      } catch {
        try {
          headers.append(name, value.replace(/[^\x20-\xff]/g, "?"));
        } catch {}
      }
    }
    const raw = new ReadableStream({
      start(controller) {
        controller.enqueue(bytes);
        controller.close();
      },
    });
    const message = {
      get from() { return meta.from; },
      get to() { return meta.to; },
      get headers() { return headers; },
      get raw() { return raw; },
      get rawSize() { return meta.rawSize; },
      get canBeForwarded() { return true; },
      setReject(reason) {
        checkActive();
        rejection = String(reason);
      },
      async forward(rcptTo, extra) {
        checkActive();
        if (typeof rcptTo !== "string") {
          throw new TypeError("Incorrect type for the 'rcptTo' argument: the provided value is not of type 'string'.");
        }
        if (extra !== undefined && !(extra instanceof Headers)) {
          throw new TypeError("Incorrect type for the 'headers' argument: the provided value is not of type 'Headers'.");
        }
        const answer = JSON.parse(await __email_forward(
          JSON.stringify({ to: rcptTo, headers: extra === undefined ? [] : [...extra] }),
        ));
        if (!answer.ok) throw fail(answer.code, answer.message);
        return { messageId: answer.messageId };
      },
      reply(reply) {
        checkActive();
        // The host allows one reply per message, once a send is admitted.
        return submit(
          reply,
          __email_reply,
          (raw) => JSON.stringify({ kind: "raw", from: raw.from, to: raw.to }),
          (shaped) => JSON.stringify({ kind: "builder", message: shaped }),
          "reply() needs an EmailMessage or an object with from, subject and text or html",
        );
      },
    };
    return {
      message,
      rejection: () => rejection,
      end: () => { active = false; },
    };
  };

  __celld.__cfEmail = { EmailMessage };
  __celld.__makeSendEmail = (name) => new SendEmail(name);
  __celld.__makeInboundEmail = makeInbound;
})();
