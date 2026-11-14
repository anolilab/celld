import { DurableObject } from "cloudflare:workers";

export class Cell extends DurableObject {
  held = null;

  // Keeps the stream it was given past the end of the call.
  hold(stream) {
    this.held = stream;
  }

  async readHeld() {
    return await new Response(this.held).text();
  }

  // Runs in a cell of its own, which shares an isolate with the cell it calls.
  async run() {
    const other = this.env.CELL.getByName("holder");
    let controller;
    const stream = new ReadableStream({
      start(c) {
        controller = c;
      },
    });
    await other.hold(stream);
    // The call has returned; the sender still feeds the stream.
    try {
      controller.enqueue(new TextEncoder().encode("sent after the call"));
      controller.close();
    } catch (error) {
      return "the sender's stream was cancelled: " + String(error && error.message || error);
    }
    try {
      return await other.readHeld();
    } catch (error) {
      return "error: " + String(error && error.message || error);
    }
  }
}

export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname === "/__ready") {
      return new Response("ready");
    }
    return new Response(await env.CELL.getByName("caller").run());
  },
};
