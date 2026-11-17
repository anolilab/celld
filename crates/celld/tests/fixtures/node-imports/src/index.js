import { EventEmitter } from "node:events";
import https from "node:https";
import { Socket } from "node:net";

// Named at module scope, called only by code this Worker never runs.
export function unused() {
  return [https.request, new Socket()];
}

export default {
  async fetch() {
    return new Response(String(new EventEmitter().listenerCount("x")));
  },
};
