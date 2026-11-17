import "node:vm";
export * from "node:tty";
import { EventEmitter } from "node:events";

const cluster = () => import("node:cluster");
const dgram = () => require("dgram");

export default {
  async fetch() {
    void [EventEmitter, cluster, dgram];
    return new Response("ok");
  },
};
