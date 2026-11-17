import "node:vm";
export * from "node:tty";
import { EventEmitter } from "node:events";

const cluster = () => import("node:cluster");
const dgram = () => require("dgram");
const rl = () => __require("node:readline");

export default {
  async fetch() {
    void [EventEmitter, cluster, dgram, rl];
    return new Response("ok");
  },
};
