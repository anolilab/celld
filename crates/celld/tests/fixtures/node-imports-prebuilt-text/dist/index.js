// import("node:tty") is only a comment.
const text = 'require("node:cluster")';
const letters = Array.from("node:dgram");
const vm = () => import(/* a comment in front */ "node:vm");

export default {
  async fetch() {
    void [text, letters, vm];
    return new Response("ok");
  },
};
