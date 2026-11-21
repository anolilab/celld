// The traced Worker: its trace items go to the `tail-consumer` project.
export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    console.log("served", url.pathname);
    if (url.pathname === "/fail") {
      throw new Error("producer failed");
    }
    if (url.pathname === "/burn") {
      // CPU the item must report, and wall time that includes it. The clock
      // does not advance inside a handler, so this counts rather than waits.
      let sum = 0;
      for (let i = 0; i < 2e8; i++) sum += Math.sqrt(i);
      console.log("burned", sum > 0);
    }
    if (url.pathname === "/hang") {
      // The consumer never returns from this one.
      console.log("make-the-consumer-hang");
    }
    if (url.pathname === "/poison") {
      // The consumer throws on this one; the response must not change.
      console.log("make-the-consumer-throw");
    }
    return new Response("produced");
  },
};
