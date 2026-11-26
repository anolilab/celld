// Throws whatever message the request names, so a test can try to pass an
// ordinary Worker failure off as a routing refusal.
export default {
  fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    throw new Error(url.searchParams.get("m") ?? "");
  },
};
