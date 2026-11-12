import { DurableObject } from "cloudflare:workers";

// Deliveries are recorded in storage, not in memory, so the log survives the
// object being evicted. `fail` makes the first n deliveries throw.
export class Cell extends DurableObject {
  async alarm(alarmInfo) {
    const storage = this.ctx.storage;
    const log = (await storage.get("log")) ?? [];
    log.push({
      at: Date.now(),
      retryCount: alarmInfo?.retryCount ?? null,
      isRetry: alarmInfo?.isRetry ?? null,
      duringAlarm: await storage.getAlarm(),
    });
    await storage.put("log", log);
    if (log.length <= ((await storage.get("fail")) ?? 0)) {
      throw new Error("alarm failure " + log.length);
    }
  }

  async fetch(request) {
    const url = new URL(request.url);
    const storage = this.ctx.storage;
    const ms = Number(url.searchParams.get("ms") ?? 0);
    switch (url.pathname) {
      case "/get":
        return Response.json({ alarm: await storage.getAlarm() });
      case "/set": {
        const fail = url.searchParams.get("fail");
        if (fail) await storage.put("fail", Number(fail));
        const at = Date.now() + ms;
        await storage.setAlarm(at);
        return Response.json({ at, alarm: await storage.getAlarm() });
      }
      case "/delete": {
        await storage.setAlarm(Date.now() + 60000);
        await storage.deleteAlarm();
        return Response.json({ alarm: await storage.getAlarm() });
      }
      case "/replace": {
        await storage.setAlarm(Date.now() + 500);
        const at = Date.now() + ms;
        await storage.setAlarm(at);
        return Response.json({ at, alarm: await storage.getAlarm() });
      }
      case "/log":
        return Response.json({
          log: (await storage.get("log")) ?? [],
          alarm: await storage.getAlarm(),
        });
    }
    return new Response("unknown", { status: 404 });
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    const name = url.searchParams.get("o") ?? "default";
    return env.OBJ.get(env.OBJ.idFromName(name)).fetch(request);
  },
};
