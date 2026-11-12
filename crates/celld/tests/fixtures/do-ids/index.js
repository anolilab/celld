import { DurableObject } from "cloudflare:workers";

const throws = (fn) => {
  try {
    fn();
    return false;
  } catch {
    return true;
  }
};

const HEX64 = /^[0-9a-f]{64}$/;

export class Cell extends DurableObject {
  async fetch() {
    const id = this.ctx.id;
    return Response.json({ name: id.name ?? null, id: id.toString() });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/__ready") {
      return new Response("ready");
    }
    const ns = env.OBJ;
    if (url.pathname === "/names") {
      const a1 = ns.idFromName("alpha");
      const a2 = ns.idFromName("alpha");
      const b = ns.idFromName("beta");
      return Response.json({
        sameString: a1.toString() === a2.toString(),
        equals: a1.equals(a2),
        differentString: a1.toString() !== b.toString(),
        differentEquals: a1.equals(b),
        hex: HEX64.test(a1.toString()),
        name: a1.name,
      });
    }
    if (url.pathname === "/roundtrip") {
      const named = ns.idFromName("alpha");
      const unique = ns.newUniqueId();
      return Response.json({
        namedEquals: ns.idFromString(named.toString()).equals(named),
        namedString: ns.idFromString(named.toString()).toString() === named.toString(),
        uniqueEquals: ns.idFromString(unique.toString()).equals(unique),
        uniqueHex: HEX64.test(unique.toString()),
      });
    }
    if (url.pathname === "/unique") {
      const ids = Array.from({ length: 5 }, () => ns.newUniqueId());
      return Response.json({
        distinct: new Set(ids.map((id) => id.toString())).size,
        equalsOther: ids[0].equals(ids[1]),
        nameUndefined: ids[0].name === undefined,
      });
    }
    if (url.pathname === "/malformed") {
      return Response.json({
        notHex: throws(() => ns.idFromString("not-a-durable-object-id")),
        tooShort: throws(() => ns.idFromString("abcd")),
        empty: throws(() => ns.idFromString("")),
      });
    }
    if (url.pathname === "/inside") {
      const id = ns.idFromName(url.searchParams.get("name"));
      const inside = await ns.get(id).fetch("http://object/");
      return Response.json({ ...(await inside.json()), outside: id.toString() });
    }
    return new Response("unknown", { status: 404 });
  },
};
