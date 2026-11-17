import process, { env as namedEnv } from "node:process";
import fs from "node:fs";
import fsp from "node:fs/promises";
import url, { fileURLToPath, pathToFileURL } from "node:url";

// Each check returns normally when it holds and throws when it does not, so a
// failed one reports what it saw. Expected values are what Node answers on a
// POSIX host.
const results = async (checks) => {
  const out = {};
  for (const [name, check] of Object.entries(checks)) {
    try {
      await check();
      out[name] = "ok";
    } catch (error) {
      out[name] = String(error?.stack ?? error);
    }
  }
  return out;
};

const is = (actual, expected) => {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) throw new Error(`expected ${e}, got ${a}`);
};

const throwsCode = async (code, run) => {
  try {
    await run();
  } catch (error) {
    is(error.code, code);
    return;
  }
  throw new Error(`expected ${code}, nothing was thrown`);
};

const processChecks = {
  global: () => is(process === globalThis.process, true),
  named: () => is(namedEnv === process.env, true),
  bare: async () => {
    const bare = await import("process");
    is(bare.default === globalThis.process, true);
  },
  getBuiltinModule: () => is(process.getBuiltinModule("node:process") === process, true),
};

const urlChecks = {
  exports: () => {
    is(url.URL === globalThis.URL, true);
    is(url.URLSearchParams === globalThis.URLSearchParams, true);
    is(url.fileURLToPath === fileURLToPath, true);
  },
  encodesSpecials: () =>
    is(pathToFileURL("/a b/c%d?e#f").href, "file:///a%20b/c%25d%3Fe%23f"),
  resolvesRelative: () => is(pathToFileURL("rel/x/../y/").href, "file:///rel/y/"),
  encodesUnicodeAndBackslash: () =>
    is(pathToFileURL("/tmp/日本\\n").href, "file:///tmp/%E6%97%A5%E6%9C%AC%5Cn"),
  decodes: () => is(fileURLToPath("file:///a%20b/%25"), "/a b/%"),
  decodesUnicode: () => is(fileURLToPath("file:///%E6%97%A5"), "/日"),
  acceptsUrlObject: () => is(fileURLToPath(new URL("file:///x/y")), "/x/y"),
  acceptsLocalhost: () => is(fileURLToPath("file://localhost/x"), "/x"),
  roundTrip: () => {
    const path = "/tmp/a b?c#d%e";
    is(fileURLToPath(pathToFileURL(path)), path);
  },
  encodedSlash: () => throwsCode("ERR_INVALID_FILE_URL_PATH", () => fileURLToPath("file:///a%2Fb")),
  foreignHost: () => throwsCode("ERR_INVALID_FILE_URL_HOST", () => fileURLToPath("file://host/x")),
  notFile: () => throwsCode("ERR_INVALID_URL_SCHEME", () => fileURLToPath("http://x/y")),
  notAUrl: () => throwsCode("ERR_INVALID_ARG_TYPE", () => fileURLToPath(5)),
  pathNotAString: () => throwsCode("ERR_INVALID_ARG_TYPE", () => pathToFileURL(5)),
  formatUrl: () =>
    is(
      url.format(new URL("http://u:p@a.com/x?y#z"), { auth: false, fragment: false, search: false }),
      "http://a.com/x",
    ),
  formatObject: () =>
    is(
      url.format({ protocol: "https", host: "a.com:8", pathname: "/x y", query: { a: 1 }, hash: "h", auth: "u:p" }),
      "https://u:p@a.com:8/x y?a=1#h",
    ),
  parseFailsLoudly: () => throwsCode("ERR_METHOD_NOT_IMPLEMENTED", () => url.parse("http://a/")),
};

const fsChecks = {
  writeAndRead: () => {
    fs.writeFileSync("/tmp/a.txt", "hello");
    is(fs.readFileSync("/tmp/a.txt", "utf8"), "hello");
    const stat = fs.statSync("/tmp/a.txt");
    is([stat.isFile(), stat.isDirectory(), stat.size], [true, false, 5]);
    fs.accessSync("/tmp/a.txt", fs.constants.W_OK);
  },
  overwrite: () => {
    fs.writeFileSync("/tmp/o.txt", "long contents");
    fs.writeFileSync("/tmp/o.txt", "ab");
    is(fs.readFileSync("/tmp/o.txt", "utf8"), "ab");
  },
  append: () => {
    fs.appendFileSync("/tmp/a.txt", " world");
    fs.writeFileSync("/tmp/a.txt", "!", { flag: "a" });
    is(fs.readFileSync("/tmp/a.txt", "utf8"), "hello world!");
    is(fs.statSync("/tmp/a.txt").size, 12);
  },
  binary: () => {
    fs.writeFileSync("/tmp/b.bin", new Uint8Array([0, 255, 7]));
    is(Array.from(fs.readFileSync("/tmp/b.bin")), [0, 255, 7]);
  },
  exclusive: () => throwsCode("EEXIST", () => fs.writeFileSync("/tmp/a.txt", "x", { flag: "wx" })),
  readdir: () => {
    fs.mkdirSync("/tmp/d");
    fs.writeFileSync("/tmp/d/x", "1");
    is(fs.readdirSync("/tmp/d"), ["x"]);
    is(fs.readdirSync("/tmp", { recursive: true }).includes("d/x"), true);
    const entries = fs.readdirSync("/tmp/d", { withFileTypes: true });
    is([entries[0].name, entries[0].isFile(), entries[0].isDirectory()], ["x", true, false]);
    is(fs.readdirSync("/tmp").includes("d"), true);
  },
  readdirNotADirectory: () => throwsCode("ENOTDIR", () => fs.readdirSync("/tmp/a.txt")),
  readdirMissing: () => throwsCode("ENOENT", () => fs.readdirSync("/tmp/missing")),
  missingParent: () => throwsCode("ENOENT", () => fs.writeFileSync("/tmp/missing/x", "1")),
  writeToDirectory: () => throwsCode("EISDIR", () => fs.writeFileSync("/tmp/d", "1")),
  writeOutsideTmp: () => throwsCode("EPERM", () => fs.writeFileSync("/etc/x", "1")),
  mkdirOverFile: () => throwsCode("EEXIST", () => fs.mkdirSync("/tmp/a.txt")),
  unlink: () => {
    fs.unlinkSync("/tmp/o.txt");
    is(fs.existsSync("/tmp/o.txt"), false);
  },
  unlinkMissing: () => throwsCode("ENOENT", () => fs.unlinkSync("/tmp/o.txt")),
  unlinkDirectory: () => throwsCode("EISDIR", () => fs.unlinkSync("/tmp/d")),
  rmDirectoryNeedsRecursive: () => throwsCode("ERR_FS_EISDIR", () => fs.rmSync("/tmp/d")),
  rmRecursive: () => {
    fs.rmSync("/tmp/d", { recursive: true });
    is(fs.existsSync("/tmp/d"), false);
    is(fs.existsSync("/tmp/d/x"), false);
  },
  rmForce: () => fs.rmSync("/tmp/missing", { force: true }),
  rmMissing: () => throwsCode("ENOENT", () => fs.rmSync("/tmp/missing")),
  promises: async () => {
    await fsp.writeFile("/tmp/p.txt", "one");
    await fs.promises.appendFile("/tmp/p.txt", "two");
    is(await fsp.readFile("/tmp/p.txt", "utf8"), "onetwo");
    is((await fsp.readdir("/tmp")).includes("p.txt"), true);
    await fsp.unlink("/tmp/p.txt");
    await fsp.mkdir("/tmp/q/r", { recursive: true });
    await fsp.rm("/tmp/q", { recursive: true });
    is(fs.existsSync("/tmp/q"), false);
    await throwsCode("ENOENT", () => fsp.unlink("/tmp/p.txt"));
  },
  callbacks: async () => {
    const call = (run) => new Promise((resolve) => run((error, value) => resolve([error?.code ?? null, value])));
    is(await call((cb) => fs.writeFile("/tmp/c.txt", "cb", cb)), [null, undefined]);
    is(await call((cb) => fs.readdir("/tmp", cb)).then(([code, names]) => [code, names.includes("c.txt")]), [null, true]);
    is(await call((cb) => fs.unlink("/tmp/c.txt", cb)), [null, undefined]);
    is((await call((cb) => fs.unlink("/tmp/c.txt", cb)))[0], "ENOENT");
  },
};

// Fills the request's `/tmp` until it refuses, reports how much it took, then
// proves a removal gives the room back.
const fill = () => {
  const chunk = Buffer.alloc(8 * 1024 * 1024, 1);
  let files = 0;
  let code = null;
  try {
    for (;;) {
      fs.writeFileSync(`/tmp/fill-${files}`, chunk);
      files += 1;
    }
  } catch (error) {
    code = error.code;
  }
  fs.rmSync("/tmp/fill-0");
  fs.writeFileSync("/tmp/fill-again", chunk);
  const small = (() => {
    try {
      fs.writeFileSync("/tmp/one-more", Buffer.alloc(chunk.length * 2));
    } catch (error) {
      return error.code;
    }
  })();
  return { code, files, chunkBytes: chunk.length, refusedBig: small, exists: fs.existsSync("/tmp/one-more") };
};

export default {
  async fetch(request) {
    const path = new URL(request.url).pathname;
    switch (path) {
      case "/__ready":
        return new Response("ready");
      case "/env":
        return Response.json({
          GREETING: process.env.GREETING,
          unset: process.env.NOT_SET === undefined,
          keys: Object.keys(process.env).sort(),
          values: Object.fromEntries(Object.entries(process.env)),
        });
      case "/process":
        return Response.json(await results(processChecks));
      case "/url":
        return Response.json(await results(urlChecks));
      case "/fs":
        return Response.json(await results(fsChecks));
      case "/fill":
        return Response.json(fill());
      default:
        return new Response("not found", { status: 404 });
    }
  },
};
