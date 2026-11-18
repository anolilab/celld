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

const throwsCodeSync = (code, run) => {
  try {
    run();
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
  invalidUrlString: () => throwsCode("ERR_INVALID_URL", () => fileURLToPath("not a url")),
  notAUrl: () => throwsCode("ERR_INVALID_ARG_TYPE", () => fileURLToPath(5)),
  pathNotAString: () => throwsCode("ERR_INVALID_ARG_TYPE", () => pathToFileURL(5)),
  formatUrl: () =>
    is(
      url.format(new URL("http://u:p@a.com/x?y#z"), { auth: false, fragment: false, search: false }),
      "http://a.com/x",
    ),
  fakeUrlObject: () =>
    throwsCode("ERR_INVALID_ARG_TYPE", () =>
      fileURLToPath({ href: "file:///etc/passwd", protocol: "file:", hostname: "", pathname: "/etc/passwd" })),
  formatUnicode: () => {
    const host = new URL("http://u:p@xn--bcher-kva.example:8080/p?q#h");
    is(url.format(host, { unicode: true }), "http://u:p@bücher.example:8080/p?q#h");
    is(url.format(host), "http://u:p@xn--bcher-kva.example:8080/p?q#h");
    is(url.format(new URL("http://xn--mnchen-3ya.de/"), { unicode: true }), "http://münchen.de/");
    is(url.format(new URL("http://a.com/x"), { unicode: true }), "http://a.com/x");
  },
  formatLegacyObject: () =>
    throwsCode("ERR_METHOD_NOT_IMPLEMENTED", () => url.format({ protocol: "http", host: "a.com", query: { a: 1 } })),
  legacyNamesStayExported: async () => {
    for (const name of ["resolve", "resolveObject", "domainToASCII", "domainToUnicode", "urlToHttpOptions", "Url"]) {
      is(typeof url[name], "function");
    }
    await throwsCode("ERR_METHOD_NOT_IMPLEMENTED", () => url.domainToASCII("a.com"));
    await throwsCode("ERR_METHOD_NOT_IMPLEMENTED", () => url.urlToHttpOptions(new URL("http://a/")));
    await throwsCode("ERR_METHOD_NOT_IMPLEMENTED", () => new url.Url());
  },
  parseFailsLoudly: () => throwsCode("ERR_METHOD_NOT_IMPLEMENTED", () => url.parse("http://a/")),
};

// What module scope sees, before any request: `process.env` is filled, and
// there is no request, so no `/tmp` to write to.
const atModuleScope = (() => {
  let tmp;
  try {
    fs.writeFileSync("/tmp/module.txt", "from module scope");
    tmp = "written";
  } catch (error) {
    tmp = error.code;
  }
  return { greeting: process.env.GREETING, tmp };
})();

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
  writeFlags: () => {
    fs.writeFileSync("/tmp/f.txt", "abcdef");
    throwsCodeSync("EBADF", () => fs.writeFileSync("/tmp/f.txt", "x", { flag: "r" }));
    is(fs.readFileSync("/tmp/f.txt", "utf8"), "abcdef");
    throwsCodeSync("ENOENT", () => fs.writeFileSync("/tmp/f-missing.txt", "x", { flag: "r" }));
    fs.writeFileSync("/tmp/f.txt", "XY", { flag: "r+" });
    is(fs.readFileSync("/tmp/f.txt", "utf8"), "XYcdef");
    fs.writeFileSync("/tmp/f.txt", "1234567", { flag: "r+" });
    is(fs.readFileSync("/tmp/f.txt", "utf8"), "1234567");
    throwsCodeSync("ENOENT", () => fs.writeFileSync("/tmp/f-missing.txt", "x", { flag: "r+" }));
    fs.writeFileSync("/tmp/f.txt", "+", { flag: "a+" });
    is(fs.readFileSync("/tmp/f.txt", "utf8"), "1234567+");
    fs.writeFileSync("/tmp/f.txt", "w", { flag: "w+" });
    is(fs.readFileSync("/tmp/f.txt", "utf8"), "w");
    throwsCodeSync("EEXIST", () => fs.writeFileSync("/tmp/f.txt", "x", { flag: "ax" }));
    fs.writeFileSync("/tmp/f-new.txt", "n", { flag: "ax" });
    fs.appendFileSync("/tmp/f.txt", "z", { flag: "w" });
    is(fs.readFileSync("/tmp/f.txt", "utf8"), "z");
    for (const flag of ["rs", "z", "", 3]) {
      throwsCodeSync("ERR_INVALID_ARG_VALUE", () => fs.writeFileSync("/tmp/f.txt", "x", { flag }));
    }
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
  // A bad flag is an argument error: it throws from the call, or rejects the
  // promise, and the callback is never told.
  badWriteFlags: async () => {
    let called = false;
    const cb = () => { called = true; };
    for (const flag of ["rs", "z", ""]) {
      throwsCodeSync("ERR_INVALID_ARG_VALUE", () => fs.writeFile("/tmp/c.txt", "x", { flag }, cb));
      throwsCodeSync("ERR_INVALID_ARG_VALUE", () => fs.appendFile("/tmp/c.txt", "x", { flag }, cb));
      await throwsCode("ERR_INVALID_ARG_VALUE", () => fsp.writeFile("/tmp/c.txt", "x", { flag }));
      await throwsCode("ERR_INVALID_ARG_VALUE", () => fsp.appendFile("/tmp/c.txt", "x", { flag }));
    }
    await new Promise((resolve) => setTimeout(resolve, 5));
    is(called, false);
  },
  readdirBuffer: async () => {
    fs.mkdirSync("/tmp/bn/sub", { recursive: true });
    fs.writeFileSync("/tmp/bn/sub/f", "1");
    const text = (names) => names.map((name) => [Buffer.isBuffer(name), name.toString()]);
    is(text(fs.readdirSync("/tmp/bn", { encoding: "buffer" })), [[true, "sub"]]);
    is(text(fs.readdirSync("/tmp/bn", "buffer")), [[true, "sub"]]);
    is(text(fs.readdirSync("/tmp/bn", { encoding: "buffer", recursive: true })), [[true, "sub"], [true, "sub/f"]]);
    const call = (run) => new Promise((resolve) => run((error, value) => resolve(value)));
    is(text(await call((cb) => fs.readdir("/tmp/bn", { encoding: "buffer" }, cb))), [[true, "sub"]]);
    is(text(await fsp.readdir("/tmp/bn", { encoding: "buffer" })), [[true, "sub"]]);
    is(fs.readdirSync("/tmp/bn"), ["sub"]);
  },
  readdirManyEntries: () => {
    fs.mkdirSync("/tmp/many");
    for (let index = 0; index < 150000; index++) fs.mkdirSync(`/tmp/many/${index}`);
    is(fs.readdirSync("/tmp/many", { recursive: true }).length, 150000);
    is(fs.readdirSync("/tmp", { recursive: true }).filter((name) => name.startsWith("many/")).length, 150000);
    fs.rmSync("/tmp/many", { recursive: true });
  },
  readdirRootAndBundle: () => {
    is(fs.readdirSync("/"), ["bundle", "dev", "tmp"]);
    for (const name of fs.readdirSync("/")) is(fs.statSync(`/${name}`).isDirectory(), true);
    is(fs.readdirSync("/dev"), []);
    is(fs.readdirSync("/bundle").includes("worker.js"), true);
    is(fs.statSync("/bundle/worker.js").isFile(), true);
    is(fs.readdirSync("/", { withFileTypes: true }).map((entry) => entry.isDirectory()), [true, true, true]);
    is(fs.readdirSync("/", { recursive: true }).includes("bundle/worker.js"), true);
    throwsCodeSync("ENOTDIR", () => fs.readdirSync("/bundle/worker.js"));
    throwsCodeSync("ENOENT", () => fs.readdirSync("/bundle/missing"));
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

// Module scope is shared by every request of the isolate, so one request's
// continuation runs inside the checkpoint of another.
const isolate = crypto.randomUUID().slice(0, 8);
let parked = false;
let release;
const gate = new Promise((resolve) => {
  release = resolve;
});
// A reaction left behind by a request that has already answered.
let fireLeft;
const leftGate = new Promise((resolve) => {
  fireLeft = resolve;
});
let leftOutcome = "did not run";

const concurrent = {
  // Waits on the gate, then reads and writes its own `/tmp`.
  async park() {
    fs.writeFileSync("/tmp/own", "mine");
    parked = true;
    await gate;
    return { isolate, own: fs.existsSync("/tmp/own"), foreign: fs.existsSync("/tmp/planted") };
  },
  // Plants a file, opens the gate while this request is the one running.
  async release() {
    fs.writeFileSync("/tmp/planted", "from the running request");
    release();
    await new Promise((resolve) => setTimeout(resolve, 100));
    return { isolate };
  },
  arm() {
    leftGate.then(() => {
      let write;
      try {
        fs.writeFileSync("/tmp/left", "x");
        write = "written";
      } catch (error) {
        write = error.code;
      }
      leftOutcome = { write, sees: fs.existsSync("/tmp/planted") };
    });
    return { armed: true };
  },
  async fire() {
    fs.writeFileSync("/tmp/planted", "from the running request");
    fireLeft();
    await new Promise((resolve) => setTimeout(resolve, 100));
    return leftOutcome;
  },
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
      case "/module":
        return Response.json({
          ...atModuleScope,
          visibleToRequest: fs.existsSync("/tmp/module.txt"),
          listed: fs.readdirSync("/tmp"),
        });
      case "/process":
        return Response.json(await results(processChecks));
      case "/url":
        return Response.json(await results(urlChecks));
      case "/fs":
        return Response.json(await results(fsChecks));
      case "/stream": {
        // The body is pulled after the handler has returned, in the turn's
        // closing checkpoints. It must still see the request's `/tmp`.
        fs.writeFileSync("/tmp/out", "written by the handler");
        const encoder = new TextEncoder();
        return new Response(
          new ReadableStream({
            pull(controller) {
              let text;
              try {
                fs.writeFileSync("/tmp/from-pull", "x");
                text = fs.readFileSync("/tmp/out", "utf8") + "|" + fs.readFileSync("/tmp/from-pull", "utf8");
              } catch (error) {
                text = error.code;
              }
              controller.enqueue(encoder.encode(text));
              controller.close();
            },
          }),
        );
      }
      case "/concurrent/park":
        return Response.json(await concurrent.park());
      case "/concurrent/parked":
        return new Response(String(parked));
      case "/concurrent/release":
        // Releasing a module that parked nothing is a misroute: say so at once.
        if (!parked) return new Response("nothing is parked in this isolate", { status: 404 });
        return Response.json(await concurrent.release());
      case "/concurrent/arm":
        return Response.json(concurrent.arm());
      case "/concurrent/fire":
        return Response.json(await concurrent.fire());
      case "/fill":
        return Response.json(fill());
      default:
        return new Response("not found", { status: 404 });
    }
  },
};
