// node:url for celld.
//
// `URL` and `URLSearchParams` are the globals. `fileURLToPath` and
// `pathToFileURL` follow Node's POSIX behaviour, including its error codes.
// The legacy `parse` and string `format` are not implemented and fail on use.
//
// Injected lazily, so an isolate that never imports node:url pays nothing.
(() => {
  "use strict";

  const nodeError = (Type, code, message) => {
    const error = new Type(message);
    error.code = code;
    return error;
  };
  const describe = (value) => {
    if (value == null) return ` Received ${value}`;
    if (typeof value === "function") {
      return ` Received function ${value.name}`;
    }
    if (typeof value === "object") {
      return value.constructor?.name
        ? ` Received an instance of ${value.constructor.name}`
        : " Received an object";
    }
    let shown = String(value);
    if (typeof value === "string") {
      if (shown.length > 28) shown = `${shown.slice(0, 25)}...`;
      shown = `'${shown}'`;
    }
    return ` Received type ${typeof value} (${shown})`;
  };
  const invalidType = (name, value) =>
    nodeError(
      TypeError,
      "ERR_INVALID_ARG_TYPE",
      `The "${name}" argument must be of type string or an instance of URL.` +
        describe(value),
    );
  const notImplemented = (name) => () => {
    throw nodeError(
      Error,
      "ERR_METHOD_NOT_IMPLEMENTED",
      `node:url ${name} is not implemented in celld`,
    );
  };

  const isURL = (value) =>
    value != null && typeof value === "object" &&
    typeof value.href === "string" && typeof value.protocol === "string";

  const fileURLToPath = (path) => {
    if (typeof path === "string") path = new URL(path);
    else if (!isURL(path)) throw invalidType("path", path);
    if (path.protocol !== "file:") {
      throw nodeError(
        TypeError,
        "ERR_INVALID_URL_SCHEME",
        "The URL must be of scheme file",
      );
    }
    if (path.hostname !== "" && path.hostname !== "localhost") {
      throw nodeError(
        TypeError,
        "ERR_INVALID_FILE_URL_HOST",
        'File URL host must be "localhost" or empty on linux',
      );
    }
    const pathname = path.pathname;
    for (let i = 0; i < pathname.length - 2; i++) {
      if (pathname[i] === "%") {
        const third = pathname.charCodeAt(i + 2) | 0x20;
        if (pathname[i + 1] === "2" && third === 102) {
          throw nodeError(
            TypeError,
            "ERR_INVALID_FILE_URL_PATH",
            "File URL path must not include encoded / characters",
          );
        }
      }
    }
    return decodeURIComponent(pathname);
  };

  // The bytes the path of a `file:` URL must not carry raw. `%` and `\` are
  // data in a POSIX path, so they are escaped here before the parser sees
  // them; `?` and `#` would otherwise start the query and the fragment.
  const ESCAPED = /[\0-\x1f\x7f "#%<>?\\`{}]|[^\x00-\x7f]/gu;
  const escapePath = (path) =>
    path.replace(ESCAPED, (char) => {
      const bytes = new TextEncoder().encode(char);
      let out = "";
      for (const byte of bytes) {
        out += `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
      }
      return out;
    });

  const resolve = (path) => {
    const absolute = path.startsWith("/") ? path : `${process.cwd()}/${path}`;
    const parts = [];
    for (const part of absolute.split("/")) {
      if (part === "" || part === ".") continue;
      if (part === "..") parts.pop();
      else parts.push(part);
    }
    return `/${parts.join("/")}`;
  };

  const pathToFileURL = (filepath) => {
    if (typeof filepath !== "string") {
      throw nodeError(
        TypeError,
        "ERR_INVALID_ARG_TYPE",
        'The "path" argument must be of type string.' + describe(filepath),
      );
    }
    let resolved = resolve(filepath);
    // `resolve` drops a trailing slash, which names a directory.
    if (filepath.endsWith("/") && !resolved.endsWith("/")) resolved += "/";
    return new URL(`file://${escapePath(resolved)}`);
  };

  // Legacy objects: `{ protocol, auth, host | hostname, port, pathname,
  // search | query, hash }`, which is what the legacy `format` accepts.
  const formatLegacy = (object) => {
    let protocol = object.protocol || "";
    if (protocol && !protocol.endsWith(":")) protocol += ":";
    let host = "";
    if (object.host) host = object.host;
    else if (object.hostname) {
      host = object.hostname.includes(":") && !object.hostname.startsWith("[")
        ? `[${object.hostname}]`
        : object.hostname;
      if (object.port) host += `:${object.port}`;
    }
    const auth = object.auth
      ? `${encodeURIComponent(object.auth).replace(/%3A/i, ":")}@`
      : "";
    let pathname = object.pathname || "";
    let search = object.search || "";
    if (!search && object.query && typeof object.query === "object") {
      const query = new URLSearchParams(object.query).toString();
      if (query) search = `?${query}`;
    } else if (!search && typeof object.query === "string" && object.query) {
      search = `?${object.query}`;
    }
    if (search && !search.startsWith("?")) search = `?${search}`;
    let hash = object.hash || "";
    if (hash && !hash.startsWith("#")) hash = `#${hash}`;
    const slashed = object.slashes ||
      /^(?:https?|ftp|gopher|file|wss?):$/.test(protocol);
    if (host || slashed) {
      if (host || protocol === "file:") host = `//${auth}${host}`;
      if (pathname && !pathname.startsWith("/")) pathname = `/${pathname}`;
    }
    pathname = pathname.replace(/[?#]/g, encodeURIComponent);
    search = search.replace("#", "%23");
    return protocol + host + pathname + search + hash;
  };

  const format = (value, options) => {
    if (isURL(value) && typeof value.searchParams === "object") {
      if (options !== undefined && (options === null ||
          typeof options !== "object")) {
        throw nodeError(
          TypeError,
          "ERR_INVALID_ARG_TYPE",
          'The "options" argument must be of type object.' + describe(options),
        );
      }
      const fragment = options?.fragment ?? true;
      const search = options?.search ?? true;
      const auth = options?.auth ?? true;
      const url = new URL(value.href);
      if (!fragment) url.hash = "";
      if (!search) url.search = "";
      if (!auth) {
        url.username = "";
        url.password = "";
      }
      return url.href;
    }
    if (typeof value === "string") return notImplemented("format(string)")();
    if (value === null || typeof value !== "object") {
      throw nodeError(
        TypeError,
        "ERR_INVALID_ARG_TYPE",
        'The "urlObject" argument must be of type object or string.' +
          describe(value),
      );
    }
    return formatLegacy(value);
  };

  __celld.__urlModule = {
    URL: globalThis.URL,
    URLSearchParams: globalThis.URLSearchParams,
    fileURLToPath,
    pathToFileURL,
    format,
    parse: notImplemented("parse"),
  };
})();
