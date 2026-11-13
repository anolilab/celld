// Regenerates the vectors block in index.js with Node's WebCrypto as the
// reference implementation: `node gen-vectors.mjs`. Keys are generated
// fresh, so every run changes the file; the outputs are recomputed with
// them. Deterministic outputs (digests, MACs, KDFs, AES, PKCS#1 v1.5,
// Ed25519, ECDH, X25519) are compared exactly; randomized ones (RSA-PSS,
// RSA-OAEP, ECDSA) are produced here and must verify or decrypt there.
const { subtle } = globalThis.crypto;
const hex = (buffer) => Buffer.from(buffer).toString("hex");
const bytes = (text) => new TextEncoder().encode(text);
const message = bytes("celld webcrypto vector");

const out = {};
const pair = async (algorithm, usages) => {
  const keys = await subtle.generateKey(algorithm, true, usages);
  return {
    pkcs8: hex(await subtle.exportKey("pkcs8", keys.privateKey)),
    spki: hex(await subtle.exportKey("spki", keys.publicKey)),
    keys,
  };
};
const strip = ({ keys, ...rest }) => rest;

for (const hash of ["SHA-1", "SHA-256", "SHA-384", "SHA-512"]) {
  out[`digest ${hash}`] = hex(await subtle.digest(hash, bytes("abc")));
}

const hmacKey = await subtle.importKey("raw", bytes("Jefe"), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
out.hmac = hex(await subtle.sign("HMAC", hmacKey, bytes("what do ya want for nothing?")));

const pbkdf2 = await subtle.importKey("raw", bytes("passwd"), "PBKDF2", false, ["deriveBits"]);
out.pbkdf2 = hex(await subtle.deriveBits({ name: "PBKDF2", hash: "SHA-256", salt: bytes("salt"), iterations: 1 }, pbkdf2, 512));

const hkdf = await subtle.importKey("raw", new Uint8Array(22).fill(0x0b), "HKDF", false, ["deriveBits"]);
out.hkdf = hex(await subtle.deriveBits({
  name: "HKDF", hash: "SHA-256",
  salt: Uint8Array.from({ length: 13 }, (_, i) => i),
  info: Uint8Array.from({ length: 10 }, (_, i) => 0xf0 + i),
}, hkdf, 336));

const aesKey = Uint8Array.from({ length: 16 }, (_, i) => i);
const iv = Uint8Array.from({ length: 16 }, (_, i) => 0xa0 + i);
const aes = async (name, params) => {
  const key = await subtle.importKey("raw", aesKey, name, false, ["encrypt"]);
  return hex(await subtle.encrypt({ name, ...params }, key, message));
};
out["AES-GCM"] = await aes("AES-GCM", { iv: iv.slice(0, 12), additionalData: bytes("aad") });
out["AES-CBC"] = await aes("AES-CBC", { iv });
out["AES-CTR"] = await aes("AES-CTR", { counter: iv, length: 64 });

const pkcs1 = await pair({ name: "RSASSA-PKCS1-v1_5", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" }, ["sign", "verify"]);
out["RSASSA-PKCS1-v1_5"] = { ...strip(pkcs1), signature: hex(await subtle.sign("RSASSA-PKCS1-v1_5", pkcs1.keys.privateKey, message)) };

const pss = await pair({ name: "RSA-PSS", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" }, ["sign", "verify"]);
out["RSA-PSS"] = { ...strip(pss), signature: hex(await subtle.sign({ name: "RSA-PSS", saltLength: 32 }, pss.keys.privateKey, message)) };

const oaep = await pair({ name: "RSA-OAEP", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" }, ["encrypt", "decrypt"]);
out["RSA-OAEP"] = { ...strip(oaep), ciphertext: hex(await subtle.encrypt({ name: "RSA-OAEP", label: bytes("label") }, oaep.keys.publicKey, message)) };

for (const [curve, hash] of [["P-256", "SHA-256"], ["P-384", "SHA-384"], ["P-521", "SHA-512"]]) {
  const ecdsa = await pair({ name: "ECDSA", namedCurve: curve }, ["sign", "verify"]);
  out[`ECDSA ${curve}`] = { ...strip(ecdsa), hash, signature: hex(await subtle.sign({ name: "ECDSA", hash }, ecdsa.keys.privateKey, message)) };
  const a = await pair({ name: "ECDH", namedCurve: curve }, ["deriveBits"]);
  const b = await pair({ name: "ECDH", namedCurve: curve }, ["deriveBits"]);
  const length = { "P-256": 256, "P-384": 384, "P-521": 528 }[curve];
  out[`ECDH ${curve}`] = { pkcs8: a.pkcs8, peer: b.spki, length, shared: hex(await subtle.deriveBits({ name: "ECDH", public: b.keys.publicKey }, a.keys.privateKey, length)) };
}

// A hash shorter or longer than the curve's field, both ways round.
for (const [curve, hash] of [["P-521", "SHA-256"], ["P-384", "SHA-256"], ["P-256", "SHA-512"]]) {
  const ecdsa = await pair({ name: "ECDSA", namedCurve: curve }, ["sign", "verify"]);
  out[`ECDSA ${curve} ${hash}`] = { ...strip(ecdsa), curve, hash, signature: hex(await subtle.sign({ name: "ECDSA", hash }, ecdsa.keys.privateKey, message)) };
}

// Every hash the other algorithms take, so each dispatch arm is pinned.
for (const hash of ["SHA-1", "SHA-384", "SHA-512"]) {
  const key = await subtle.importKey("raw", bytes("Jefe"), { name: "HMAC", hash }, false, ["sign"]);
  out[`hmac ${hash}`] = hex(await subtle.sign("HMAC", key, message));
  const p = await subtle.importKey("raw", bytes("passwd"), "PBKDF2", false, ["deriveBits"]);
  out[`pbkdf2 ${hash}`] = hex(await subtle.deriveBits({ name: "PBKDF2", hash, salt: bytes("salt"), iterations: 2 }, p, 256));
  const h = await subtle.importKey("raw", bytes("ikm"), "HKDF", false, ["deriveBits"]);
  out[`hkdf ${hash}`] = hex(await subtle.deriveBits({ name: "HKDF", hash, salt: bytes("salt"), info: bytes("info") }, h, 256));
}
for (const hash of ["SHA-1", "SHA-384", "SHA-512"]) {
  const o = await pair({ name: "RSA-OAEP", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash }, ["encrypt", "decrypt"]);
  out[`RSA-OAEP ${hash}`] = { pkcs8: o.pkcs8, hash, ciphertext: hex(await subtle.encrypt({ name: "RSA-OAEP" }, o.keys.publicKey, message)) };
}
const aes256 = Uint8Array.from({ length: 32 }, (_, i) => i);
const aesWith = async (name, raw, params) => {
  const key = await subtle.importKey("raw", raw, name, false, ["encrypt"]);
  return hex(await subtle.encrypt({ name, ...params }, key, message));
};
out["AES-256-GCM"] = await aesWith("AES-GCM", aes256, { iv: iv.slice(0, 12) });
out["AES-256-CBC"] = await aesWith("AES-CBC", aes256, { iv });
out["AES-256-CTR"] = await aesWith("AES-CTR", aes256, { counter: iv, length: 64 });
for (const tagLength of [96, 112]) {
  out[`AES-GCM tag ${tagLength}`] = await aesWith("AES-GCM", aesKey, { iv: iv.slice(0, 12), tagLength });
}
// The private JWK of the PKCS#1 v1.5 key above, member by member.
out["RSA private JWK"] = await subtle.exportKey("jwk", await subtle.importKey("pkcs8", Buffer.from(out["RSASSA-PKCS1-v1_5"].pkcs8, "hex"), { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, true, ["sign"]));

const ed = await pair({ name: "Ed25519" }, ["sign", "verify"]);
out.Ed25519 = { ...strip(ed), signature: hex(await subtle.sign("Ed25519", ed.keys.privateKey, message)) };

const xa = await pair({ name: "X25519" }, ["deriveBits"]);
const xb = await pair({ name: "X25519" }, ["deriveBits"]);
out.X25519 = { pkcs8: xa.pkcs8, peer: xb.spki, shared: hex(await subtle.deriveBits({ name: "X25519", public: xb.keys.publicKey }, xa.keys.privateKey, 256)) };

// The fixture is one file because a `no_bundle` deploy ships only its main
// module, so the vectors live in index.js between the markers.
import { readFileSync, writeFileSync } from "node:fs";
const path = new URL("./index.js", import.meta.url);
const source = readFileSync(path, "utf8");
const begin = "// BEGIN VECTORS\n";
const end = "// END VECTORS\n";
const block = `${begin}// Generated by gen-vectors.mjs from Node ${process.version}'s WebCrypto. Do not edit.\nconst V = ${JSON.stringify(out, null, 2)};\n${end}`;
writeFileSync(path, source.slice(0, source.indexOf(begin)) + block + source.slice(source.indexOf(end) + end.length));
