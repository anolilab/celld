# Roadmap

The work is tracked as epics. Each link opens the issue with its tasks.

- [#45 Test infrastructure and benchmarks](https://github.com/anolilab/celld/issues/45): tests and benchmarks that show the promises hold.
- [#46 Security and tenant isolation](https://github.com/anolilab/celld/issues/46): keep one application, and one tenant, out of another.
- [#47 Multi-app, multi-tenant control plane](https://github.com/anolilab/celld/issues/47): run more than one application on a fleet.
- [#48 Node.js and workerd API compatibility](https://github.com/anolilab/celld/issues/48): close the gaps in the Node.js and workerd APIs.
- [#49 Reliability and upstream defects](https://github.com/anolilab/celld/issues/49): fix the defects that lose, delay, or corrupt work.
- [#50 Fork governance and code health](https://github.com/anolilab/celld/issues/50): licence, contribution rules, and code health.
- [#61 Cloudflare binding coverage](https://github.com/anolilab/celld/issues/61): cover the Cloudflare bindings that applications use.

## Near-term order

1. CI and platforms.
2. Secure defaults: the operator token and egress control on by default.
3. Silent wrong results in crypto and fetch.
4. Reliability work, each change with its bench delta.
5. Bindings, then multi-app.

[Benchmarks](benchmarks.md) describes how celld is measured. Published results
with raw data are tracked in
[#10](https://github.com/anolilab/celld/issues/10).
[Limitations](limitations.md) lists the current gaps.
