# MEP: Retire legacy REST routes and make V1 APIs optional

- **Created:** 2026-09-22
- **Author(s):** @liliu-z
- **Status:** Under Review
- **Component:** Proxy
- **Related Issues:** #49846

## Summary

Remove the non-underscore `/api/v1/*` routes from the Proxy metrics port and
add `proxy.http.enableV1` to control registration of the remaining V1 vector
and console APIs. The switch defaults to `true`; V2, listeners, probes, and
metrics keep their existing behavior.

## Motivation

The Proxy publishes three older HTTP surfaces across two listeners. The
non-underscore `/api/v1/*` routes expose protobuf-shaped business operations on
the metrics port, alongside the underscore console APIs. The main HTTP listener
also exposes the simpler `/v1/vector/*` REST API alongside V2.

Operators need to retire the old business routes, including `/api/v1/health`,
and reduce the remaining V1 surface without disabling the listeners or V2.
This follows the management-plane authentication work.

## Public Interfaces

```yaml
proxy:
  http:
    enableV1: true
```

`proxy.http.enableV1` is exported, defaults to `true`, and requires restarting
each Proxy to apply. It controls route registration, not socket binding.

| Surface | Default port | `enableV1: true` or omitted | `enableV1: false` |
| --- | --- | --- | --- |
| Non-underscore `/api/v1/*`, including `/api/v1/health` | 9091 | Removed | Removed |
| `/api/v1/_*` console and telemetry APIs | 9091 | Registered | Not registered |
| `/v1/vector/*` | 19530 | Registered | Not registered |
| `/v2/vectordb/*` | 19530 | Registered | Registered |
| `/healthz`, `/livez`, metrics and other management routes | 9091 | Existing behavior | Existing behavior |

19530 is the default shared Proxy gRPC/HTTP port; `proxy.http.port` can select a
separate HTTP listener. The metrics port can also be configured. The existing
`proxy.http.enabled` listener switch remains independent of `enableV1`.

No proto, SDK, metric name, or stored-data format changes are part of this
design.

## Design Details

The Proxy no longer calls the legacy `Handlers.RegisterRoutesTo` registrar.
`newMetricsPortEngine` mounts only console APIs when `enableV1` is true.
`startHTTPServer` conditionally registers V1 and always registers V2. The
underlying business RPC implementations and shared HTTP utilities remain.

Console authentication keeps its existing policy: with `adminAuthEnabled=true`,
it requires root and applies the browser request checks; otherwise it follows
`authorizationEnabled`. Removing the business routes also removes their special
authentication branches and the old health-route exception. V1/V2 data-plane
authentication is unchanged.

## Compatibility, Deprecation, and Migration Plan

This deliberately removes the old non-underscore API even with default
configuration. Clients must migrate to `/v2/vectordb/*` on the Proxy HTTP port
where an equivalent operation exists, or use the gRPC SDK. V2 is not a
wire-compatible alias: URLs, methods, request schemas and response schemas
differ. This change does not claim that every old RPC projection has an exact
V2 equivalent.

Replace `/api/v1/health` probes with `/healthz` for readiness or `/livez` for
liveness on the metrics port. The retired legacy REST smoke script, its private
fixtures, and its Jenkins invocation are removed together; the separate V2
REST test runner remains.

Disabling `enableV1` also disables the APIs used by WebUI and the telemetry
pages, including client command submission. Their static pages are controlled
separately by `proxy.http.enableWebUI`; set that to `false` as well to hide
them. This flag does not stop gRPC client telemetry or remove other management
endpoints. Existing V1 vector clients continue to work with the default setting.

Unregistered paths are handled as unknown routes. Existing global middleware
can still reject a request first, for example an unauthenticated request on the
main HTTP listener. There is no redirect or automatic conversion to V2.

To restore the optional V1 vector and console routes after disabling them, set
`proxy.http.enableV1: true` and restart each Proxy. That switch cannot restore
the removed non-underscore routes; rollback of that removal requires running a
version that still registers them. Migrate their clients and probes before
upgrading.

## Test Plan

- Configuration tests cover the YAML default, the default when the key is
  absent, and explicit disabling without disabling the HTTP listener.
- The production metrics-port registrar is checked with the default, `true`,
  and `false`: all 47 former business method/path pairs stay absent; all 35
  console method/path pairs remain authenticated when enabled and absent when
  disabled.
- A live Proxy HTTP listener test checks V1 collection listing and V2 collection
  listing with the default and both explicit settings; all 10 V1 method/path
  pairs are absent when disabled.
- Console middleware tests cover both existing authorization settings, root
  versus ordinary users, browser challenges, cross-site rejection, and explicit
  non-browser requests. Existing management HTTP tests cover probes, scrapes,
  other management routes and authentication behavior.

Proxy tests require Milvus native libraries. Local validation and any dependency
limitations are recorded in the PR; these tests do not substitute for a deployed
cluster or browser acceptance test.

## Rejected Alternatives

- Keep the non-underscore routes behind `enableV1`: this would retain the
  business API that this change retires.
- Use separate switches for the console and vector V1 routes: one setting
  matches the requested operational choice to disable both surfaces together.

## References

- [Issue #49846](https://github.com/milvus-io/milvus/issues/49846)
- [Management-plane authentication MEP](20260912-management-plane-authentication.md)
