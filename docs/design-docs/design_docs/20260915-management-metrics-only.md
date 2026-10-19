# Management HTTP metrics-only mode and V1 API retirement (2.6)

Related issue: [#49846](https://github.com/milvus-io/milvus/issues/49846).
Related master work: [#52580](https://github.com/milvus-io/milvus/pull/52580).
API alignment: [#53753](https://github.com/milvus-io/milvus/pull/53753).
This is a smaller exposure-reduction option for 2.6, not a backport of root
authentication or configuration projection changes.

## Problem and intended deployment

The metrics port, normally 9091, also exposes configuration, cluster diagnostic
APIs, WebUI, profiling, management operations and legacy REST operations. Some
deployments only need Prometheus scrapes and Kubernetes probes on this port.
They should be able to disable the remaining HTTP surface consistently across
Proxy, MixCoord and workers without provisioning another authentication system.

Add `common.security.managementMetricsOnly`, default `false`. Operators enable
it on every process and restart those processes. Network policy must continue to
restrict the port to trusted monitoring/probe clients: this mode does not add
authentication or TLS to metrics or probes.

```yaml
common:
  security:
    managementMetricsOnly: true
```

The environment spelling `COMMON_SECURITY_MANAGEMENTMETRICSONLY=true` is also
supported. `METRICS_PORT` can change the listener port; the policy follows that
listener rather than assuming that every deployment uses port 9091.

## Public behavior

Only GET and HEAD on these exact paths are served:

| Path | Response |
| --- | --- |
| `/metrics` | Existing Milvus Prometheus registry |
| `/metrics_default` | Existing default Prometheus registry |
| `/healthz` | Existing health HTTP status; generic status text |
| `/livez` | Existing liveness HTTP status; generic status text |
| `/management/check/ready` | Existing role readiness HTTP status; generic status text |

Probe query parameters, including `role`, retain their meaning. A probe failure
retains its HTTP failure code; component names, state lists, raw errors and
upstream response headers are discarded. HEAD has no response body. The policy
does not turn an unregistered readiness handler during startup into a healthy
response.

All other application paths and methods return 404 before dispatch, including:

- `/management/config/get`, `/management/config/alter`, `/management/stop`, and
  other management operations;
- `/webui/` and all `/api/v1/_*` console APIs;
- `/debug/pprof/*`, regardless of `proxy.http.enablePprof`;
- `/eventlog`, which therefore cannot start/discover its additional gRPC listener
  through HTTP in this mode;
- the legacy `/api/v1` tree, including `/api/v1/metrics` and `/api/v1/health`;
- implicit or downstream registrations on `http.DefaultServeMux`.

Go's HTTP server still handles protocol-level `OPTIONS *` itself with an empty
200 response; it does not dispatch an application handler or add CORS headers.

This mode does not return wildcard CORS headers. Proxy's CORS middleware is not
mounted on the restricted server. This mode does not affect
the main port. The separate `proxy.http.enableV1` setting below controls main-port
V1 registration; `/v2/vectordb` and gRPC retain their existing behavior.

When the flag is false, the management mux preserves its existing pprof/WebUI
flags, methods, response bodies and DefaultServeMux behavior. Independently, the
Proxy retires the non-underscore legacy API and makes the remaining V1 routes
optional as described below.

## API alignment with master PR #53753

Remove all 47 non-underscore `/api/v1/*` method/path pairs from the metrics port,
including `/api/v1/health`, regardless of either switch. This is a breaking
removal even with the default configuration. The underlying RPC implementations
remain available through gRPC.

Add the exported, restart-required `proxy.http.enableV1` setting, default `true`:

```yaml
proxy:
  http:
    enableV1: true
```

| Surface | `enableV1: true` or omitted | `enableV1: false` |
| --- | --- | --- |
| Non-underscore `/api/v1/*` on the metrics port | Removed | Removed |
| `/api/v1/_*` console APIs on the metrics port | Registered unless metrics-only | Not registered |
| `/v1/vector/*` on the Proxy HTTP port | Registered | Not registered |
| `/v2/vectordb/*` and HTTP listener | Existing behavior | Existing behavior |
| Metrics, probes and other management endpoints | Governed by metrics-only | Governed by metrics-only |

The 2.6 console has 27 routes; master also has telemetry routes that are not
backported here. Retained console routes keep the existing 2.6 authentication
policy: `common.security.authorizationEnabled` requires a valid user or API key.
No master root-authentication or browser challenge policy is introduced.
`managementMetricsOnly=true` takes precedence on the metrics port regardless
of `enableV1`; `proxy.http.enabled` continues to control the main HTTP listener.

Disabling V1 prevents WebUI from fetching console data. Static pages remain
controlled by `proxy.http.enableWebUI`; disable that setting too to hide them.
Unregistered paths use the existing unknown-route behavior, including any global
middleware that runs before dispatch. No redirects or V2 aliases are introduced.

Migrate old business clients to `/v2/vectordb/*` where an equivalent operation
exists, or to a gRPC SDK. V2 has different URLs and request/response schemas;
there is no claim of an exact V2 equivalent for every retired operation.
Replace `/api/v1/health` probes with `/healthz` for readiness or `/livez` for
liveness before upgrading, even if both new switches retain their defaults.
The retired `e2e-restful.sh` smoke script, private fixtures and Nightly invocation
are removed together; the separate V1/V2 REST test runner remains.

Setting `enableV1=true` and restarting restores optional V1 vector/console routes,
subject to metrics-only mode. It does not restore the retired non-underscore
routes; that requires rolling back to a version that still registers them.

## Implementation and configuration lifecycle

`managementMux` owns registration and dispatch for the management HTTP server.
It captures the mode at the first registration. Restricted mode creates a private
ServeMux and accepts only the five literal path registrations. The Proxy's `/`
Gin fallback and method/host-qualified extension patterns are not admitted.
Request method and escaped path checks run before ServeMux path cleaning and
redirects. Later registrations and requests use the same captured policy.

Probe handlers execute against a response writer that records their final HTTP
status and discards the body without buffering it. The public response is built
from the status alone. This preserves health decisions without exposing their
diagnostic explanation.

Configuration refresh cannot change an existing server's mode. Invalid boolean
values fail startup rather than silently selecting the unrestricted mode.
`/management/config/alter` always rejects SET and reset of this key, even while
the mode is off, using the configuration manager's normalized key identity.
That prevents an anonymous caller from planting an etcd override that defeats a
later file-based opt-in on restart. This applies to single and batch requests;
a rejected batch writes none of its entries.

The item is not `Immutable`: that mechanism persists its initial value into etcd,
which could pin the default false above a later operator configuration. Direct
operator edits to configuration sources retain their normal priority; operators
must check the effective value on each process before restarting.

## Compatibility and limits

- Deployments using remote WebUI, pprof, event logs, old REST APIs or management
  operations must migrate those workflows before enabling the mode. In
  particular, `/management/stop` is unavailable; shutdown should use the normal
  process/container lifecycle. Use `/healthz` or `/livez` instead of the old
  `/api/v1/health` probe.
- The change does not authenticate or redact Prometheus output. Network
  isolation remains necessary for the metrics portion of finding F.
- Eventlog's gRPC implementation is not changed. The restricted HTTP endpoint
  cannot create it; this is not a claim about an embedder that starts the logger
  directly through another interface. Restarting is required when enabling.
- Metrics-only is an opt-in reduction of the exposed surface. Retiring the old
  REST routes happens on upgrade, but does not by itself protect management APIs.
- No new root credential cache, verifier RPC, browser login flow, metric label,
  error code mapping or safe configuration projection is introduced.

## Verification

Tests use the production registration path and server handler. They cover
implicit DefaultServeMux routes, same-path host-qualified extensions, the Proxy
fallback, future registrations, path encodings, path cleanup/redirects, methods,
real Prometheus scrapes, successful and failing readiness, probe response
sanitization, and startup policy retention after configuration changes.

Configuration tests cover defaults and environment opt-in. Coordinator tests
cover normalized aliases, SET/reset, both flag values, single/batch requests and
rejecting an entire batch before persistence. The standard Go runner includes
the HTTP tests and the coordinator regression. Native coordinator compilation
and deployment network policy verification remain separate checks from the
HTTP handler tests.

V1 tests exercise the production Proxy registrar for all 47 retired and 27
retained console method/path pairs, both authorization settings, and the missing,
true and false V1 settings. A live HTTP listener test checks V1 and V2 collection
listing and the absence of all 10 V1 vector routes when disabled. Configuration
tests check the shipped YAML, missing-key default and listener independence;
management mux tests check that metrics-only remains effective under both V1
settings. Native Proxy tests require Milvus core and RocksDB libraries in CI.
