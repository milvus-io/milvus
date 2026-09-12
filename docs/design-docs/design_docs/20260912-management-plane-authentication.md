# MEP: Optional management-plane authentication

- **Created:** 2026-09-12
- **Author(s):** @liliu-z
- **Status:** Under Review
- **Component:** Proxy, Coordinator, Other
- **Related Issues:** #49846

## Summary

Add an opt-in root-authentication gate for the metrics port, independently of
main-port data authorization. Preserve probes, bound credential verification
resources, and make browser request requirements explicit.

Issue: [#49846](https://github.com/milvus-io/milvus/issues/49846)

Implementation: [#52580](https://github.com/milvus-io/milvus/pull/52580).
Configuration redaction and protected configuration projections are a separate
dependency, [#52579](https://github.com/milvus-io/milvus/pull/52579).

## Motivation

The metrics port, normally 9091, also exposes process lifecycle operations,
coordinator management, profiling, event logs, the web console, and legacy REST
data operations. These endpoints need an authentication policy independent of
the main service port's data-plane authorization policy.

`common.security.adminAuthEnabled` defaults to `false`. When enabled, operator
endpoints require HTTP Basic authentication with the cluster's root credential.
Being an ordinary RBAC user, a superuser, or an API-key holder does not grant
access to the root-only surface. The existing data-plane policy still governs
the main service port.

## Public Interfaces

| Metrics-port surface | Flag disabled | Flag enabled |
| --- | --- | --- |
| `/management/*`, except readiness | Existing behavior | Root Basic authentication |
| `/debug/pprof/*`, `/log/level`, `/eventlog` | Existing behavior | Root Basic authentication |
| `/webui/`, `/telemetry`, `/api/v1/_*` | Existing behavior | Root Basic authentication |
| Legacy REST data operations under `/api/v1` | Existing authorization policy | Existing valid-user/API-key policy if `authorizationEnabled=true`; otherwise root Basic authentication |
| `/healthz`, `/livez`, `/metrics`, `/metrics_default`, `/management/check/ready` | Open | Open |
| `/api/v1/health` | Existing authorization policy | Exempt from the new gate; existing authorization still applies |

`internal/http.Register` requires explicit `AdminAuth` marking on operator
routes and panics on an unmarked registration, even when the flag is disabled.
The guard extracts the path from Go ServeMux method- and host-qualified
patterns. It is a registration guard, not a classifier for arbitrary third-party
routers.

The proxy builds its metrics-port Gin tree through `newMetricsPortEngine`.
Its middleware classifies console routes by the `/_` prefix. The console route
enumeration test and assembled-router test must cover newly added routes.
Main-port HTTP handlers are registered separately.

Milvus routes use a private ServeMux. With authentication disabled and pprof
enabled, more-specific legacy routes on `http.DefaultServeMux` remain reachable
for compatibility. The fallback disappears while authentication is enabled.

## Design Details

### Credential ownership and request decisions

The verifier registry has one mutex-protected slot per role. Resolution uses a
fixed order: Proxy, MixCoord, then worker. This makes standalone behavior
independent of role initialization order. Each owner removes its registration
when its stop sequence completes so authenticated graceful-drain requests can
continue during shutdown.

Proxy and MixCoord fetch the stored root hash through their credential APIs.
Workers lazily construct a MixCoord client and fetch `GetCredential(root)`;
there is no management-auth client construction while the gate is unused.
Missing responses, error statuses, empty hashes, malformed stored hashes,
lookup deadlines, and construction failures are verification failures, not
wrong passwords.

| Failure | HTTP result |
| --- | --- |
| Missing or blank credentials, wrong root password | 401 |
| Non-root username, before password verification | 403 |
| Rejected browser request origin | 403 |
| Credential cannot be checked, or work is saturated | 503 |

Unauthenticated responses do not include dependency errors or stack traces.
`AuthDecision` keeps the HTTP status, JSON code, metric outcome, and verified
principal together for the HTTP and Gin adapters. A successful decision carries
root through a private typed request-context key. In-process telemetry calls
consume that verified identity without copying the password into RPC metadata.

### Browser requests and explicit client intent

The gate checks browser request metadata before checking credentials. A reported
cross-site or same-site request, an unknown `Sec-Fetch-Site`, or `Origin: null`
does not become trusted because it carries the correct password. Top-level GET
navigation is allowed only for document surfaces, which do not perform an action.

Missing browser metadata is not evidence that a caller is a non-browser client.
The [Fetch Metadata specification](https://www.w3.org/TR/fetch-metadata/)
restricts those headers to potentially trustworthy URLs; ordinary remote HTTP
is outside that guarantee. When both `Origin` and Fetch Metadata are absent,
credential-bearing calls must explicitly send `X-Milvus-Admin-Request: true`.
The header cannot override metadata that reports another site. Cross-origin
preflights must not authorize that header together with ambient credentials.

Browser console access requires HTTPS with request metadata preserved by the
reverse proxy. A document request without sufficient browser context is refused
instead of prompting the browser to cache a root credential on an insecure
origin. Non-browser management clients add the explicit header, for example:

```sh
curl --user root --header 'X-Milvus-Admin-Request: true' \
  'https://milvus-admin.example/management/querycoord/balance/status'
```

The password is prompted for rather than embedded in the command. This new
client requirement only applies while the opt-in gate is enabled. Open probes
and scrapes retain their existing policy.

### Cache and resource bounds

All roles share `CachedRootVerifier` semantics:

- A successfully fetched hash is fresh for 10 seconds. Password rotation can
  therefore take up to that freshness window to be observed by a verifier.
- If refresh fails, a previously fetched hash is usable for up to 10 minutes
  from its last successful fetch. This allows a node to be drained during a
  coordinator outage, but can also retain acceptance of a previous password
  during that outage. Once this window expires, verification fails closed.
- Lookup failures are cached for 2 seconds. Concurrent refresh requests share
  backend work, which receives a 5-second context deadline. Each caller also
  limits its own wait to 5 seconds: a service-discovery implementation that
  ignores the backend context cannot prevent timely failure or stale fallback.
- At most 32 callers attach to a shared hash lookup. Cancellation releases the
  HTTP caller promptly but retains its accounting slot until the shared result
  is released. This also bounds singleflight result-channel retention. If the
  backend remains blocked despite its deadline, its retained slots stay occupied;
  additional callers are shed rather than spawning replacement backend work.
- Worker client construction has one shared completion signal and result per
  in-flight construction. A constructor that ignores cancellation can remain
  blocked, but repeated timeout windows must not retain another channel for
  each request. A completed construction publishes its result to all waiters;
  a later attempt can retry a failed construction.
- bcrypt comparisons admit a quarter of `GOMAXPROCS`, floored at two, with a
  bounded queue and a 500 ms maximum queue wait. This bounds concurrency rather
  than attempt rate. On a small pod it can still consume substantial CPU.
- The last successfully verified password has a SHA cache tied to the exact
  stored hash. A changed hash invalidates that cache. Passwords over bcrypt's
  72-byte limit are rejected before lookup or comparison.

`Forget` advances a generation so an old in-flight fetch cannot repopulate a
discarded cache. Worker `Close` cancels its lifetime context, drops its cache,
and closes its client. A construction completing after close must release the
new client instead of publishing it.

### Configuration and event logs

The gate is refreshable and is read for each request. It is deliberately not an
immutable configuration item: persisting its default in etcd would outrank a
later YAML setting. `/management/config/alter` always rejects this key, including
while the gate is off, using `config.FormatKey` to recognize equivalent spellings.
That prevents an anonymous caller from planting a disabling value before an
operator enables the feature. The normalization memo is bounded to 4096 entries
and only caches keys whose input and normalized forms are at most 1024 bytes.
Cached strings own their storage so even a short key cannot retain the larger
request buffer it came from. Larger keys still normalize correctly without
retaining their strings. This bounds memo retention, not the size of an
individual incoming request body.

The HTTP `/eventlog` endpoint only discovers an otherwise unauthenticated gRPC
stream. With the flag enabled that stream binds to loopback, including when an
existing listener is switched. A failed security upgrade closes the old wildcard
listener. Event-log shutdown closes a separate notification channel and stops
the gRPC server so recording concurrently cannot send on a closed event queue.

## Compatibility, Deprecation, and Migration Plan

`milvus_admin_auth_total{endpoint,result}` records management-auth and cross-site
decisions. `endpoint` is a registered route pattern, never an arbitrary request
path. The ordinary data-plane credential verifier does not increment this metric.
Failure logs are rate limited and bound caller-controlled fields.

Enabling the flag requires updating graceful-shutdown, management, and profiling
clients. The port itself remains plaintext HTTP. Keep it on a trusted network or
behind a TLS-terminating proxy and rotate the root password with
`UpdateCredential` or `db.update_password` first. Changing
`common.security.defaultRootPassword` does not rotate an existing credential.
Official compose deployments and embedded standalone scripts publish 9091;
operators must review that exposure before enabling browser authentication.

## Test Plan

Unit tests exercise authentication decisions, real HTTP/Gin route assembly,
credential response validation, stale and failed refreshes, saturation,
cancellation, password rotation, role priority, event-log listener switching,
and configuration aliases. These tests do not replace a real coordinator
failover or browser console acceptance test. Native-linked packages require
Milvus C++ dependencies and must also compile and run in CI.

## Rejected Alternatives

- Reusing `authorizationEnabled` would couple administration to the data plane
  and admit ordinary users to operations that can stop components.
- A last-registration-wins verifier would make standalone ownership depend on
  goroutine scheduling. Explicit role priority avoids that ambiguity.
- Gating only event-log HTTP discovery would leave its unauthenticated gRPC
  stream reachable through a separately discoverable port.
- Treating missing browser metadata as a non-browser request would leave
  ordinary remote HTTP outside the origin check. Referer alone is insufficient
  because it can be suppressed; an explicit client header closes that case.
- A semaphore or singleflight alone bounds work but not retained waiters. Both
  backend work and caller retention need separate bounds.

## References

- [Management-plane authentication issue #49846](https://github.com/milvus-io/milvus/issues/49846)
- [Authentication implementation #52580](https://github.com/milvus-io/milvus/pull/52580)
- [Safe configuration projections #52579](https://github.com/milvus-io/milvus/pull/52579)
- [Fetch Metadata request headers](https://www.w3.org/TR/fetch-metadata/)
