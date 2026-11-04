# Ownership-refactor verification and adversarial review

## Execution ownership

`Server.MultiSearch` delegates to `s.proxy.MultiSearch`. `Proxy.MultiSearch` owns
validation, shared admission channels, dispatcher goroutines and ordered outcomes.
The transport installs the ordinary Search entry callback once; its terminal
handler calls `component.Search` directly. No MultiSearchExecutor type remains.
No Search/HybridSearch task, DQL scheduler, QueryNode, protocol or SDK implementation
was changed by this refactor.

## G1: construction and projection of relevant values

- The new uninitialized-entry branch originates existing `merr.ErrServiceNotReady`
  (code 1, retriable SystemError). `merr.Wrap` adds context without changing the
  code. `merr.Status` projects it into the envelope response; the unit test asserts
  code 1 and Retriable=true. No sentinel or wire mapping was changed.
- For children, the existing `GetRequestInfo` reads database/collection scope and
  SearchRequest.Nq. `RateLimitInterceptorWithMetaCache` calls Limiter.Check, then
  GetFailedResponse creates a SearchResults from the typed quota error. The
  callback and Proxy.MultiSearch preserve that SearchResults. The gRPC test uses
  the real request-info path with a metadata fixture and injected limiter; it
  verifies code 8, Retriable=true, exact token counts (3+7, then 5), and no leaf
  execution for the rejected item. No quota mapper was stubbed.
- Both transport and core coordination check Go errors and SearchResults status.
  Collection/query errors already expressed as SearchResults remain item-local.
  gRPC errors become the item's grpc_code/message, as in the preceding prototype.
- `UnaryServerInfo.Server` now identifies the Proxy component. Repository search
  found its relevant consumer in the authentication override check. Neither the
  concrete Server nor Proxy implements AuthFuncOverride, so the ordinary
  authentication function remains selected for this implementation.
- Each callback still forks RPCStats and the shared access-log interceptor creates
  an item-local access-info object. The statistics race test passes. No metric
  label, status projection, old-code mapping or logging implementation changed.

## G2: failure paths exercised

| Failure | Evidence and result |
| --- | --- |
| Entry callback not installed | Core test: retriable ServiceNotReady; no unchecked Search fallback. |
| Duplicate initialization / concurrent publication | Core tests: callback and permit channels do not reset; race detector passes. |
| Shared admission exhausted | Core test: retriable TooManyRequests; two batches share the child cap. |
| Child quota rejection | Real gRPC test: rejected child retains status, successful sibling returns, exact-once charging. Limiter refusal is injected; live dynamic quota updates were not tested. |
| Missing/invalid authentication | Native standalone and both K8s proxies reject envelope; gRPC test confirms no quota/leaf work. |
| Cross-database RBAC | Native checks: db_a search succeeds and same-name collection in db_b is denied with gRPC code 7. |
| Missing database / invalid vector dimension | Native mixed batch preserves successful siblings. Observed missing-db code 800; invalid-dimension result carries existing code 2000 and nested segcoreCode=2001 in the reason. This refactor does not repair or reinterpret existing upstream error mappings. |
| Malformed/oversized envelope | Native and core checks: duplicate IDs and 129 items return 1100; oversized request returns 1102. No children dispatched on core validation failures. |
| Response budget / invalid leaf response | Core tests exercise aggregate response failure and internal-error outcome. |
| Client cancellation/deadline | Core tests stop dispatch, cancel permit waits and release admission. Native tests observe DEADLINE_EXCEEDED then successful recovery. |
| Proxy component context cancellation | New core test cancels an active child, releases both permit types and rejects subsequent dispatch. Proxy.Stop calls node.cancel; this test exercises that context signal, not the duration/order of the entire shutdown procedure. |

## G3/G4: remaining limits

The gRPC test uses a real Proxy coordinator and real entry chain, with a test leaf
and metadata/limiter fixture. Native checks exercise actual search tasks and
QueryNodes. The complete repository Go suite was not run. Legacy-cluster upgrade
forwarding, private Cloud hooks/billing, REST/async SDK, full error details and
resumable partial results remain unverified or unimplemented. Atomic callback
publication prevents initialization races; it does not enable live configuration
replacement. Cancellation bounds active Search calls, not all backend cleanup.

The fresh four-run CPU comparison uses the new binary and both API variants.
Historical stock and mixed-load evidence remains labeled with its older binary;
no matched before/after refactor speedup is claimed.
