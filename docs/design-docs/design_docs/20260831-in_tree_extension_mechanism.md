# MEP: In-Tree Extension Mechanism for Distributions

- **Created:** 2026-08-31
- **Author(s):** @xiaocai2333
- **Status:** In progress
- **Component:** pkg/extension | cmd/milvus | paramtable | Proxy | Coordinator
- **Related Issues:** #52979
- **Implemented by:** #52981 (mechanism), seams follow per package

## Summary

`pkg/extension` lets a distribution that is **compiled into the milvus binary**
take over a fixed set of behaviors - proxy-side request interception, API-key
verification, RBAC seeding, DDL admission, a coordinator-hosted control-plane
engine, resource-group interception, graceful vector-index drop, per-resource-
group load placement and unauthenticated internal listeners - without forking
the code that hosts them. A distribution links its implementation, installs one
`Provider` at boot, and builds its own `main` around the exported
`cmd/milvus.Main`. A stock binary installs nothing and behaves exactly as
before.

This document is the contract: the shape of the table, the rules under which
it evolves, the conventions every capability follows, and which seams have
landed.

## Motivation

The managed-cloud form of milvus had been a fork carrying ~9 behavior changes
in the proxy and the coordinators. Every rebase re-applied them by hand, and
every change to a hosting function risked silently dropping one. The existing
plug-in point, `internal/util/hookutil`, loads a `.so` and covers request
observation and API-key verification only; it cannot reach coordinator
internals, listeners or load semantics, and its untyped `interface{}` surface
cannot express what those need.

The alternative to a fork is a set of typed seams in the tree, consulted at the
points the fork used to patch, with the implementation living out of tree.
Nothing in a community build should pay for them: no allocation, no lock, one
atomic load and one nil comparison on a request path.

## Public interfaces

### The table

```go
type Capabilities struct {
    ProxyExt          ProxyExtension
    APIKey            APIKeyVerifier
    RBACBootstrap     RBACBootstrapper
    Admission         AdmissionChecker
    CoordinatorEngine CoordinatorEngine
    ResourceGroups    ResourceGroupInterceptor
    IndexDrain        IndexDrainer
    LoadPlacement     LoadPlacementScope
    InternalSurfaces  InternalSurfaces
}

type Provider interface {
    Name() string
    Requires() []CapabilityID
    Capabilities() Capabilities
}

func SetProvider(p Provider) error   // at most once, before any component starts
func Caps() *Capabilities            // one atomic load; read-only by contract
```

A nil field means "not taken over, native path". `SetProvider` fails - with
`merr.ErrServiceInternal` - on a nil or typed-nil provider, a typed-nil
capability, a `Requires()` entry that is absent or unknown, and a second
installation. `Caps()` returns a shared zero table when nothing is installed
so both paths cost the same.

### The entry point

`cmd/milvus.Main(args []string)` is the former `cmd/main.go` body. A
distribution's `main`:

```go
func main() {
    if err := extension.SetProvider(kite.Provider()); err != nil {
        log.Fatal(err)
    }
    milvus.Main(os.Args)
}
```

`Main` copies `args` and does not modify the caller's slice.

### The primary configuration file

`paramtable.PrimaryConfigName()` reports the file every paramtable reads in
`milvus.yaml`'s position. It is decided **before the process starts**: at link
time through
`-ldflags "-X github.com/milvus-io/milvus/pkg/v3/util/paramtable.primaryConfigName=kite.yaml"`,
overridden by the `MILVUS_PRIMARY_CONFIG` environment variable. It cannot be a
call: `internal/proxy`, `datacoord`, `rootcoord`, the coordinator and the
component clients all declare `var Params = paramtable.Get()` at package level,
and `Get` calls `Init`, so the global table exists before any `main` or `init`
a distribution could write. A name that is not a bare `.yaml`/`.yml` file name
panics at the first table, because the file source's own rejection of such a
name silently drops every local yaml source.

## Design

### Evolution policy

The table and the interfaces are consumed outside this repository, so they
change under one rule set (also in the package doc of `pkg/extension`):

1. `Capabilities` gains fields; it never loses or renames one. A new capability
   is a new field with a new interface.
2. An interface a **form implements** is one of two kinds, and says which:
   - **Noop-based**: every method has an inert answer, given by a `NoopXxx`
     type implementations embed. A method may be added only together with
     its inert default. `ProxyExtension`, `AdmissionChecker`,
     `RBACBootstrapper`, `CoordinatorEngine`, `ResourceGroupInterceptor`,
     `IndexDrainer`, `LoadPlacementScope`.
   - **Frozen**: no inert answer exists, so no method is ever added; a new
     need becomes a new `Capabilities` field. `APIKeyVerifier`,
     `InternalSurfaces`.
3. An interface **milvus implements** and hands to a form (`MixCoord`,
   `ProxyConnections`, `CoordClient`, `CredentialStore`) may gain methods and
   never loses one.
4. Structs crossing the boundary (`QueryPlacement`, `ResourceGroupUpdate`,
   `ShardLeaderReadiness`, `InternalListeners`) gain fields, never lose them.

Consequence: every signature in `pkg/extension` is a one-time decision. This
is why the reject hooks take a `context.Context`, `AdmissionChecker` receives
the request, and `InternalSurfaces` returns a struct.

### Fall-through and short-circuit

Every `Intercept*` method answers "fall through" with the zero value of its
results: `nil` error, `(false, nil)`, `(nil, nil)`. Anything else is a
short-circuit. None of them returns `*commonpb.Status`: `merr.Status(nil)` is a
**non-nil success status**, so a Status-returning hook written as
`return merr.Status(check())` would have short-circuited every request the
check passed - Insert answering success with an empty result and dropping the
rows. With `error` as the type, the same idiom is `return check()` and a
passing check falls through by construction.

Each method states whether it MAY REJECT, MAY REPLACE, or is OBSERVE ONLY; an
undocumented method observes (the HBASE-18770 convention).

### Error contract

An error a form returns reaches the client through `merr.Status`, so it must be
or wrap (with `merr.Wrap/Wrapf`) a sentinel from `pkg/util/merr`. Any other
error collapses to `UnexpectedError` (code 1), which no SDK retries. A
condition the request caused is an input-class sentinel; a condition of the
deployment (a cluster not up yet) is `merr.ErrServiceUnavailable` or another
retriable one.

### Mutation

A request handed to a form is read-only, and so is everything reachable from
it: milvus runs the native path on the same object after a fall-through and may
log or retry it. Where a hook may hand milvus something else to use, it returns
it (`RewriteRequestParams`, `BeforeCreateResourceGroup`, `ResourceGroupUpdate.
Forward`).

### Request annotation

`OnConnect` / `RewriteRequestParams` / `EnsureQueryReady` are one mechanism: a
form learns which of its own clusters a request is for - at Connect, per RPC,
or from a reserved DQL parameter it strips - carries it on the context under
its own key, and reads it back when admitting the query. milvus never learns
the vocabulary. `OnDisconnect` bounds the per-connection half.

### Routing scope

`EnsureQueryReady` returns a `QueryPlacement`; its `ResourceGroup` is bound
onto the request context by milvus alone (`WithQueryResourceGroup`, private
key) and honored by the shard-leader lookup, so "made ready on" and "routed to"
are the same resource group by construction. `QueryPlacement.Release` runs
`Finish` at most once and is deferred by the seam on every exit path including
panics.

### Shard-leader readiness

`ShardLeaderReadiness` and its reason constants are defined once, here.
querycoord's computation (`internal/querycoordv2/utils`, added by #52716) is
to import and return this type; #52716 currently carries its own copy with
identical values, and whichever of the two PRs merges second makes the switch.
When a producer returns an error the struct is unspecified; callers classify
on the error with `merr.IsRetryableErr` first.

### Relation to hookutil

hookutil stays the surface for out-of-tree binary plug-ins; a capability is
added here, never there. Where both could answer (API keys) the seam consults
this package first and falls back to hookutil only when the capability is nil.

## Seams

Nothing in #52981 consults the table. Each seam lands as its own change with
the tests that exercise the fall-through and the takeover path.

| Capability | Seam | Status |
|---|---|---|
| `ProxyExtension.InterceptDML` | proxy Insert/Delete/Upsert/Flush/FlushAll/ImportV2 | pending |
| `ProxyExtension.InterceptAdminRPC` | 27 admin RPCs in proxy impl.go | pending |
| `ProxyExtension` load-semantics group | proxy Load/Release/GetLoadState/GetLoadingProgress | pending |
| `ProxyExtension.OnConnect/OnDisconnect/Start` | proxy Connect + connection manager | pending |
| `ProxyExtension.RewriteRequestParams/EnsureQueryReady` | proxy Search/HybridSearch/Query | pending |
| `APIKeyVerifier` | proxy authentication interceptor | pending |
| `RBACBootstrapper` | rootcoord init, writing through the catalog as `MetaTable.InitCredential`/`initRbac` do (the broadcast path is not up yet) | pending |
| `AdmissionChecker` | rootcoord CreateCollection/CreateDatabase | pending |
| `CoordinatorEngine` | distributed/mixcoord server | pending, after #52716 |
| `ResourceGroupInterceptor` | querycoord resource-group handlers | pending |
| `IndexDrainer` | datacoord DropIndex/CreateIndex, querycoord index checker | pending |
| `LoadPlacementScope` | querycoord load job | pending |
| `InternalSurfaces` | distributed/proxy listeners | pending |

## Compatibility, deprecation, migration

- No behavior change in a stock binary: no seam consults the table; `cmd/main.go`
  is a thin wrapper around `Main`; the primary config name defaults to
  `milvus.yaml`.
- No configuration key is added. `MILVUS_PRIMARY_CONFIG` is an environment
  variable read only by `paramtable`.
- `ResetForTest` exists only under the `test` build tag.
- The pkg/proto types that cross the boundary (`querypb`, `indexpb`,
  `internalpb`) carry no cross-version promise; an in-tree form compiles
  against the same revision.

## Test plan

- `pkg/extension`: registry (install, requirement, unknown requirement, double
  install, typed-nil provider and capability, every `CapabilityID` requirable
  when supplied and refusable when absent), every Noop base inert and
  inherited by an embedder, the short-circuit-by-type guard, `QueryPlacement.
  Release` once-only, context keys unforgeable. 100% statement coverage under
  `-race`.
- `paramtable`: default name, link-time name, environment override, bad names
  refused on both paths, and end to end: a table built under the replaced name
  reads that file and not `milvus.yaml`; a missing primary is skipped.
- `cmd/milvus`: `Main` returns on the usage branch and leaves the caller's
  args intact; the package is now in `run_go_unittest.sh` and
  `run_go_codecov.sh`.
- Each seam change carries its own tests for fall-through and takeover.

## Rejected alternatives

- **Extending hookutil.** Untyped, `.so`-loaded, and unable to reach the
  coordinator, listeners or load semantics. Kept for its existing users.
- **One `Capabilities` field per method group of `ProxyExtension`** (split into
  `DMLInterceptor`, `LoadSemantics`, ...). Fully consistent with "fields only",
  but the seams already written against one interface would be reworked for a
  benefit the Noop base already provides. Revisit if a form ever needs one
  group without the others.
- **A run-time `UsePrimaryConfigName(name)` call.** Shipped first, then found
  to be unreachable early enough (see "The primary configuration file").
- **Returning `*commonpb.Status` from the reject hooks.** The `merr.Status(nil)`
  trap; see "Fall-through and short-circuit".
