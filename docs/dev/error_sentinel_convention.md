# Error Sentinel Convention

Milvus error handling has two distinct layers. This document describes the rules
that separate them, the rationale, and an audit of where the codebase currently
violates them. Companions: [error_handling_guide.md](./error_handling_guide.md)
(day-to-day how-to) and
[error_handling_casebook.md](./error_handling_casebook.md) (real
positive/negative examples of the mistakes that survive review).

## The two layers

### 1. Typed merr (wire-protocol errors)

Defined in `pkg/util/merr/errors.go` (`ErrCollectionNotFound`,
`ErrParameterInvalid`, etc.). These carry a numeric error code that is
serialized into `commonpb.Status{ErrorCode, Reason}` and shipped to the
client over gRPC. They are the only thing a client (or another Milvus
component on the receiving end of an RPC) sees.

Creation: `merr.WrapErrXxxMsg(...)` / `merr.WrapErrXxxErr(cause, ...)`
(origination only — never to add context to a typed merr that already exists,
see `merr.Wrap`).

### 2. Internal sentinels (single-process control flow)

Created with `errors.New(...)` at package scope (e.g. `errIgnoredAlterAlias`,
`errReleaseCollectionNotLoaded`, `errNodeNotEnough`). These are signaling
vocabulary inside a single Go process: a callee tells its caller "this is an
idempotent no-op" or "the queue is empty" so the caller can branch / retry /
ignore. They are **not** part of the wire protocol.

The catcher is always `errors.Is(err, sentinelX)` at some boundary in the
calling stack, and the boundary either:

- translates to `merr.Success()` (idempotency: e.g. drop something that
  doesn't exist → success), or
- translates to a typed merr `merr.WrapErrXxxMsg(...)` (e.g. user already
  exists → `WrapErrParameterInvalid`).

## The hard invariant

> **Any internal sentinel must be `errors.Is`-caught and translated to a
> typed merr (or `Success`) before crossing any gRPC handler boundary —
> client-facing or component-to-component.**

Why "any gRPC boundary, not just client-facing": gRPC serializes errors to
`commonpb.Status{ErrorCode, Reason}`. The Go-level pointer identity that
`errors.New` sentinels rely on does not survive the wire. The peer's
`merr.Error(status)` reconstructs a typed merr from the numeric code; the
sentinel chain is gone forever. So a sentinel that escapes an internal coord
RPC is just as broken as one that escapes a user-facing RPC — only quieter,
because no customer sees the resulting `Code=65535 (unexpected)`.

### What breaks the invariant

The `errors.Is` chain survives `return err`, `errors.Wrap(err, "...")` /
`merr.Wrap(err, "...")` (cockroachdb thin wrap), **and**
`merr.WrapErrServiceInternalErr(err, "...")` — `milvusError.Unwrap()`
returns the inner error, so `errors.Is(outer, innerSentinel)` stays true through any
of them. It is **destroyed** only by:

- Putting the cause in a format argument instead of the chain:
  `merr.WrapErrXxxMsg("...: %s", err)`, or the `%w` mistake (`WrapErr*Msg`
  formats with `fmt.Sprintf`, which does **not** honor `%w` and renders
  `%!w(...)`). The inner error reaches the message text but is unreachable via
  `Unwrap()`, so `errors.Is(outer, innerSentinel)` returns false.
- Any custom wrapper that doesn't implement `Unwrap()`.

Do **not** confuse this with the separate code/retriability rule.
`merr.Wrap(err, ...)` and `merr.WrapErrXxxErr(err, ...)` both keep `errors.Is`
intact, but they differ in what the result reports at the boundary:

- `merr.Wrap(err, ...)` preserves the inner's `Code()` and `IsRetryable` — the
  chain still resolves to the inner's `*milvusError`.
- `merr.WrapErrXxxErr(err, ...)` reports the **outer** sentinel's code and
  retriability: `As()` resolves to `ErrServiceInternal` (Code 5,
  non-retriable), **masking** the inner's classification.

So there are two distinct rules, often conflated:

1. To keep `errors.Is` working: never stuff the cause into a format string;
   pass it as the error argument.
2. To add context **without changing the classification** (preserve the inner
   code + retriability): use `merr.Wrap`, not `merr.WrapErr*Err`. Reserve
   `merr.WrapErrXxxErr` for when you intend to *assert* a new classification
   (e.g. this genuinely is a service-internal error). (See also
   [feedback rule](#related-rules) on `merr.Wrap` vs `merr.WrapErr*Err`.)

## Naming convention

The convention has two layers, matched to the two error categories:

### Wire-protocol layer (typed merr) — uppercase `Err*` in `pkg/util/merr` only

All errors that may cross any gRPC boundary (client-facing or
component-to-component) must be `*merr.milvusError` defined in
`pkg/util/merr/errors.go`. They have:

- A numeric code passed to `newMilvusError(...)`; uniqueness is enforced by
  the init-time code registry (defining a second sentinel on an occupied code
  panics at package init, since `milvusError.Is` matches by code alone)
- A `var ErrXxx = newMilvusError(...)` declaration in `pkg/util/merr/errors.go`
- An exported `WrapErrXxxMsg` / `WrapErrXxxErr` helper

If an error needs to be visible to the wire, it lives here. No exceptions.

Every sentinel also carries an Input-vs-System classification (who is to
blame) that drives `Status.Retriable`, the `cause` metric label, lb_policy
failover and `retry.Do`; see "Input vs System: who is to
blame?" in [error_handling_guide.md](error_handling_guide.md).

#### Code-range partition

Codes are allocated in families. Before adding a sentinel, scan the registry
(`grep -nE "= newMilvusError\(" pkg/util/merr/errors.go`) and place the new
code inside its family's range — the init-time registry panics on a
*duplicate*, but it cannot tell you that 1305 belongs to the MQ family. Don't
open a new range for a one-off; most "new" errors fit an existing family or an
existing sentinel (check before inventing: both `ErrSegcore` and
`ErrMqInternal` were nearly re-invented during the standardization work).

| Range | Family | Range | Family |
|---|---|---|---|
| 1–99 | service-level (`NotReady` 1, `Unavailable` 2, `Internal` 5, …) | 1300–1399 | MQ |
| 100–199 | collection | 1400–1499 | privilege / RBAC |
| 200–299 | partition | 1600–1699 | alias |
| 300–399 | resource group | 1700–1799 | field |
| 400–499 | replica | 1800–1899 | HTTP / REST gateway |
| 500–599 | channel | 1900–1999 | replicate / CDC |
| 600–699 | segment | 2000–2099 | segcore + knowhere (cgo; table-driven, see below) |
| 700–799 | index | 2100–2199 | import |
| 800–899 | database | 2200–2299 | query / requery plan |
| 901–999 | node | 2300–2399 | compaction |
| 1000–1099 | io / storage / serialization / data integrity | 2400–2499 | function pipeline (`ErrFunctionFailed` 2400) — but `ErrDataNodeSlotExhausted` is 2401, so check occupants before assuming |
| 1100–1199 | request parameter | 2500–2599 | KMS |
| 1200–1299 | metrics | 2600–2699 | snapshot |
| | | 3000+ | misc (`ErrOperationNotSupported` 3000, `ErrOldSessionExists` 3001) |

`65535` (`(1<<16)-1`) is `errUnexpected` — the wire fallback for errors that
carry no merr code. It is reserved; never originate it deliberately. The
2000–2099 segcore range is owned by the cgo conversion table: go through
`merr.SegcoreError` (`pkg/util/merr/utils.go`), which consults the
code/retriability table in `pkg/util/merr/segcore.go` — don't hand-pick
numbers in the range (casebook Pattern 7).

#### `milvusError.Is` matches by code — two consequences

1. **One code, one sentinel.** Two sentinels sharing a code would be
   `errors.Is`-equal; the init-time registry panic exists to make that
   unrepresentable.
2. **Promoting an internal `errors.New` sentinel to a merr widens every
   guard.** A bare sentinel matches by pointer identity; a merr matches by
   code. After a conversion, `errors.Is(err, thatSentinel)` matches *any*
   error with the same code — silently. Before converting, run
   `grep -rn "errors.Is(.*<sentinelName>"` and audit every hit (casebook
   Pattern 6 documents the data-loss-class near-miss this rule comes from).

### Internal-sentinel layer — lowercase `errXxx`, same-package only

Internal sentinels live in `internal/...` packages, are created with
`errors.New(...)`, and are **lowercase / unexported**. The rule:

> **A `var err* = errors.New(...)` declared in `internal/...` may only be
> referenced inside the same Go package. Cross-package consumers must not
> see it.**

This makes Go visibility do the enforcement: if you need a signal across
package boundaries, you either (a) lift it into the wire layer as a typed
merr, or (b) redesign the API so the signal flows via a return value
(e.g. `(ignored bool, err error)`), not via the error type.

Example (current code, after the 04-coord cleanup):

```go
// errFull / errNoSuchElement are INTERNAL sentinels: caught by errors.Is
// inside the compaction inspector / scheduler loop and never serialized
// across any gRPC boundary.
var (
    errFull          = errors.New("compaction queue is full")
    errNoSuchElement = errors.New("compaction queue has no element")
)
```

### Cross-package idempotency: use a return-value flag, not an exported sentinel

The previous code exported `meta.ErrResourceGroupOperationIgnored` so that
the parent package `querycoordv2` could catch it via `errors.Is` and
translate to `merr.Success()`. This was an exported `Err*` in
`internal/...` — visually indistinguishable from a `merr.ErrXxx` typed
wire error, easy to misuse.

The current code instead encodes the signal in the return value:

```go
// meta/resource_manager.go
func (rm *ResourceManager) CheckIfResourceGroupAddable(...) (ignored bool, err error) {
    if proto.Equal(rm.groups[rgName].GetConfig(), cfg) {
        return true, nil   // idempotent no-op
    }
    ...
}

// querycoordv2/ddl_callbacks_alter_resource_group.go (broadcaster)
func (s *Server) broadcastCreateResourceGroup(...) (ignored bool, err error) {
    if ignored, err := s.meta.CheckIfResourceGroupAddable(...); err != nil || ignored {
        return ignored, err
    }
    ...
}

// querycoordv2/services.go (RPC handler)
ignored, err := s.broadcastCreateResourceGroup(ctx, req)
if err != nil { return merr.Status(err), nil }
if ignored { return merr.Success(), nil }
```

No sentinel crosses the package boundary; the signal travels via a
structured return value. This is the preferred pattern for any new
cross-package idempotency case.

## Current state (audit done on err-std-04-coord branch, 2026-05-19)

Across `internal/{datacoord,rootcoord,querycoordv2}` there are 28
`errors.New(...)` sentinels. Their fates:

| Kind | Count | Examples | Status |
|---|---|---|---|
| Idempotency: caught → `merr.Success()` | 13 catch sites, ~12 distinct sentinels | `errIgnoredAlterAlias`, `errIgnoredCreateCollection`, `errReleaseCollectionNotLoaded`, `errUserNotFound`, ... | ✅ compliant |
| Caught → translated to typed merr | 3 catch sites (`errUserAlreadyExists`, `errRoleAlreadyExists`, `errRoleNotExists`) | client gets `WrapErrParameterInvalidMsg(...)` or `WrapErrServiceInternalMsg(...)` | ✅ compliant (1100 / 5) |
| Background-only (never enter an RPC handler) | 5 (`errFull`, `errNoSuchElement`, `errNodeNotEnough`, `errDisposed`, `errTypeNotFound`) | compaction queue / resource observer / session lifecycle / checker registry | ✅ compliant |
| Cross-package idempotency via `(ignored bool, err error)` signature | 1 (resource group create/drop) | meta layer returns `ignored=true`; querycoordv2 RPC handler translates to `merr.Success()` — no sentinel crosses package | ✅ compliant (refactored from exported `ErrResourceGroupOperationIgnored` in this branch) |
| Dead code (function with 0 callers) | 3 (`errNilResponse`, `errNilStatusResponse`, `errUnknownResponseType` — all only used by `VerifyResponse` in `datacoord/util.go`) | safe to delete | 🪦 cleanup candidate |
| ~~**Violation**: escapes RPC handler with no catch~~ (resolved) | 3 (`errEmptyUsername`, `errEmptyRoleName`, `errEmptyPrivilegeGroupName`) | origin sites in `meta_table.go` now emit `WrapErrParameterInvalidMsg` directly; the bare sentinels are gone | ✅ resolved |
| ~~**Semantic miscategorization**: caught and wrapped with the wrong typed merr code~~ (resolved) | 1 (`errTypeNotFound` in `ops_services.go:87,101`) | invalid `CheckerID` from the client now wrapped as `WrapErrParameterInvalidMsg` (code 1100), was `WrapErrServiceInternal` (code 5) | ✅ resolved |

### Cleanup status

1. ✅ Done — `errEmptyUsername` / `errEmptyRoleName` / `errEmptyPrivilegeGroupName`
   at the origin sites in `internal/rootcoord/meta_table.go` now emit
   `merr.WrapErrParameterInvalidMsg("username is empty")` etc. directly; the bare
   sentinels no longer exist.
2. ✅ Done — `errTypeNotFound` at the catcher in
   `internal/querycoordv2/ops_services.go:87,101` is now wrapped as
   `merr.WrapErrParameterInvalidMsg("invalid checker type %d: %v", req.CheckerID, err)`.
3. Not done — delete `VerifyResponse` and its three dead-code sentinels
   (`errNilResponse`, `errNilStatusResponse`, `errUnknownResponseType`).

## Future linter ideas

Three candidates, in order of how cheap they are to implement and how
hard the enforcement is. **Tier 1.5's return form is now implemented (see
below); Tier 1 and Tier 2 remain a design queue.**

### Tier 1 — exported-sentinel ban (1 hour to write)

The simplest rule: **`internal/...` packages may not declare exported
`var Err\w+ = errors.New(...)`.** Scan all `internal/...` *.go files,
fail CI if any match. Two paths to fix a violation:

1. Lowercase it (`var errXxx = errors.New(...)`) — only callable inside the
   same package. If the lint fails because a cross-package caller needs the
   signal, see fix 2.
2. Refactor the API so the signal travels via a return value
   (e.g. add `ignored bool` to the return tuple) and delete the sentinel.

This makes Go visibility itself the enforcement mechanism: anything that
needs to look like `merr.ErrXxx` to a reviewer can only exist in
`pkg/util/merr`. Internal sentinels stay quietly lowercase in their owning
package.

Lowercase sentinels (`var errXxx = errors.New(...)`) inside `internal/...`
are still encouraged to carry an `INTERNAL: ...` doc comment for reviewer
context, but it's not enforced — the visibility rule already prevents the
worst-case (`Err*` collision with `merr.ErrXxx`).

### Tier 1.5 — bare-usage ban (1 hour grep, half-day AST for 100% precision)

> **Status — return form implemented.** The **return** form of this ban is now
> enforced by a `gocritic`/`ruleguard` rule (`rawmerrerror` in `rules.go`), run
> under `make verifiers`: it rejects `return errors.New / fmt.Errorf /
> errors.Errorf` from function bodies (package-level sentinels, `cmd/`, `tests/`,
> codegen and the walimpls harness exempt). The day-to-day guide is
> [error_handling_guide.md](./error_handling_guide.md). The **no-exceptions**
> form below (local `:=`, `panic(...)`, function argument) is *not* covered:
> ruleguard's DSL cannot match "a call anywhere in a function body but not in a
> `ValueSpec`", so the full ban still needs the AST-based Tier 2 linter.

**Hardest enforcement, no exceptions.** `internal/...` packages may not
use `errors.New(...)` or `errors.Errorf(...)` **inline inside a function
body**. The only legal site for these calls is a package-level
`var <Name> = errors.New(...)` (sentinel declaration).

Allow/deny matrix:

| Form | Location | Verdict |
|---|---|---|
| `var errInvalid = errors.New("invalid")` | package-level (file top / `var` block) | ✅ allowed |
| `var ( errA = ...; errB = ... )` | package-level `var` block | ✅ allowed |
| `return errors.New(...)` | function body | ❌ banned |
| `x := errors.New(...)` | function body local | ❌ banned |
| `panic(errors.New(...))` | function body | ❌ banned |
| `foo(errors.New(...))` | function body argument | ❌ banned |

**Why no exceptions** (even for "local break signal" / "log-only" /
"panic-bound" cases that look harmless today):

- Today's local var can be hoisted to package-level by tomorrow's refactor
  and silently start crossing boundaries.
- A linter with exceptions needs AST-level wire-reachability analysis
  (expensive); a no-exception linter is one grep.
- Forces authors to use the right primitive instead of `errors.New` as
  a universal escape hatch:
  - **break signal from a callback** → define a `type doneSignal struct{}`
    that implements `error` and use `errors.As`. Intent is now in the type,
    not in a string-keyed sentinel.
  - **"unreachable" assertion** → just `panic(...)`. If caller already
    does `if err != nil { panic(err) }`, fold it into the callee.
  - **input validation / config validation** → `merr.WrapErrParameterInvalidMsg(...)`
    or `status.NewInvalidArgument(...)` depending on layer.

Implementation: grep version covers ~95% true violations in ~1 hour.
AST version (go/analysis) covers the edge cases (e.g. `init()` body
assigning to a package var) but needs ~half a day. Start with grep,
upgrade if false-positive rate exceeds 5%.

```bash
# grep skeleton
grep -rnE 'errors\.(New|Errorf)\(' internal/ --include='*.go' \
  | grep -v _test.go \
  | grep -vE ':[0-9]+:\s*(var\s+)?[A-Za-z_]+\s*=\s*errors\.(New|Errorf)' \
  | grep -vE ':[0-9]+:\s*[A-Za-z_]+\s+(\w+\s+)?=\s*errors\.(New|Errorf)'
# Any remaining line = violation
```

A `//nolint:err-bare` escape valve with a required justification comment
handles the genuine outliers (a few `init()` patterns, embedded `errors.Mark`
usage, etc.).

### Tier 2 — escape-path linter (~1 day, go/analysis based)

For every gRPC handler method (anything matching the
`internal/{rootcoord,datacoord,querycoordv2}/services.go,root_coord.go,*_handler.go`
pattern, return type `(*proto.XxxResponse, error)` or `(*commonpb.Status,
error)`), trace the err-return data-flow. Any error that:

- transitively originates from an `INTERNAL:`-tagged sentinel, **and**
- reaches a `return Status{Code: merr.Status(err)}` or `return err` without
  passing through an `errors.Is(err, internalSentinelX) { ... }` branch,

is a violation. Report file:line of the leak.

This catches the actual invariant violation (the 3 RBAC empty sentinels
would have been flagged), not just naming hygiene. Requires AST analysis;
worth doing if the cost of one more silent `Code=1` to a client is high.

### Tier 3 (no longer necessary if Tier 2 is in place) — wrap-rule linter

Scan `merr\.WrapErr[A-Za-z]+Err\(err, ` (note: first arg `err`, not a
fresh string-only origination) and require the cause `err` to not itself be
a typed merr. This is what
[`feedback_merr_wrap_rule`](../../) already enforces by convention; Tier 2
would catch the symptom (sentinel escape) as a side effect.

## Related rules

- `feedback_merr_wrap_rule` (this repo's collaboration memory): "Add context
  to an existing err with `merr.Wrap` / `merr.Wrapf`, never with
  `merr.WrapErr*Err` — the latter masks the inner typed code and
  retriability (the `errors.Is` chain itself is preserved via `Unwrap()`)."
- `project_errstd_autogen_defects`: three systematic defects in the
  auto-generated `errors.Wrap → merr` conversion in this branch series;
  defects #2 and #3 are direct consequences of violating the rules in
  this document.
