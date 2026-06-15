# Error Handling Guide

How to produce and return errors in Milvus server code — the day-to-day how-to.
For the underlying rules, the sentinel naming convention, and the enforcement
roadmap, see [error_sentinel_convention.md](./error_sentinel_convention.md). For
real positive/negative examples of the mistakes that survive review — wrong
classification, masked codes, broken `errors.Is` chains — see
[error_handling_casebook.md](./error_handling_casebook.md). For
the canonical numeric code list, see the sentinel definitions in
[`pkg/util/merr/errors.go`](../../pkg/util/merr/errors.go). (The
[appendix_d_error_code.md](../developer_guides/appendix_d_error_code.md)
appendix predates merr and lists the **deprecated** `commonpb.ErrorCode` enum,
not the merr codes.)

## TL;DR

1. **Never** `return errors.New(...)` or `return fmt.Errorf(...)`. A linter
   rejects it (see [The one rule](#the-one-rule-never-return-a-bare-error)).
2. An error that leaves your component must be a **typed** error carrying a code.
   The lingua franca across Milvus components is **merr**.
3. Pick one of three forms: originate a typed merr, add context with `merr.Wrap`,
   or — only if a caller branches on it by identity — a package-level sentinel.

## The mental model: three kinds of error, one boundary rule

Milvus has three legitimate kinds of error, distinguished by **how far they
travel**, not by syntax:

| Kind | Scope | Carrier | Example |
|---|---|---|---|
| **① merr** | across Milvus components & to clients | numeric code on the main gRPC wire | `merr.ErrCollectionNotFound` |
| **② component-internal dialect** | between sub-modules of one big component | that component's own typed error + its own wire | `streamingutil/status.StreamingError` |
| **③ internal sentinel** | within a single Go process | `errors.New` pointer identity, caught by `errors.Is` | `errSessionVersionCheckFailure` |

The single rule that ties them together:

> **The error you must return follows the interface's promise** — it is decided
> by *who is on the other side of the boundary you are crossing*, not by where
> the error was born.

- Crossing into **another Milvus component** (proxy → rootcoord), or returning to
  a **client**: the promise is **merr**. Translate to a typed merr at that
  boundary.
- Staying **inside one component** (e.g. the streaming sub-modules talking to
  each other): the component may speak its own typed dialect (`StreamingError`).
  It is **not** required to be merr — but it must still be *typed*, and it gets
  translated to merr at the component's outer edge.
- On **no** wire, ever: a **bare** `errors.New` / `fmt.Errorf`. Internal
  sentinels (kind ③) are bare, but they never reach a wire — they are caught by
  `errors.Is` and translated first.

### Why component-internal dialects are allowed (the StreamingError case)

Streaming is one big component whose sub-modules (streamingnode, streamingcoord,
the streaming client) talk to each other constantly. They use
`streamingutil/status.StreamingError`, which has its **own** error codes and its
**own** gRPC encoding. That is deliberate: it is a *bounded context* with its own
vocabulary. There is intentionally **no** global "StreamingError → merr"
auto-converter — that would erase the dialect. Instead the conversion happens
**once, at the consumer's boundary**, and the consumer decides how, based on
what its own interface promises:

```go
// rootcoord consumes a streaming service inside CreateCollection.
err := s.streamingService.DoSomething(ctx, ...) // may return *StreamingError
if err != nil {
    // (a) You care about the code the client sees → translate explicitly:
    if se := status.AsStreamingError(err); se != nil && se.IsRateLimitRejected() {
        return merr.WrapErrServiceRateLimit("streaming backpressure")
    }
    // (b) You don't care about a precise code → let it fall back at the
    //     boundary (see "The safety net"); the client gets a generic
    //     internal-class error. Prefer being explicit, but this is allowed.
    return err
}
```

## The one rule: never return a bare error

```go
return errors.New("segment not loaded")           // ❌ linter rejects
return fmt.Errorf("segment %d not loaded", id)     // ❌ linter rejects
```

Why it is banned, even though the boundary would "fix it up" anyway: a bare error
that escapes to a gRPC boundary becomes `Code=65535 (Unexpected)` — visually
indistinguishable from "the server hit an unhandled bug". A wall of
`errors.New("reason 1")`, `errors.New("reason 2")` is a sign nobody planned the
error taxonomy: the caller cannot program against it, and it all collapses into
one opaque code on the wire. A typed error costs one extra word and makes the
failure *addressable*. The linter exists to build the habit: **the thing I
return is always typed.**

## Decision tree: what should I return?

```
Am I crossing into another component / returning to a client?
│
├─ No (staying inside my own component)
│   ├─ My component has its own typed dialect (e.g. streaming)?
│   │     → use that dialect's factory (status.New*), not merr, not errors.New
│   └─ Otherwise → a typed merr (rules below); or, only if a caller in THIS
│         process branches on the outcome by identity, a package-level sentinel (§3.3)
│
└─ Yes → I must return a typed merr:
    ├─ Brand-new failure, no underlying error worth carrying?
    │      → merr.WrapErrXxxMsg("detail %s", v)             (§3.1 originate)
    │        and pick Input vs System deliberately (next section)
    ├─ I hold an underlying error and want to KEEP its code, just add context?
    │      → merr.Wrap(err, "while doing X")                (§3.2 add context)
    └─ I hold an underlying error and want to DOWNGRADE it to a generic class?
           → merr.WrapErrServiceInternalErr(err, "...")     (§3.2 — deliberate override)
```

## Input vs System: who is to blame?

Every merr is classified as **InputError** (the request author's fault) or
**SystemError** (Milvus's fault, the default). Choosing a factory chooses the
classification, so when you originate an error, ask one question first:

> **Would a correctly implemented Milvus ever hit this branch, given this
> request?** If the request content itself triggers it → InputError. If
> reaching this branch means a Milvus bug or internal failure → SystemError.

Quick rules for the cases that get misclassified in practice:

- A **plan / task type / request produced by a coordinator** is not user input.
  An unrecognized task type or malformed compaction plan is an internal
  protocol violation (think mixed-version rolling upgrade) →
  `WrapErrServiceInternalMsg`, even though the check looks like validation.
- **Data produced by segcore or another internal component** is not user
  input. A violated data-shape contract (ValidData length, truncated vectors)
  is a Milvus bug → `WrapErrServiceInternalMsg`.
- A **TOCTOU race** (state was valid at check time, changed by execution time)
  is not user input → keep it a system error.

### How classification is attached

Two mechanisms, used in different situations:

1. **Baked-in sentinels.** ~25 sentinels are declared
   `WithErrorType(InputError)` in `errors.go` (`ErrParameterInvalid/Missing/
   TooLarge`, `ErrPrivilegeNotPermitted`, `ErrDatabaseInvalidName`, ...).
   Using their factory *is* the classification — which is exactly why reaching
   for `WrapErrParameterInvalidMsg` to express an internal assertion is wrong.
2. **Boundary marking** for dual-use sentinels. `ErrCollectionNotFound` stays
   SystemError (internal refresh/retry paths depend on that), and the proxy
   boundary stamps it InputError only where the name came from the user:

   ```go
   // proxy meta cache, the central chokepoint for user-supplied names:
   return collection, merr.WrapErrAsInputErrorWhen(err,
       merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
   ```

   `WrapErrAsInputError(err)` marks unconditionally;
   `WrapErrAsInputErrorWhen(err, targets...)` marks only if the error's code
   matches a target. Both preserve `errors.Is` and the code — they relabel the
   classification, nothing else.

### What the classification drives

| Surface | InputError behavior |
|---|---|
| `commonpb.Status` | `ExtraInfo["is_input_error"]="true"`, `Retriable` forced `false` |
| Prometheus | request counted as `fail_input` / `rejected_user` (vs `fail_system` / `rejected_system`) |
| Access log / failure log | `error_type` field set accordingly |
| proxy lb_policy | **no cross-replica failover** — retrying a bad request elsewhere can't help |
| `retry.Do` | aborts immediately instead of retrying |

The last two rows are why misclassification is not cosmetic: marking an
internal failure as InputError disables the retry/failover machinery that
would have healed it, and a dashboard blames users for Milvus bugs.

### Pitfalls (each of these happened)

- **Don't mark a shared sentinel InputError globally** to fix one callsite —
  every internal `retry.Do` loop waiting on that error stops retrying. Use
  boundary marking instead. Pre-flight scan before adding
  `WithErrorType(InputError)` to a sentinel (or stamping at a new boundary).
  The retrier is usually a **caller in a different file** than the producer,
  so a same-file overlap check is not enough — trace one level up the call
  graph:

  ```bash
  # 1. every site that originates the code (substitute the real wrapper symbol):
  grep -rn "WrapErrServiceUnavailable" internal/ pkg/ --include='*.go'
  # 2. for each producing function from step 1, find its callers…
  grep -rn "CheckAllQnReady" internal/ --include='*.go'
  # 3. …and check whether any caller invokes it inside a retry.Do body:
  grep -rn -A8 "retry\.Do" internal/rootcoord/create_collection_task.go | grep CheckAllQnReady
  ```

  A real save (the example the commands above trace):
  `WrapErrServiceUnavailableMsg("file resource not synced, …")` originates in
  `internal/coordinator/file_resource_observer.go` (`CheckAllQnReady`); the
  `retry.Do` polling it during CreateCollection lives one call up, in
  `internal/rootcoord/create_collection_task.go` — a same-file scan finds
  nothing. That error must ride a retriable system code
  (`ErrServiceUnavailable`), never an InputError-marked one. See casebook
  Pattern 5.
- **Don't classify in a helper** what only the boundary can know. The same
  not-found is the user's fault when the name came from a request, and a
  system fault when it came from internal state — stamp at the chokepoint
  where the origin is known.
- **"Looks like validation" is not the test.** Coordinator-to-node protocol
  checks, segcore output checks, and cgo boundary checks all look like
  validation; none of them are user input.

## The three correct ways

### 3.1 Originate a typed error — `WrapErrXxxMsg` / `WrapErrXxx`

When the failure starts here and there is no inner error worth preserving:

```go
return merr.WrapErrParameterInvalidMsg("nq (%d) exceeds the limit (%d)", nq, max)
return merr.WrapErrCollectionNotFound(collectionName)
```

Pick the sentinel whose **code** matches the failure's meaning (see the sentinel
definitions in [`pkg/util/merr/errors.go`](../../pkg/util/merr/errors.go)). This is the common
case: most of the time you only need to attach a message to a well-chosen code,
and the framework does the rest.

### 3.2 Add context but keep the code — `merr.Wrap`, never `WrapErr*Err`

When you already hold a typed error and only want to add a breadcrumb:

```go
if err := s.loadSegment(ctx, id); err != nil {
    return merr.Wrap(err, "while loading sealed segment") // ✅ keeps the inner code
}
```

`merr.Wrap` / `merr.Wrapf` is a thin wrapper (like `errors.Wrap`): it prepends
context and **preserves** the inner error's code and its `errors.Is` chain.

⚠️ Do **not** reach for `merr.WrapErrServiceInternalErr(err, ...)` (or any
`merr.WrapErrXxxErr`) just to add context. Those build a
`wrappedMilvusError{sentinel: ErrServiceInternal}` whose `code()` returns the
**outer** sentinel's code — they **overwrite** the inner typed code with
ServiceInternal (5) and force it non-retriable (`As()` resolves to the outer
sentinel). The `errors.Is` chain itself survives via `Unwrap()`, but the typed
code and retriability are masked. `WrapErrXxxErr` is *only* for
when you **deliberately** want to relabel the inner error to a new code (e.g.
collapse a noisy internal failure into one ServiceInternal for the client).

This split is intentional, and the framework deliberately does **not** try to be
clever: *keep the code* versus *downgrade the code* is a decision you state, not
one the framework guesses. `merr.Wrap` **always** keeps the inner code;
`WrapErrXxxErr` **always** relabels to the outer one. A helper that "smartly"
preserved the inner code whenever it recognized a typed merr would blur the
intent — a reader could no longer tell from the call site whether the author
meant to preserve or to downgrade. The choice of helper *is* the statement of
intent.

### 3.2.1 The base-package case: pass through, wrap, or relabel?

Low-level packages (`pkg/...`, `internal/util/...`) sit under many callers and
usually receive an error from something even lower — etcd, S3, a third-party
library, another util. For every such error you must consciously pick one of
three. Getting this wrong in a base package is expensive: it is multiplied across
every caller.

| Choice | Use it when | How |
|---|---|---|
| **Pass through** the original `err` | the inner err is already a typed error meaningful to your caller, or your package has no business classifying it — let the caller decide | `return err` |
| **Wrap, keep the code** | you want to add a breadcrumb (which key/path/op failed) without changing what the error *means* | `merr.Wrapf(err, "etcd txn on key %s", k)` |
| **Relabel / downgrade** | the inner err is a leaky implementation / third-party detail your caller should not see; you translate it into the typed merr your package's interface promises | `merr.WrapErrXxxErr(err, "...")` |

Deciding questions, in order:

1. **Is the inner err already typed and meaningful to my caller?** → pass through.
2. **Does any caller `errors.Is` the inner err's identity?** → pass through or
   `merr.Wrap` (both preserve the chain); **never relabel** — it hides the chain.
3. **Is the inner err a third-party / implementation detail I promise to hide?**
   → relabel to the typed merr my interface promises.

A base package that relabels too eagerly destroys codes the upper layers needed;
one that passes a raw third-party error straight through leaks an untyped error
toward the boundary. Neither is acceptable — the choice must be deliberate, and it
follows your package's interface promise, not convenience.

### 3.3 Need identity branching or reuse — a package-level sentinel

Define a sentinel when a caller **in the same process** branches on the outcome
by identity, via `errors.Is`:

```go
// internal/util/sessionutil/session_util.go — caught by isNotSessionVersionCheckFailure
// and used as a retry.Do predicate; the identity must survive, so it stays a
// bare sentinel rather than a merr error.
var errSessionVersionCheckFailure = errors.New("session version check failure")
```

Rules for sentinels (full version in
[error_sentinel_convention.md](./error_sentinel_convention.md)):

- **Package-level, never function-local.** A local `x := errors.New(...)` is a
  refactor hazard — tomorrow it gets hoisted and silently crosses a boundary.
  Lift it to a `var` at package scope.
- **Lowercase / unexported** when it lives in `internal/...`. If a
  *cross-package* caller needs the signal, do **not** export the sentinel —
  redesign the API to carry the signal in a return value (e.g.
  `(ignored bool, err error)`).
- It must be `errors.Is`-caught and translated to a typed merr (or
  `merr.Success()`) **before** crossing any gRPC boundary. A sentinel that
  reaches the wire is just an opaque `Code=65535`.
- **Converting an existing sentinel to a typed merr changes `errors.Is`
  semantics**: `merr.Is` matches by numeric code alone, so every
  `errors.Is(err, thatSentinel)` guard widens from "this exact signal" to
  "any error sharing the code". Run
  `grep -rn "errors.Is(.*<sentinelName>"` first and audit every hit — see
  casebook Pattern 6 for the near-miss this prevents.

When unsure between 3.1 and 3.3: if nobody does `errors.Is` on it, you don't need
a sentinel — just originate a typed merr with a message (3.1).

## The safety net: boundary fallback (and why not panic)

If a non-typed error does reach a gRPC handler, `merr.Status(err)` falls back to
`Code=65535 (Unexpected)`. This is a **backstop, not a feature**: it keeps the
server from leaking internals or crashing, but the client gets an opaque code.
Treat any `Code=65535` in logs as "someone forgot to type their error".

Why the boundary **falls back instead of panicking**: third-party libraries and
deep call chains produce errors on paths that tests cannot fully cover. Panicking
on "not a typed merr" would turn a stray untyped error into an outage. The
contract is therefore: **fall back to a generic code, never panic.** The goal of
the linter and this guide is to make that fallback path *empty in practice* — so
that every error a client sees was deliberately typed at its source.

## Enforcement

- A `gocritic`/`ruleguard` rule (`rawmerrerror` in `rules.go`) rejects
  `return errors.New / fmt.Errorf / errors.Errorf` from function bodies. It runs
  under `make verifiers` (via `static-check`), so no extra command is needed.
  Exempt paths (run outside the request path): `*_test.go`, `cmd/`, `tests/`,
  codegen, the walimpls harness, and `/mocks/` (generated mock helpers are
  test infrastructure even though some lack a "Code generated" header).
- It catches the **direct-return** form — the one that lets a raw error escape to
  a boundary. Assignment-then-return escapes (`e := errors.New(); return e`) and
  the full no-exceptions ban require the AST-based linter described as "Tier 2"
  in [error_sentinel_convention.md](./error_sentinel_convention.md).
