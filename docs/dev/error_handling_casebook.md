# Error Handling Casebook

Real positive and negative examples for choosing, wrapping and classifying merr
error codes — a companion to:

- [error_handling_guide.md](./error_handling_guide.md) — the rules and the
  decision tree (read it first).
- [error_sentinel_convention.md](./error_sentinel_convention.md) — sentinel
  layering and naming.

Every negative example below is **real**: it is what the first draft of the
error-standardization PR #50221 (issue #47420) actually wrote, caught and
corrected during its review — 60+ misclassifications in total. (The PR was
squash-merged, so these intermediate states are not visible in master's
history; before #50221 most of these sites returned bare `fmt.Errorf`.) They
cluster into the seven patterns in this document. If you (human or coding agent) are about to originate, wrap or
classify an error, check your change against these patterns — the linter
catches *bare* errors, but it cannot catch a *wrong* code or a *wrong*
classification. That is what this casebook is for.

> Quick self-check before any error-handling change:
>
> 1. Adding context to an existing error? → `merr.Wrap` / `merr.Wrapf` only.
>    `WrapErrXxxErr` overwrites the inner code (Pattern 3).
> 2. The check "looks like validation"? → ask *who produced the value*. Plans,
>    task types and component outputs are not user input (Patterns 1–2).
> 3. Passing a cause? → as the error argument, never into the format string
>    (Pattern 4).
> 4. Marking a sentinel `InputError`? → first grep for `retry.Do` users of that
>    code; InputError aborts retries (Pattern 5).
> 5. Converting an `errors.New` sentinel to merr? → first grep
>    `errors.Is(..., thatSentinel)`; `merr.Is` matches by **code**, so identity
>    guards silently widen (Pattern 6).
> 6. About to "fix" an odd-looking boundary conversion? → it may be a wire
>    contract (Pattern 7). Check for a comment, then check SDK/e2e expectations.

---

## Pattern 1 — "Looks like validation" is not user input

A `switch`/`if` that rejects an unexpected value *looks* like input validation.
The classification question is not "is this a check?" but **"who produced the
value being checked?"** A plan, task type, or request assembled by a
coordinator is internal protocol, not user input. If the check fires, Milvus
(or a mixed-version deployment) has a bug — the user did nothing wrong, and
returning an InputError would disable the retry/failover machinery and blame
the user on dashboards.

❌ As first drafted (caught in review): an unrecognized task type — assigned
by the coordinator, never typed by a user — returned a parameter error:

```go
// task type comes from the coordinator, not from any user request
default:
    return p[TypeKey], merr.WrapErrParameterInvalidMsg("unrecognized task type '%s', taskID=%s", p[TypeKey], p[TaskIDKey])
```

✅ As merged (`pkg/taskcommon/properties.go`, `GetTaskType`):

```go
default:
    // Task types are assigned by the coordinator; an unrecognized one means a
    // protocol mismatch (e.g. a newer coordinator scheduling onto an older
    // node), which is system blame, not user input.
    return p[TypeKey], merr.WrapErrServiceInternalMsg("unrecognized task type '%s', taskID=%s", p[TypeKey], p[TaskIDKey])
```

Same fix applied to: datanode `CreateTask`/`QueryTask`/`DropTask` dispatch,
compaction `preCompact` plan-shape checks (mix/sort/L0/bump), and the
`reduceStatisticResponse` unknown-key branch.

**Rule of thumb:** if the only way to reach the branch is a Milvus bug or a
mixed-version rolling upgrade → `WrapErrServiceInternalMsg`, even though the
code shape screams "validation".

## Pattern 2 — Internal component output is not user input

Data produced by segcore, a query node, or any internal stage is a contract
between Milvus components. When proxy-side reduce code finds a shape violation
in what a query node returned, that is a Milvus bug — not `ParameterInvalid`.

❌ As first drafted: proxy search-reduce blamed the user for a malformed
querynode response:

```go
if data.NumQueries != nq {
    return merr.WrapErrParameterInvalidMsg("search result's nq(%d) mis-match with %d", data.NumQueries, nq)
}
```

✅ As merged (`internal/proxy/search_reduce_util.go`, `checkSearchResultData`):

```go
if data.NumQueries != nq {
    return merr.WrapErrServiceInternalMsg("search result's nq(%d) mis-match with %d", data.NumQueries, nq)
}
```

Same fix applied to: the queryutil merge helpers (13 sites), proxy
`ShowPartitions` array-alignment checks in `meta_cache.go`, `channels_mgr`
vchannel/pchannel count checks, querynode stats-key handling, and the
function-pipeline runner output checks (46 sites — those use
`WrapErrFunctionFailedMsg`, the typed code that subsystem promises).

A sub-case worth naming: **recovered panics are never input errors.**

```go
// internal/datanode/external/manager.go — task executor:
if r := recover(); r != nil {
    ...
    reason := fmt.Sprintf("task panicked: %v", r)
    ...
    // A recovered panic is a server-side failure, never caller input.
    retErr = merr.WrapErrServiceInternalMsg("%s", reason)
}
```

## Pattern 3 — `WrapErrXxxErr` is a relabel, not a context-adder

`merr.WrapErrServiceInternalErr(err, ...)` (and every `WrapErrXxxErr`) reports
the **outer** sentinel's code and retriability — it *masks* the inner typed
code. Reaching for it "to add context" silently downgrades a precise,
possibly-retriable inner error into a generic non-retriable ServiceInternal.

```go
// ❌ inner err is already typed (e.g. ErrIoTooManyRequests, retriable);
//    this collapses it to ServiceInternal(5), non-retriable:
return merr.WrapErrServiceInternalErr(err, "failed to load segment %d", segID)

// ✅ keep the code, add the breadcrumb:
return merr.Wrapf(err, "failed to load segment %d", segID)
```

`WrapErrXxxErr` is only for a **deliberate** relabel — when your interface
promises to hide the inner detail behind a different code. The choice of
helper *is* the statement of intent; see §3.2 of the guide.

Storage-layer corollary (a cluster of real fixes): errors from
`ChunkObjectStorage` Read/Write/Remove/Copy are **already typed** by
`mapObjectStorageError` (`ErrIoKeyNotFound`, `ErrIoTooManyRequests` retriable,
…). Wrapping them with `WrapErrIoFailedErr`/`WrapErrServiceInternalErr`
flattens that taxonomy. Use `merr.Wrap`; originate a new Io error only for
errors born untyped (e.g. `WalkWithObjects` historically returned raw client
errors).

## Pattern 4 — The cause goes in the error argument, never the format string

```go
// ❌ errors.Is chain destroyed; %w renders as "%!w(...)" because
//    WrapErr*Msg formats with fmt.Sprintf:
return merr.WrapErrServiceInternalMsg("compact failed: %s", err)
return merr.WrapErrServiceInternalMsg("compact failed: %w", err)

// ✅ cause in the chain (deliberate relabel to ServiceInternal):
return merr.WrapErrServiceInternalErr(err, "compact failed")
// ✅ or, if the inner code should survive (Pattern 3):
return merr.Wrap(err, "compact failed")
```

Symptoms of the broken form: `errors.Is(outer, innerSentinel)` returns false,
typed inner codes vanish from the wire, retriability resets, and the audit
trail in `Status.Reason` still *looks* fine — which is why this one survives
review so often.

## Pattern 5 — InputError aborts `retry.Do`: scan before you mark

`retry.Do` aborts immediately on any InputError (and the proxy lb_policy stops
cross-replica failover). Before adding `WithErrorType(InputError)` to a
sentinel — or marking a code at some boundary — grep for files that both
produce/consume that code **and** sit inside a `retry.Do` loop. A transient
"not ready yet" condition must never ride an InputError-marked code.

Real case: `CheckAllQnReady` is polled inside `retry.Do` during
CreateCollection (waiting for query nodes to sync file resources — a transient
condition). Had its error used an InputError-marked code, the retry loop would
have aborted on the first attempt:

```go
// internal/coordinator/file_resource_observer.go — transient wait condition,
// expressed with a retriable system code on purpose:
err = merr.WrapErrServiceUnavailableMsg("file resource not synced, node-%d", nodeID)

// internal/rootcoord/create_collection_task.go — the consumer (condensed):
err := retry.Do(ctx, func() error {
    ...
    return t.fileResourceObserver.CheckAllQnReady()
}, retry.Attempts(10), retry.Sleep(3*time.Second))
```

For transient conditions use `ErrServiceUnavailable` / `ErrServiceNotReady` /
`ErrServiceResourceInsufficient` — never a code that is (or may become)
InputError-marked.

**The dual-identity solution.** When the *same* code is user-blame at the API
boundary but a transient internal condition elsewhere
(`ErrCollectionNotFound`, `ErrDatabaseNotFound`, `ErrPartitionNotFound`,
`ErrAliasNotFound`, `ErrFieldNotFound`), the sentinel stays SystemError and
the boundary stamps it — only where the name came from the user:

```go
// internal/proxy/meta_cache.go — the central chokepoint for user-supplied names:
return collection, merr.WrapErrAsInputErrorWhen(err,
    merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
```

`WrapErrAsInputError(When)` relabels the classification only; code, message and
`errors.Is` chain are untouched. Internal `retry.Do` paths (datacoord
handler/recovery refreshing a possibly-stale cache) keep retrying through the
unmarked sentinel.

## Pattern 6 — Converting an `errors.Is` control-flow sentinel to merr

`merr.Is` matches by **numeric code alone**. A bare `errors.New` sentinel
matches by pointer identity. Convert a control-flow sentinel to a merr and
every `errors.Is(err, thatSentinel)` guard starts matching **all** errors that
share the code — silently.

Real near-miss (review P0): the `errIgnored*` idempotency family (defined in
`internal/rootcoord/meta_table.go` — `errIgnoredCreateCollection`,
`errIgnoredAlterCollection`, `errIgnoredDropPartition`, ...) is caught by
`errors.Is` guards in `root_coord.go` and translated to success. Had these
become merr errors (say, code 5), *any* ServiceInternal error in those paths
would have been swallowed into a fake success — a data-loss class bug.

```go
// ✅ correct as-is (meta_table.go): pointer-identity sentinel, caught
//    in-process, translated before any wire boundary (see the convention doc):
errIgnoredCreateCollection = errors.New("ignored create collection") // create collection with same schema, so it can be ignored.

// root_coord.go — the guard whose semantics a merr conversion would widen:
if errors.Is(err, errIgnoredCreateCollection) { ... }
```

**Before converting any sentinel to merr:** `grep -rn "errors.Is(.*thatName"`
— every hit is a guard whose semantics you are about to widen from "this exact
signal" to "any error with this code".

## Pattern 7 — Boundary conversions that look wrong but are contracts

Two standing examples; both carry code comments — read them before "fixing".

**knowhere ConfigInvalid (2006) → ParameterInvalid (1100).** At the
index-param validation boundary, knowhere reports *user-supplied* index params
as ConfigInvalid. Routing all C-status codes through the segcore system-error
table here broke ~25 e2e cases: SDKs assert code 1100 / "invalid parameter"
for bad index params. The boundary keeps the special case
(`internal/util/indexparamcheck/vector_index_checker.go`, `HandleCStatus`):

```go
const knowhereConfigInvalid = 2006
if int32(status.error_code) == knowhereConfigInvalid {
    return merr.WrapErrParameterInvalidMsg("%s", errorMsg)
}
return merr.SegcoreError(int32(status.error_code), errorMsg) // system blame, code preserved in message
```

This is the *reverse* of Pattern 1: here the C++ side really is validating
user input, so collapsing it into the system table would have been the
misclassification.

**segcore pass-through codes collapse on the wire.** Most C++ codes are
deliberately projected to 2000 (`ErrSegcore`) on the wire, with the original
code preserved in `Reason` as `segcoreCode=`. This includes C++ codes 2001 and
2002; they do **not** map to the similarly numbered Go sentinels. The dedicated
`segcoreCodeTable` mappings are: 2003 → `ErrSegcoreUnsupported` (wire 2001),
2033 → `ErrSegcorePretendFinished` (wire 2002, control-flow signal), 2037 →
`ErrSegcoreFollyOtherException`, 2038 → `ErrSegcoreFollyCancel`, 2039 →
`ErrSegcoreOutOfRange`, 2040 → `ErrSegcoreGCPNativeError`, 2046 →
`ErrCollectionSchemaVersionNotReady` (wire 110), and 2099 → `KnowhereError`.
Codes 2037, 2040, and 2046 are retriable. Don't "improve" a call site by
hand-picking a 20xx number — go through `merr.SegcoreError(code, msg)` and let
the table decide retriability and projection. Guard tests:
`pkg/util/merr/segcore_test.go` (`wire_code_projection`,
`TestSegcoreCodeTableCoverage`).

---

## Choosing a code: quick reference for commonly confused sentinels

Full list: `pkg/util/merr/errors.go` (the init-time registry panics on
duplicate codes). The table covers the choices that actually get confused in
practice. "Input" = baked-in `WithErrorType(InputError)`; "boundary" = stamped
via `WrapErrAsInputError(When)` at the proxy chokepoint only.

| You want to express… | Use | Not | Why |
|---|---|---|---|
| request value is malformed / out of range | `ErrParameterInvalid` 1100 (Input) | `ErrServiceInternal` | user can fix it by changing the request |
| request lacks a required field | `ErrParameterMissing` 1101 (Input) | `ErrParameterInvalid` | both Input; Missing is the precise statement |
| an internal invariant / contract was violated | `ErrServiceInternal` 5 | `ErrParameterInvalid` | Patterns 1–2: nobody's request caused it |
| transient "not ready, try again shortly" | `ErrServiceUnavailable` 2 / `ErrServiceNotReady` 1 (retriable) | any not-found code | Pattern 5: must survive `retry.Do` |
| name lookup failed, name typed by user | `ErrCollectionNotFound` 100 etc. **+ boundary stamp** | marking the sentinel Input globally | dual identity, Pattern 5 |
| name lookup failed from internal state (id-based, cache refresh) | `ErrCollectionNotFound` 100 etc., unmarked | `ErrServiceInternal` | keeps refresh/retry paths alive; stays system-blame |
| user's import file/data is bad | `ErrImportFailed` 2100 (Input) | `ErrImportSysFailed` | 2101 is the server-side twin — pick by blame, not by stage |
| import broke for server-side reasons | `ErrImportSysFailed` 2101 | `ErrImportFailed` | same, reversed |
| bad search/query expression or plan | `ErrQueryPlan` 2201 (Input) | `ErrParameterInvalid` | dedicated code, SDKs branch on it |
| object-storage op failed (already typed by `mapObjectStorageError`) | `merr.Wrap(err, …)` | `WrapErrIoFailedErr` | Pattern 3: don't flatten Io taxonomy |
| C++ status crossing cgo | `merr.SegcoreError(code, msg)` | hand-picked 20xx sentinel | Pattern 7: the table owns projection/retriability |
| persisted meta / binlog is corrupt | `ErrDataIntegrity` 1009 | `ErrParameterInvalid` | stored state, not the current request |
| function/embedding pipeline contract broke | `WrapErrFunctionFailedMsg` 2400 | `ErrServiceInternal` | that subsystem's promised code |
| in-process control-flow signal (idempotent no-op, queue empty) | package-level `errors.New` sentinel, caught + translated | any merr | Pattern 6; see convention doc |

---

## For coding agents (and humans in a hurry)

Minimum procedure when a task touches error handling:

1. Read [error_handling_guide.md](./error_handling_guide.md) §"Decision tree"
   and §"Input vs System" — then this casebook's pattern list.
2. Apply the blame test: is the **request content itself** what forces this
   branch? → Input factory. A Milvus bug, or an internal/transient failure
   (e.g. the not-ready condition in Pattern 5, a TOCTOU race), → System
   factory — even when a correct Milvus does reach it on a valid request
   (those must stay SystemError so `retry.Do` keeps retrying).
3. Never use `WrapErrXxxErr`/`WrapErrXxxMsg("%s", err)` to add context —
   `merr.Wrap(f)` only (Patterns 3–4).
4. Before adding/marking InputError: grep `retry.Do` consumers (Pattern 5).
   Before converting a sentinel to merr: grep `errors.Is` guards (Pattern 6).
5. Don't invent codes, don't hand-edit boundary conversions with comments
   (Pattern 7), don't return bare `errors.New`/`fmt.Errorf` (the linter will
   reject it anyway).
6. Touched a wire projection, oldCode mapping, or metric label? Run the guard
   tests: `pkg/util/merr/error_classification_test.go` (closed-world Input
   set), `segcore_test.go` (wire projection), and a **full** `make test-go` —
   contract changes break packages you didn't touch.
