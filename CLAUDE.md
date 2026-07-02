# Milvus

Vector database. Go + C++ (internal/core/) + Rust (tantivy).
pkg has its own go.mod (module: `github.com/milvus-io/milvus/pkg/v3`). Run `go get` from `pkg/` when adding dependencies there, not from root.

## Architecture

Coordinators manage metadata and scheduling; nodes execute work.
- Coordinators: rootcoord, datacoord, querycoordv2 (note the v2 suffix in directory names)
- Nodes: proxy (user-facing), querynodev2, datanode, streamingnode
- All component interfaces defined in `internal/types/types.go`

## Subsystems & Code Map

Each subsystem has a **top-level doc** (overview with links to sub-documents) and multiple **sub-documents** (detailed design, invariants, interfaces). The top-level doc alone is NOT sufficient — it is an index, not the content.

### Mandatory reading procedure

When your task modifies, explains, depends on, or affects a subsystem below, execute these steps IN ORDER before responding or writing code:

**Step 1 — Read the top-level doc.** Identify all sub-documents it links to.

**Step 2 — Read sub-documents.** The scope depends on task type:
- **Design tasks** (new feature, architecture change, cross-component change): Read **every** sub-document under the subsystem. No exceptions — design requires full-picture understanding. Do NOT judge relevance yourself; read all of them.
- **Targeted tasks** (bug fix, single-component change, code explanation): Read sub-documents that cover the components your task touches. When uncertain whether a sub-document is relevant, read it.

**Step 3 — Read source code** listed in each doc's "Key Packages" section. At minimum read the files directly related to your task.

**Step 4 — Cross-check** documentation against code. If they contradict, STOP and ask the user to resolve before proceeding.

NEVER answer based on documentation alone or code alone. NEVER skip Step 2 — this is the most common failure mode.

### Subsystems Reference

- [**Observability**](docs/agent_guides/observability/README.md): Logging, metrics, tracing, gRPC access logs, and observability debug workflows.
- [**Streaming System**](docs/agent_guides/streaming-system/streaming-system.md): Write path, WAL, DDL/DCL execution, replication && CDC.

## Testing

Go tests MUST use `-tags dynamic,test` and `-gcflags="all=-N -l"` (disable optimizations/inlining) or they won't compile / mockey-based monkey patching will fail:

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/querycoordv2/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/proxy/... -run TestXxx
```

Per-module shortcuts: `make test-querycoord`, `make test-proxy`, etc.

## Verification gate (MANDATORY before claiming "done" or pushing for review)

The reading procedure above tells you how to *enter* the code. This tells you how to *prove a change works*. A change is NOT verified by "it compiles + unit tests pass + success-path e2e is green." When the goal of a change is a **behavior** (retry / classification / routing / error-code propagation / fallback / cache invalidation / concurrency), execute these IN ORDER. Skipping them is the most expensive failure mode — it ships changes that are self-consistent but miss their purpose.

**G1 — Verify the data, not just your transform.** If your change adds a function/layer that maps or preserves a value X (an error code, a Status, a category, a flag), you only verified *your function*. Now verify its INPUT. Audit **every** place that constructs, throws, or rewrites X across the **whole repo** — not only the lines you edited. grep the escape hatches: `throw`, `.ToString()`/stringify, blanket fallbacks (catch-all → `Invalid` / `IOError` / `UnexpectedError`). A value destroyed or mis-set upstream makes your boundary logic dead code. Audit at the SOURCE of X, never only at the boundary that consumes it.

**G2 — Trace each real failure mode end-to-end.** Success-path e2e — even thousands of cases — does NOT exercise the failure modes a behavioral change exists for (S3 throttle, corrupt file, OOM, cancel, timeout, not-ready). For EACH one: either trace it by hand from origin → consumer, or fault-inject it, and confirm it lands in the intended bucket. "All green" on the happy path is not evidence the change works; it is only evidence you did not break the happy path.

**G3 — Do not over-claim.** Commit messages and PR body may assert ONLY benefits verified end-to-end via G1+G2. A benefit that depends on un-audited upstream or an un-triggered failure mode must be written as "follow-up" or "preserves codes for observability; retry wiring unverified" — never as achieved. A reviewer will verify your claim against the running system; over-claiming wastes their round.

**G4 — Adversarial self-review before human review.** Before pushing, do one pass asking: which failure mode have I NOT traced to its bucket? which upstream construction site of X have I NOT read? what would an adversarial reviewer grep for? Fix the gaps, or list them explicitly in the PR.

## Run Milvus Locally

```bash
scripts/start_standalone.sh    # start standalone mode
scripts/start_cluster.sh       # start cluster mode
scripts/stop_graceful.sh       # stop
scripts/standalone_embed.sh    # embedded standalone (no external deps)
```

## Code Conventions

- Error handling: use `merr` package, not fmt.Errorf — see mandatory procedure below
- Logging: use `github.com/milvus-io/milvus/pkg/v3/mlog` only; do not use `pkg/log`, standard `"log"`, direct `zap`, or `fmt.Println`. Every log call must pass a real `ctx` by priority: function parameter ctx > struct ctx > `context.TODO()`. Refer to [logging.md](docs/agent_guides/observability/logging.md).
- Import order: standard → third-party → github.com/milvus-io (enforced by gci)
- Config params: paramtable (`pkg/v2/util/paramtable`), config in `configs/milvus.yaml`

### Error handling (mandatory when originating, wrapping, or classifying errors)

Read [error_handling_guide.md](docs/dev/error_handling_guide.md) (decision tree,
Input-vs-System) and [error_handling_casebook.md](docs/dev/error_handling_casebook.md)
(the 7 mistake patterns) BEFORE writing the change. Non-negotiable rules:

1. Blame test: is the **request content itself** what forces this branch? → Input factory. A Milvus bug, or an internal/transient failure (not-ready, TOCTOU race), → System factory — even when a correct Milvus does reach it on a valid request (transient errors are System, and must stay retriable). "Looks like validation" is not the test.
2. Add context to an existing error with `merr.Wrap/Wrapf` ONLY — `WrapErrXxxErr(err, …)` masks the inner code; cause never goes into a format string.
3. Before marking anything InputError: grep `retry.Do` consumers. Before converting an `errors.New` sentinel to merr: grep `errors.Is` guards.
4. Pick codes from the existing family ranges in `pkg/util/merr/errors.go` (scan first; see the partition table in [error_sentinel_convention.md](docs/dev/error_sentinel_convention.md)); never hand-pick 20xx segcore codes.
5. Touched a wire projection, oldCode mapping, or metric label? Run the merr guard tests AND a full `make test-go` — contract changes break packages you didn't touch.
6. **C++ side (segcore / milvus-storage / cgo boundary) — the rules above are Go/merr; the same discipline applies in C++, and the verification gate (G1) is non-negotiable here.** The final class is decided at the `ThrowInfo` / `AssertInfo` / `SegcoreError` / arrow-`Status` / `LOON_*` **construction sites**, NOT at the cgo boundary translator. A boundary helper (`KnowhereStatusToErrorCode` / `ArrowStatusToErrorCode` / `LoonResultToErrorCode`) is correct ONLY if its inputs carry the right category — so audit upstream, not the helper. When a code must survive to the cgo boundary, grep every construction/throw/rewrite site and confirm none collapse it: `FailureCStatus` needs a real `SegcoreError` (an `ExecOperatorException` or `throw std::runtime_error(status.ToString())` destroys the code), and an upstream `IOError`-rewritten-as-`Invalid` (or catch-all → `IOError`) silently inverts transient vs permanent. Apply the blame test (rule 1) at EVERY such site, not only the ones you edit.

## PR and Commit Conventions

PR title format: `{type}: {description}`. Valid types: `feat:`, `fix:`, `enhance:`, `test:`, `doc:`, `auto:`, `build(deps):`.
PR body must be non-empty. Issue/doc linking rules:
- `fix:` — must link issue (e.g. `issue: #123`)
- `feat:` — must link issue + design doc under `docs/design-docs`
- Every Milvus feature should have a related design doc under `docs/design-docs`; submit the doc in this repository and link it from the Milvus feature PR.
- `enhance:` — must link issue if size L/XL/XXL
- `doc:`, `test:` — no issue required
- 2.x branch PRs must link the corresponding master PR (e.g. `pr: #123`)

DCO check is required. Always use `-s` so the developer's Signed-off-by is appended last:

```
git commit -s -m "commit message

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

`-s` auto-appends `Signed-off-by: <developer>` at the end. The developer MUST be the final sign-off, not the AI.

## Generated Files — Do Not Hand-Edit

- Mock files (`internal/mocks/*`, `mock_*.go`): regenerate with `make generate-mockery-{module}`
- Proto files (`pkg/proto/*.pb.go`): regenerate with `make generated-proto-without-cpp`
