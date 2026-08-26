# Unified Membership-Filter Expression: `membership_match`

- **Issue**: [#52777](https://github.com/milvus-io/milvus/issues/52777)
- **Status**: Draft
- **Date**: 2026-08-22
- **Previous design docs**:
  - [20260707-bloom-filter-expression.md](./20260707-bloom-filter-expression.md) — `bloom_match` (approximate)
  - [20260714-roaring-exact-membership-expression.md](./20260714-roaring-exact-membership-expression.md) — `roaring_match` (exact)

## TL;DR

Add a unified surface syntax

```
membership_match(<field>, {<blob-bytes-template>})
membership_match(<field>, {<blob-bytes-template>}, type=bloom|roaring)
not membership_match(<field>, {<blob-bytes-template>})
```

whose filter kind is derived from the blob's self-describing magic header:
`MBF1` lowers to the existing `BloomFilterExpr` plan node (approximate), `MRB1`
to the existing `RoaringFilterExpr` node (exact). The old `bloom_match` and
`roaring_match` names are intentionally not retained because neither has shipped
in a release. The wire protocol does not change: plans carrying membership
filters are byte-compatible with today's.

The second half of this proposal is internal: the parser, proxy guards, and
segcore execution of both kinds are merged into one parameterized chain so a
future third membership structure (e.g. cuckoo/xor) is one registration, not a
fork.

## Motivation

`bloom_match` and `roaring_match` were designed and landed separately. They
share ~90% of their machinery — surface parsing shape, deferred-call fill,
per-request size budgeting, delete/element-level guards, log redaction, and an
almost line-for-line identical segcore executor — but each copy lives in its
own file with its own naming. Consequences:

1. Every cross-cutting fix (a new container expression in `walkExpr`, a new
   guard site, redaction coverage) must be applied twice and can silently miss
   one kind.
2. The two copies already disagree in places where they claim to mirror the
   same reference implementation (see "Divergences resolved" below).
3. Adding a third membership structure would triple the surface.

Unifying control flow while keeping each kind's *data plane* (envelope format,
hashing/probing algorithm, error model) separate removes the duplication
without touching what genuinely differs.

## Surface syntax and typing

| | `membership_match` |
|---|---|
| Kind | dynamic (blob magic), optionally pinned by `type=` |
| Blob format | sniffed: MBF1 → bloom, MRB1 → roaring |
| Fields | enforced per resolved kind |
| Delete | per resolved kind |

The optional `type=` argument improves readability in expressions and logs. It
must be `bloom` or `roaring` and must agree with the blob magic; a mismatch is a
request error. When omitted, both envelopes remain self-describing and are
sniffed at fill time. Unknown or too-short headers fail closed.

### Compatibility

* `membership_match` is the only supported surface name. Since the predecessor
  expressions have not shipped in a release, no alias or deprecation period is
  retained.
* Wire format unchanged: the unified syntax lowers to the existing plan nodes
  (`BloomFilterExpr`, oneof field 22; `RoaringFilterExpr`, field 23). Rolling
  upgrade behaves exactly as documented in the two predecessor MEPs: old QNs
  reject plans containing those fields via their existing default branch;
  proxies must be upgraded first as before.

## Parser and proxy changes (Go)

All in `internal/parser/planparserv2/`:

1. **One spec table** (`membership_filter.go`): the unified function → kind +
   properties (`allowInDelete`). Behavior switches read the table; format
   validation stays in `bloom_match.go` (MBF1 envelope + value-domain check)
   and `roaring_match.go` (MRB1 body walk + decoded-size estimate).
2. **Soft-keyword option parsing**: the ordinary two-argument call and the
   `type=bloom|roaring` form both emit the same deferred `CallExpr`. `type` and
   `membership_match` remain legal field identifiers outside that call shape.
3. **One fill path** (`fillMembershipMatchExpressionValue`): strict
   parameter-shape validation (adopting roaring_match's guarded form — the old
   bloom fill indexed call parameters unguarded), template resolution, kind
   resolution from magic plus optional type-consistency check, per-kind admission gate, materialization
   into `BloomFilterExpr`/`RoaringFilterExpr`.
4. **Tree tools merged**: `hasMembershipFilterExpr`,
   `hasDeleteUnsafeMembershipFilterExpr`, `collectMembershipFilterExprs`,
   `PlanContainsMembershipFilter`,
   `PlanContainsMembershipFilterUnsafeForDelete`. One walker
   (`walkExpr`) serves all of them plus redaction.
5. **Redaction moved out of `bloom_match.go`** into `plan_redact.go`,
   driven by kind-agnostic blob slots, so the roaring feature no longer
   depends on identifiers declared in the bloom file.
6. **Preflight charges bodies, not whole blobs**
   (`fill_expression_value.go`). This fixes a real bug in the interim state:
   the aggregate preflight charged the full blob length while the per-blob gate
   allowed the fixed 32-byte header on top, so a maximum-sized SBBF body
   (64 MiB + 32 B) was rejected before materialization — halving the usable
   tier, the exact bug the original design warned against. Preflight now sniffs
   the kind and subtracts the envelope header, mirroring the per-blob gate.
7. **Proxy config**: the old kind-specific names converge while preserving two
   independent resource limits. `proxy.maxMembershipFilterSize` (default
   64 MiB) limits one blob body and falls back to the released
   `proxy.maxBloomFilterSize` key (plus the development-only
   `proxy.maxRoaringFilterSize` predecessor). The separate
   `proxy.maxMembershipFilterPlanSize` (default 128 MiB) limits aggregate
   serialized membership-bearing plans for one request and falls back to
   `proxy.maxBloomFilterPlanSize`. The parser uses that same aggregate ceiling
   for a conservative body-occurrence preflight shared by all HybridSearch
   sub-requests and scorer filters; exact `proto.Size` accounting remains the
   final gate. Keeping the two dimensions separate prevents a legacy plan
   setting from silently widening the per-blob admission limit. The fixed MRB1
   admissions (262,144 high containers, 64 MiB decoded estimate per occurrence,
   aggregate) are unchanged.

### Guards unified

* **Delete**: rejected iff the plan contains a kind that cannot be proven exact
  (`BloomFilterExpr`, deferred `membership_match`
  whose blob is not yet sniffed). Exact kinds pass.
* **element_filter**: all membership kinds rejected inside element expressions
  (row-offset executors vs global element IDs).
* **MATCH_\***: tightened from bloom-only to all kinds. Previously a
  `roaring_match` inside a MATCH_* element predicate was not syntactically
  rejected; every MATCH_* predicate field is element-level, so its executor
  supplies element IDs where the row-offset prober expects segment offsets —
  the same reason `roaring_match` is rejected inside `element_filter`. The gap
  was unreachable in practice (integer element fields fail the roaring field
  check via nested paths) but the guard now states and enforces the invariant.

## Execution changes (C++ segcore)

New `exec/expression/MembershipFilterExpr.{h,cpp}`:

```cpp
template <typename LogicalExpr, typename ProbePolicy>
class PhyMembershipFilterExpr : public SegmentExpr { ... };

struct BloomMembershipProbe { /* SplitBlockBloomFilterView */ };
struct RoaringMembershipProbe { /* shared RoaringMembership* */ };

using PhyBloomFilterExpr  = PhyMembershipFilterExpr<expr::BloomFilterExpr, BloomMembershipProbe>;
using PhyRoaringFilterExpr = PhyMembershipFilterExpr<expr::RoaringFilterExpr, RoaringMembershipProbe>;
```

All control flow lives once in the template: exec-path selection
(raw-data preferred, index-only reverse-lookup fallback), batched execution,
cacheability (`IsCacheable() == false`), reorder-tier behavior, JSON probing
(bloom-only, discarded at instantiation for roaring via `if constexpr` on the
policy). Each kind's data plane stays in its probe policy: MBF1 zero-copy view
with domain gating vs decode-once portable Roaring64. The C++ type aliases
preserve the historical class names, so factory construction is unchanged.

### Divergences resolved

The two former implementations disagreed on three points; each was resolved
against a verified ground truth rather than by taste:

1. **NULL vs candidate-mask order**. `bloom_match` checked NULL before the
   candidate mask on its original raw path; `roaring_match` checked the mask
   first, pinned by tests demanding raw≡index bit-identity. While this work
   was in flight, upstream standardized on the untouched contract and added
   explicit pins for bloom as well
   (`ScalarBitmapInputLeavesExcludedNullCandidatesUntouched`,
   `JsonBitmapInputLeavesExcludedNullCandidatesUntouched`,
   `IndexOnlyBitmapInputPrunesReverseLookupsByCandidatePosition`). The unified
   chain therefore adopts mask-first everywhere: excluded candidates keep
   their initial `(false, valid)` regardless of nullness, identical to how
   the WithMask index-path helpers leave them, so one query returns the same
   column whichever way the segment is loaded. A probed NULL row never
   matches under either polarity (`res = valid = false`), which is where the
   three-valued promise lives.
2. **Index-fallback plumbing**. `bloom_match` used the unmasked
   `ProcessDataByOffsets`/`ProcessIndexLookupByOffsets`; `roaring_match` used
   the `...WithMask` variants. Unified on the WithMask variants: their empty-
   mask degenerate case is identical to the unmasked helpers (verified in
   `Expr.h`: `has_candidate_mask = mask != nullptr && !mask->empty()`), so one
   code path serves offset-input and plain batches.
3. **bitmap_input size assertion**. `roaring_match` asserted
   `bitmap_input.size == real_batch_size`; `bloom_match` did not. Kept the
   assertion for both: a disagreeing size reads past the bitmap end silently.

## What deliberately does NOT change

* The MBF1 and MRB1 formats, their golden vectors, and all four SDK builders
  (Go, pymilvus, C++, Java) — blobs built yesterday remain valid forever when
  supplied to `membership_match`.
* The plan proto: fields 22/23 frozen; no new message.
* Per-kind semantics: false-positive model, NULL/three-valued logic, JSON
  strict typing, signed-int key mapping, empty-set edge cases.
* The fixed MRB1 admissions and the MBF1 128 MiB format cap.

## Testing

* `internal/parser/planparserv2/membership_match_test.go` (new): MBF1→bloom /
  MRB1→roaring lowering, explicit type/magic consistency, soft-keyword field
  compatibility, privacy-safe
  unknown-magic failure, kind-specific field-domain enforcement at fill time,
  delete-safety classification including the fail-closed deferred case,
  element_filter/MATCH_* rejections, shared preflight budget across the unified
  name.
* Existing `bloom_match_test.go` / `roaring_match_test.go` migrated to the
  unified predicates and body-basis budgets; all prior assertions preserved.
* `internal/proxy/membership_filter_plan_size_test.go`: the exact serialized
  plan gate and the parser preflight budget are both shared across HybridSearch
  sub-requests.
* `pkg/util/paramtable`: fallback-key precedence tests, including a regression
  pin that the legacy plan key cannot widen the per-blob limit.
* segcore: existing bloom/roaring expression unit tests compile against the
  unified physical classes; golden-vector conformance tests
  untouched.

## Future work

* Third membership structures (cuckoo/xor): add an envelope + probe policy +
  one spec-table row.
* SDK-side sugar: builders may offer `membership_match` templates; not required
  because server-side sniffing makes any existing builder blob usable.
