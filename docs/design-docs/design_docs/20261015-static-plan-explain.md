# MEP: Static plan explainer

- **Created:** 2026-10-15
- **Author(s):** @joshbahk
- **Status:** Draft
- **Component:** Proxy
- **Related Issues:** #53829 (this design), #53830 (API and CLI exposure, follow-up)
- **Released:** n/a

## Summary

`ExplainPlan` in `internal/parser/planparserv2` describes an already
constructed `planpb.PlanNode` without executing it: a versioned JSON document
(plan type, search or query parameters, a predicate tree with one node per
`Expr` kind, output fields, scorers, function chains, plan options, and
caller-supplied candidate indexes) plus a text view for people and logs. This
document is the output contract that #53829 asks to agree on. Version 1 is a
Go function only; exposing it through an API or CLI is #53830.

## Motivation

The only way to see what an expression became is the debug printer
`ShowExprVisitor` (12 of the 22 `Expr` kinds; an unrecognized kind renders as
`""`) or reading the proto by hand. A user cannot tell, without running the
search, how the rewriter reshaped a filter (`f == {a} || f == {b}` becomes one
`term`; `AND` operands may be reordered), whether a numeric constant is integer
or floating point on a JSON field (the two are different plans), or which
indexes could serve a predicate. The issue's acceptance criteria: the
explanation agrees with the constructed plan, never executes retrieval, and
represents unknown nodes rather than omitting them.

## Public Interfaces

Go only; no proto, SDK, config, metric, or segcore change.

```go
func ExplainPlan(plan *planpb.PlanNode, schema *typeutil.SchemaHelper, opts *ExplainOptions) (*PlanExplanation, error)
func (p *PlanExplanation) JSON() ([]byte, error) // versioned contract
func (p *PlanExplanation) Text() string          // not covered by the version

type ExplainOptions struct {
    RedactTemplateValues bool                       // RedactPlanForLog's rule and markers, plus a templated GIS WKT
    FieldIndexes         map[int64][]IndexCandidate // the plan carries no index metadata
}
type IndexCandidate struct {
    Name, Type     string
    Params         map[string]string
    NestedPath     []string // nil: every column of the field; empty: the root value
    CoversSubpaths bool     // a flat JSON index also serves the paths below NestedPath
}
const ExplainVersion = 1
```

JSON contract, version 1. The full key set with each key's presence rule is
pinned by `TestExplainPlan_ContractKeys`; the shape is:

- Top level: `version`, `plan_type` (`vector_search`, `query`, or `unknown`;
  the deprecated bare-predicate `PlanNode` explains as `query`),
  `vector_search`, `query`, `predicates`, `output_fields`, `dynamic_fields`,
  `scorers`, `unknown_node_count`; `score_option`, `function_chains`,
  `options`, `namespace`, `detail` when present.
- `vector_search`: `field`, `vector_type`, `metric_type`, `topk`,
  `search_params` (decoded with `UseNumber`, kept verbatim when not JSON),
  `round_decimal`, `group_by`, `iterator`; `json_path`, `hints` when present.
- `query`: `is_count`, `limit`; `group_by`, `aggregates`, `order_by`,
  `cursor` when present.
- Predicate node: `node` is one of the 22 `Expr` kinds (`term`, `unary`,
  `binary`, `compare`, `unary_range`, `binary_range`, `arith_compare`,
  `arith`, `value`, `column`, `exists`, `always_true`, `json_contains`,
  `call`, `null`, `random_sample`, `gis`, `timestamptz_arith_compare`,
  `element_filter`, `match`, `bloom_filter`, `roaring_filter`) or `unknown`;
  every other key applies to some kinds only: `op` (proto enum name), `field`,
  `right_field`, `value`, `values`, `extra_values`, `lower`, `upper`,
  `operand`, `arith_op`, `left`, `right`, `child`, `children`, `element`,
  `function`, `struct_name`, `in_field`, `template`, `operand_template`,
  `wkt`, `distance`, `count`, `sample_factor`, `interval`, `blob`, `detail`.
- Field reference: `id`; `name`, `type`, `element_type`, `nested_path`,
  `primary_key`, `auto_id`, `partition_key`, `clustering_key`, `nullable`,
  `element_level`, `candidate_indexes` when they apply.

Rules:

- `version` changes only when an existing key is renamed, removed, or changes
  type. Adding a key keeps the version; consumers ignore keys they do not know.
- Keys that always apply are emitted even when null (`vector_search`, `query`,
  `predicates`, `search_params`, `group_by`, `iterator`, `iterator.last_bound`,
  `scorers[].filter`, and a range bound's `value`). The list keys `values` and
  `children` are present whenever the node kind has a list, an empty one
  included, so `f in []` reads `"values": []`.
- Floats always carry a decimal point or an exponent (`2.0`, `1e+21`), so an
  integral float is never read as an integer. NaN and ±Inf are the object
  `{"non_finite": "NaN" | "+Inf" | "-Inf"}`.
- Membership-filter blobs are summarized as `{"bytes": N}`; the search
  iterator token is never reproduced.

## Design Details

- Placement: `internal/parser/planparserv2`, next to the parser, the rewriter
  and `plan_redact.go`, since the explainer needs the same `planpb` knowledge
  and reuses the package's redaction rule.
- Completeness: one `case` per `Expr` oneof member. A protoreflect test walks
  the oneof, so a member added to `plan.proto` fails
  `TestExplainPlan_CoversEveryExprType` until the explainer covers it.
  Anything unrecognized (an unset `Expr`, a missing child, an unknown plan
  node kind, an unknown `GenericValue` kind) becomes an `unknown` node with a
  `detail` and increments `unknown_node_count`. The three children the parser
  leaves nil on purpose (the predicate of a bare `random_sample` or
  `element_filter`, the right operand of `array_length`) are not unknown.
- Fidelity to what segcore evaluates: fields resolve through the supplied
  `SchemaHelper` for names, but the plan's own `ColumnInfo` type and flags
  win, and a field the schema does not know is reported by id. Presence is
  kept where segcore keys on presence: `namespace` is emitted exactly when the
  plan carries one (an empty string included); the JSON grouping options are
  reported whenever the plan carries a path, a type, or `strict_cast`; zero
  thresholds and empty lists stay visible; numeric kinds and float widths are
  preserved.
- Candidate indexes: the plan carries no index metadata, so the caller
  supplies `FieldIndexes` (the proxy has the collection's index descriptions).
  They attach to the vector field and to predicate columns, not output fields.
  A candidate with a `NestedPath` attaches only to columns reading exactly
  that path, since a typed JSON path index serves one path; with
  `CoversSubpaths` it also attaches to the paths below, which is how the query
  node routes a flat JSON index (`json_cast_type` `JSON`;
  `exec/expression/IndexPathSelection.h`). `NestedPath` is in the plan's
  `ColumnInfo` form; the caller derives it from the index's `json_path`
  parameter, which datacoord stores as an escaped JSON pointer
  (`internal/util/typeutil.ParseAndVerifyNestedPath`, a package that imports
  the parser, so the explainer cannot parse it itself), and sets
  `CoversSubpaths` from the cast type. They are candidates: whether segcore
  uses one depends on segment state at execution time, and the list does not
  reproduce its finer routing checks. The explanation owns copies of them.
- Redaction: `RedactTemplateValues` elides template-supplied values with the
  rule and markers of `RedactPlanForLog`, plus a templated GIS WKT, which that
  rule misses. Known limit, shared with `RedactPlanForLog`: the rewriter runs
  after template filling and rebuilds some nodes without template provenance.
  Term normalization (`rewriter/json_term.go`) does so whatever
  `common.enabledOptimizeExpr` says, for an ARRAY field's `in` list of arrays
  and for a JSON `in` list that holds arrays or mixes kinds; with the option
  on, the optimizer does so for `not (f == {t})`, a single-element `in`, `OR`
  and range merges and `IN` merges. Those values explain as literals.
  Carrying provenance through the rewriter is a separate change.
- Function chains are summarized by name, stage and operation names; the
  expressions inside operations are out of scope for version 1. Scalars
  outside the contract (`materialized_view_involved`, `bm25_avgdl`,
  `query_field_id`, `search_topk_ratio`, `refine_topk_ratio`,
  `search_iterator_v2_info.token`, `placeholder_tag`, and the
  `elements_same_type` hint of `json_contains`, derived from the values the
  node already reports) are listed in the `PlanExplanation` doc comment;
  `unknown_node_count` does not cover them.
- Failure cases: a nil plan returns an error; `JSON()` cannot fail on
  non-finite floats; the plan is never mutated (`proto.Equal` before and after
  is asserted).

## Compatibility, Deprecation, and Migration Plan

Additive. No existing code path changes; `ShowExprVisitor` is untouched. No
upgrade, rollback, data-format, or wire impact. Contract evolution follows the
version rule above; #53830 will expose the JSON through an API and inherits
the same rule.

## Test Plan

Unit tests in `explain_test.go`, run with `-tags dynamic,test`: a byte-exact
pin of one search plan (key order included); the reflective key pin; the
protoreflect walk of the `Expr` oneof; one parser-produced plan per node kind;
nil-by-design children; zero thresholds; non-finite floats as a predicate
value, scorer weight, sample factor, GIS distance and iterator bound; integer
versus float kinds; empty lists; filled,
unfilled and redacted templates, GIS and binary range included; redaction
parity with `RedactPlanForLog` across eight rewriter shapes with the optimizer
on and off; candidate indexes including JSON path scoping and copy semantics;
column flags; search and query options in JSON and text; function chains;
read-only via `proto.Equal`; a runnable `ExampleExplainPlan`. No end-to-end
test in this change, since it adds no API surface; #53830 carries those.

## Rejected Alternatives

- Extending `ShowExprVisitor`: a debug printer with no versioning, no
  redaction, no unknown-node handling, and output that existing log consumers
  may depend on.
- Emitting the plan with `protojson`: exposes proto field names as the
  contract, carries blobs of tens of MiB, the iterator token and template
  values, and cannot attach index candidates or mark unknown nodes.
- Resolving fields from the schema alone: a plan built against another schema
  version would not explain, and segcore evaluates the plan's `ColumnInfo`.
- Looking up indexes inside the explainer: the parser package has no access
  to index metadata; the caller does, and the function stays pure.
- Non-finite floats as bare strings: a JSON field compared with the number
  NaN and one compared with the string `"NaN"` are different plans and would
  explain identically.
- Fixing rewriter provenance here: a rewriter change with a design decision
  for merged nodes (one template name per node); proposed separately.

## References

- #53829, #53830
- `internal/parser/planparserv2/explain.go`, `explain_test.go`,
  `show_visitor.go`, `plan_redact.go`, `rewriter/`
- `pkg/proto/plan.proto`
