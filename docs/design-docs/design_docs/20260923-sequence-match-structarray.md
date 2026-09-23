# MEP: Sequence Match over a StructArray

- **Created:** 2026-09-23
- **Author(s):** @xiaofan-luan
- **Status:** Draft
- **Component:** Proxy, QueryNode, Segcore, SDK
- **Related Issues:** TBD
- **Released:** TBD

## Summary

Add `SEQUENCE_MATCH(...)` as a Boolean expression in the existing scalar `filter` language. Other filter terms first identify candidate parent rows; the operator then examines elements within each candidate row in a declared order, binds earlier elements to later predicates, and evaluates to true if at least one complete sequence exists. The MVP returns ordinary parent rows, not the matched elements. It does not introduce general SQL aliases, joins across rows, or a new storage type.

## Motivation

A video clip can contain an array of typed events. `MATCH_ANY(events, $[kind] == "collision")` can establish that one event exists, but separate quantified predicates cannot require that the cat in one event is the cat in a later path conflict, or that the vehicle in that conflict is the vehicle that subsequently turns and collides. They also cannot compare the timestamps of those particular events. This forces applications to retrieve many candidate clips and evaluate ordered relationships outside Milvus.

The existing StructArray design already stores typed sub-fields and preserves element positions in its nested scalar indexes. This proposal keeps inexpensive row and element predicates as candidate selectors, then performs the correlated, ordered check only within selected rows. It addresses exact curation filters such as “a cat enters, its path conflicts with a car, then that car turns within one second.”

## Public Interfaces

The MVP adds a Boolean `SEQUENCE_MATCH` operator to the existing `filter` expression. The grammar below is illustrative and requires parser and plan extensions. `ORDER_BY` and `STEP` are clauses local to the operator, not standalone filter functions. This is a query-time operator, not a schema `Function` that generates fields at ingest time.

```python
rows = client.query(
    collection_name="video_clips",
    filter='''camera_id == "cam-1"
        && PHRASE_MATCH(clip_caption, "cat near a car")
        && MATCH_ANY(events, $[kind] == "collision")
        && SEQUENCE_MATCH(events,
             ORDER_BY($[start_ms], $[event_id]),
             STEP($[kind] == "enter_view" && $[actor_type] == "cat"),
             STEP($[kind] == "path_conflict"
                  && $[actor_id] == @0[actor_id],
                  WITHIN($[start_ms], @0[start_ms], 0, 1000)),
             STEP($[kind] == "swerve_left"
                  && $[actor_id] == @1[target_id],
                  WITHIN($[start_ms], @1[start_ms], 0, 1000)),
             STEP($[kind] == "collision"
                  && $[actor_id] == @2[actor_id],
                  WITHIN($[start_ms], @2[end_ms], 0, 2000)))''',
    output_fields=["video_id", "uri"],
    limit=100,
)
```

`$[field]` denotes a sub-field of the element being tested by the current step. `@N[field]` denotes a sub-field of the element bound at zero-based step `N`. A step may refer only to earlier steps. Each step binds one distinct element. `steps` must contain between two and five entries in the MVP. The current element must sort strictly after the element bound by the previous step; unrelated elements may appear between them. The order is lexicographic over the declared keys. The MVP requires a numeric time key and a deterministic tie-break key; the example uses clip-relative millisecond timestamps and stable event IDs. Every step after the first requires `WITHIN(current_time, prior_time, min_ms, max_ms)`, which enforces `min_ms <= current_time - prior_time <= max_ms` with inclusive boundaries. It is the only cross-step time arithmetic in the MVP. No implicit time window or adjacency rule is inferred.

`SEQUENCE_MATCH` has the type Boolean at parent-row granularity. It can be combined with ordinary scalar, text, and `MATCH_ANY` terms using `&&` or `||`, with normal Boolean precedence and parentheses. The example's `&&` terms are necessary conditions for a returned row. Existing `output_fields`, `limit`, and pagination continue to count parent rows. No captures, offsets, event IDs, or generated match fields are included in the MVP response. A filter consisting only of `SEQUENCE_MATCH` considers all visible rows. This proposal does not change the meaning of existing `MATCH_ANY`, `TEXT_MATCH`, or `PHRASE_MATCH` expressions.

## Design Details

### Data and semantics

The input is one StructArray whose elements are typed events. Typical scalar sub-fields are `event_id`, `kind`, `actor_id`, `target_id`, `start_ms`, and `end_ms`. Time fields may be signed INT8, INT16, INT32, or INT64 and must use one domain within a row, such as milliseconds relative to the clip start. The executor widens them before subtraction to avoid overflow. `SEQUENCE_MATCH` operates within one parent row; two adjacent clip rows are never combined. Empty arrays cannot match. Elements with a null ordering time or tie key cannot participate in a match. Element positions may change after data replacement, so the MVP does not expose them as persistent identifiers.

The operator is existential: it succeeds if **any** ordered tuple of distinct array elements satisfies every step predicate. All eligible starting elements must be considered. In particular, failure of the earliest cat event cannot suppress a later cat event that completes a sequence. Predicates on a single step are evaluated against that same element; references such as `@1[target_id]` use the actual element selected by the partial match, not an arbitrary event in the parent row. For identical timestamps, the tie-break key determines order. If an input row nevertheless contains duplicate order-key tuples, the operator breaks the remaining tie by element offset for deterministic execution within that row, without giving the offset application-level meaning.

The MVP supports current-element scalar predicates against constants and direct equality or inequality between a current-element field and an earlier step's field, combined with Boolean conjunction/disjunction. Cross-step references must have compatible scalar types. The required `WITHIN` clause on subsequent steps compares two integer time sub-fields with constant bounds. It rejects cross-step ordered comparisons outside `WITHIN`, arithmetic within a step predicate, references to a future step, vector comparisons, aggregation, negated sequence patterns, repetition, optional steps, cross-row joins, and nested StructArrays. A phrase predicate in the top-level `filter` constrains the parent row, but does **not** bind its textual hit to a particular event; element-level phrase matching requires a separate design and index contract.

### Planning and execution

The filter planner treats `SEQUENCE_MATCH` as a Boolean expression node. For an `AND` conjunction, indexable scalar, phrase, and nested predicates can produce a candidate parent-row bitmap before sequence evaluation. A necessary precondition such as `MATCH_ANY(events, $[kind] == "collision")` may further reduce candidates, but the sequence operator never treats a parent-level hit as proof of a same-element or ordered relationship. User-provided preconditions are semantic constraints: an overly restrictive condition can exclude an otherwise valid sequence. The planner may infer additional **necessary** step-type predicates, but must not apply heuristic pruning that changes exact results. Under `OR` and `NOT`, the planner must preserve Boolean semantics and cannot simply intersect all sibling bitmaps; the sequence expression is evaluated for the rows required by the Boolean plan. Projection and limit apply after the complete filter expression is evaluated.

For each candidate row, the executor obtains the required StructArray sub-fields and an ordering of its elements. The first implementation requires resident raw child columns, including the first child that supplies array offsets and parent validity; it scans elements of candidate rows and evaluates constant-only portions of step predicates at element granularity. Existing nested-index postings preserve flattened element IDs and parent-row mapping, so a later optimization can use them to narrow step candidates without changing semantics. Correlated comparisons such as `$[actor_id] == @1[target_id]` are evaluated after earlier bindings exist. The executor advances partial bindings in time order, prunes them when explicit time bounds are exceeded, and stops after the first full match because the MVP only needs a Boolean answer. It must not stop after the first *starting element* fails.

The evaluator must preserve element identity until the full sequence is verified. Existing `MATCH_ANY` can reduce element hits to a parent-row bitmap immediately; doing so between sequence steps would conflate different cats, cars, or times. Final successful rows are merged into the ordinary parent-row result path. The MVP bounds candidate evaluations per parent row (initially one million); exceeding the budget fails the query explicitly rather than silently turning a valid row into a non-match. The implementation should expose candidate-row count, evaluated-element count, and partial-state count for query diagnostics.

### Search interaction

The MVP accepts `SEQUENCE_MATCH` in the `filter` of scalar `query()` only. A later `search()` integration may use the sequence result as an exact parent-row eligibility predicate before ANN ranking. Filtering only a fixed ANN top-K after retrieval would yield a candidate-limited answer and can miss eligible rows; it must not be presented as equivalent. Element-level vector hits require an additional contract tying the hit element to a chosen sequence step.

### Why no captures in the MVP

An existential parent-row filter can reuse the existing Query result shape. Returning all matching chains requires a new result model that groups step bindings into a chain, defines first/all-match ordering and truncation, and carries those groups through QueryNode, Proxy, protocol, and SDK. That is a separate follow-up. A possible shape is `sequence_matches: [{span_ms, captures: [{step, event_id, offset}, ...]}]`; it is **not** part of this MEP's public interface. Stable application references would use `event_id`, whereas an array offset is only valid for the returned row version.

## Compatibility, Deprecation, and Migration Plan

Filters without `SEQUENCE_MATCH` are unchanged. The new operator rejects unsupported array schemas or expressions at planning time. In the MVP, sealed segments must have resident raw data for the first StructArray child and every sub-field referenced by ordering, predicates, or windows. An index-only child is unsupported even if its scalar index supports reverse lookup: the first child's offsets and parent validity still require a resident column. The executor checks this before requesting array offsets and returns an explicit unsupported-query error. Collections retaining these raw columns need no data migration or index rebuild. Existing nested scalar indexes may accelerate parent prefilters, while element-level index acceleration is deferred. Users should keep long videos in bounded clip rows so that per-row ordering and partial-state memory remain predictable.

## Test Plan

System tests will insert clips with controlled event arrays and compare the query result with a reference evaluator. Fixtures must cover: a successful cat-to-car-to-collision chain; same event types spread across different actors; the first start failing while a later start succeeds; multiple possible cars; unrelated events between steps; strict order with equal timestamps and tie-break keys; inclusive time-window boundaries and one millisecond outside; empty and single-element arrays; and several parent rows where only some satisfy the top-level `filter`. A phrase match on `clip_caption` must not be interpreted as an element-level phrase hit or step binding.

Run the same fixtures against growing and sealed segments and with and without nested scalar indexes, retaining resident raw values in both paths. Verify that dropping either a referenced sub-field or the first StructArray child from a sealed segment returns an explicit unsupported-query error before array-offset lookup, even if an index remains loaded. Verify `AND`/`OR`/`NOT` combinations preserve normal filter semantics, query limit and pagination count parent rows, the scan and index paths return identical results, and invalid field references or incompatible types produce errors. Add a stress fixture with many repeated event types to measure partial-state growth, pruning, and first-success short-circuit without relying on a silent match cap. Regression coverage must establish unchanged behavior for ordinary scalar filters, `MATCH_ANY`, text filters, and queries without `SEQUENCE_MATCH`.

## Rejected Alternatives

- **A standalone `AS` alias feature.** Naming an element does not retain its binding through evaluation; the sequence operator needs positional binding state first.
- **Several independent `MATCH_ANY` predicates.** They can find a common parent row but cannot prove order, time difference, or actor identity across selected elements.
- **Only application-side sequence evaluation.** It is useful as a prototype, but requires transferring full event arrays for many rejected rows and duplicates evaluation logic in clients.
- **Returning all matching chains in the MVP.** It expands the result and pagination contract before the existential predicate and its execution cost are validated.

## References

- [Struct Data Type](20260306-struct.md)
- [Milvus in-repository design docs](../README.md)
