# Design Document: Segment Reopen Atomic Read & Update with Copy-on-Write

**Date**: 2026-06-27
**Component**: Segcore / QueryNode
**Scope**: `ChunkedSegmentSealedImpl.{h,cpp}`

---

## 1. Overview

### 1.1 Motivation

`ChunkedSegmentSealedImpl` is responsible for a large amount of “serve queries while updating runtime state” behavior during the lifetime of a sealed segment, including but not limited to:

- field data / column group loading and replacement
- scalar / vector / text / json index state updates
- runtime state transitions during reopen / apply load diff
- default-value fields, array offsets, auxiliary indexes, and derived runtime structures

If these updates mutate live members directly, while readers use a separate published metadata / readiness view to determine visibility, the implementation becomes prone to mixed-state reads:

- schema / load_info has already switched, but the runtime object is not ready yet
- the runtime object has already been replaced, but published readiness has not caught up
- prepare is cancelled, leaving unpublished resources behind in live state
- old readers and new readers observe inconsistent lifetimes for the same resource

Therefore, the long-term goal for `ChunkedSegmentSealedImpl` should not be “patch a few individual reopen windows,” but rather to establish one unified **copy-on-write runtime publication model**:

> All reader-visible runtime state that is mutable during reopen/load/update should, whenever possible, be prepared on cloned runtime state and published atomically together with metadata.

### 1.2 Design Goal

This design document describes the **target end-state architecture** for `ChunkedSegmentSealedImpl`:

- readers only consume snapshot-owned published state
- updaters prepare new state on cloned runtime state
- publish atomically switches `(metadata + runtime)`
- finalize is responsible only for post-publication cleanup or follow-up work
- old readers safely retain the old runtime graph through shared ownership
- future feature work should default to the same COW publication model

This is a long-term evolution direction and is not limited to the field or index types already migrated today.

### 1.3 Non-Goals

This design does **not** require the following:

- turning reopen into a transactional rollback system
- migrating all runtime resources in a single step
- removing all historical locks or old members immediately
- forcing every object into runtime snapshots before its concurrency semantics are properly redesigned

The goal is **one unified direction with phased delivery**, not a one-shot rewrite of the entire sealed segment implementation.

### 1.4 Rolling Upgrade Compatibility

The index-update migration assumes a coordinator baseline whose IndexChecker
schedules segment index changes through `LoadScope_Reopen`. The historical
`LoadScope_Index` wire value (`2`) is retired and remains reserved so it cannot
be reused for another meaning.

This design does not support a mixed-version deployment in which an older
QueryCoord still emits `LoadScope_Index` requests to a newer QueryNode after the
legacy index-update handler has been removed. A QueryNode that receives wire
value `2` rejects it as an internal protocol mismatch with an explicit upgrade
message. The deployment must upgrade QueryCoord to a version that emits
`LoadScope_Reopen` before routing index-update requests to these QueryNodes.

The retired value must not fall through to the full segment load path: an index
update is not a full load and must not trigger full-load distribution, bloom
filter, or delete-stream side effects. It must also not be blindly remapped to
`LoadScope_Reopen`, because a legacy request may omit newer
`SegmentLoadInfo` fields; publishing that request as the new load-info snapshot
could discard metadata already held by the newer QueryNode. Supporting that
version skew would require a dedicated compatibility adapter that merges and
normalizes the legacy request against the current published load info before
reopen, which is outside the scope of this design.

---

## 2. Core Problem

### 2.1 Mixed-State Read

Without a unified runtime snapshot, a typical read path is often split into two steps:

1. read schema / load info / readiness from published state
2. read the actual column / index / offsets / auxiliary runtime object from live members

This means a reader does not see one single, coherent object graph. Instead, it observes a composition of two different ownership systems:

- published metadata snapshot
- live runtime members

As soon as the update path is not perfectly synchronized, inconsistency becomes possible.

### 2.2 Prepare Pollution

If reopen / load / replace / drop mutates live runtime first and publishes metadata later, it introduces prepare pollution:

- prepare is cancelled after live state has already been modified
- prepare fails after reader-visible runtime has already been partially changed
- finalize is forced to behave like rollback-oriented cleanup

This makes reopen semantics fragile, and failure paths become much harder to reason about than success paths.

### 2.3 Lifetime Split

Old readers and new readers must coexist safely across a publication boundary. If runtime resources are not owned by published state, then:

- old readers cannot naturally retain the old object graph
- update paths may destroy or replace old runtime objects too early
- correctness depends on extra locks, implicit conventions, or fragile manual lifetime handling

This class of lifetime-split problems can repeat across field data, array offsets, and many kinds of index runtime objects.

---

## 3. Architectural Target

### 3.1 Single Source of Truth

The final goal for `ChunkedSegmentSealedImpl` is to converge reader-visible runtime state onto a single source of truth:

```text
PublishedSegmentState
  ├─ schema
  ├─ load_info
  ├─ commit_ts
  ├─ readiness bitsets
  └─ runtime (snapshot-owned resource graph)
```

A reader that calls `CapturePublishedState()` should obtain, in one step:

- the currently visible metadata
- the currently visible readiness state
- the currently visible runtime ownership graph

In other words:

> A reader should no longer assemble its view by combining published state with live members.

### 3.2 Copy-on-Write Publication Model

Any runtime update that affects reader-visible behavior should ultimately follow the same flow:

```text
capture current published state
        │
        ▼
clone current runtime graph / container topology
        │
        ▼
prepare new runtime state on cloned graph
        │
        ▼
publish new (metadata + runtime) atomically
        │
        ▼
finalize post-publication cleanup / side work
```

Key requirements:

- prepare must not pollute live visible state
- publish is the only visibility boundary
- finalize must not “repair” inconsistencies that readers have already been allowed to observe
- old readers must continue to safely hold the old object graph through the old snapshot

### 3.3 Publication Invariants

For any reader-visible mutable runtime resource, the following invariants should hold:

1. **Prepare invariant**
   - a newly prepared resource is not visible to readers before publish

2. **Publication invariant**
   - metadata visibility and runtime visibility switch atomically

3. **Lifetime invariant**
   - old published state owns the old runtime graph
   - new published state owns the new runtime graph
   - both coexist naturally through shared ownership

4. **Readiness invariant**
   - published readiness must stay synchronized with published runtime state
   - it must never be possible for runtime to contain a resource that readiness does not reflect, or for readiness to claim visibility while the runtime object is missing

---

## 4. Runtime Snapshot Model

### 4.1 Runtime Resource Bundle

`PublishedSegmentState` should not only contain metadata/readiness. It should also own a snapshot-scoped runtime resource bundle.

Conceptually, the runtime bundle should be able to contain all reader-visible mutable runtime state, for example:

- field columns
- array offsets
- struct/array helper mappings
- scalar index runtime objects
- vector index runtime objects
- text index runtime objects
- json-related runtime index / stats objects
- any future reader-visible mutable runtime helper state

The principle is not “what has already been moved today,” but rather:

> If a runtime resource is mutated during reopen/load/update and is directly consumed by readers, it should be treated as a candidate member of the runtime snapshot.

### 4.2 Clone Strategy

COW does not imply deep-copying all payloads.

The recommended strategy is:

- clone container topology (`map` / `set` / indirection graph)
- keep reusing existing `shared_ptr`, cache slots, or immutable payload objects whenever possible
- rewrite only the nodes that actually need replacement in the cloned runtime

This allows the model to preserve:

- clear publication semantics
- safe old-reader / new-reader lifetime behavior
- manageable clone cost

### 4.3 Readiness as Derived Publication State

Field-ready, index-ready, and raw-data-related readiness should not be treated as an independent fact source separate from runtime objects.

In the target architecture, readiness is a **derived consistency expression** of published state:

- it may be maintained explicitly
- it may also be synchronized from runtime during normalize / publish
- but it must not drift away from runtime state over time

The design rule is:

> The published runtime object graph and readiness bitsets must describe the same visible world.

---

## 5. Reader Model

### 5.1 Reader Rule

Future `ChunkedSegmentSealedImpl` reader paths should follow this rule:

- capture the published snapshot first
- fetch column / index / offsets / helper objects from snapshot-owned runtime
- use them through pinned / shared ownership
- do not bypass the snapshot to read directly from live mutable members

### 5.2 Accessor Convergence

Whether the accessor is for field data, array views, json access, ngram access, or vector access, the long-term convergence direction should be the same:

- the authoritative source is `PublishedSegmentState::runtime`
- the accessor return value must pin its underlying dependency graph
- the caller should not need to care whether the object came from the old snapshot or the new snapshot

### 5.3 Old Reader / New Reader Coexistence

With a snapshot-owned runtime graph:

- an old reader captures the old snapshot and continues safely
- a new reader captures the new snapshot after publish
- old and new readers do not need to share one mutable live-member view

This is one of the main benefits of the COW publication model.

---

## 6. Update Model

### 6.1 Prepare / Publish / Finalize Separation

All important update paths should clearly separate three stages:

#### Prepare

- prepare new resources based on captured published state and cloned runtime state
- execute load / replace / drop / rebuild / fill / relink logic
- remain invisible to readers

#### Publish

- atomically publish the new snapshot
- ensure the new snapshot carries both metadata and runtime
- keep readiness synchronized with runtime after publication

#### Finalize

- perform post-publication cleanup or follow-up actions
- release old resources, update bookkeeping, or perform auxiliary work if needed
- never act as the place where reader-visible inconsistency gets repaired after the fact

### 6.2 Mutation Rule

For any future mutable runtime state added to `ChunkedSegmentSealedImpl`, the default pattern should be:

- mutate cloned runtime during `prepare`
- switch the whole snapshot during `publish`
- clean up old resources or record side effects during `finalize`

If a resource cannot follow this model, the PR should explicitly explain:

- why it cannot enter the runtime snapshot
- why its concurrency semantics cannot be expressed by shared ownership + publication boundary
- why it does not reintroduce mixed-state visibility problems

### 6.3 Drop / Replace Semantics

Drop / replace should also become snapshot-driven:

- drop is expressed by removing the entry from the next runtime snapshot
- replace is expressed by replacing the entry in the next runtime snapshot
- the old snapshot retains ownership of the old resource until no reader still references it

This avoids the fragile pattern of “delete from live member first, then hope readers do not step on it.”

---

## 7. Migration Direction

### 7.1 Incremental, But Convergent

This design allows phased rollout, but every phase must converge in the same direction rather than creating multiple long-lived state models.

In other words:

- phased rollout is allowed
- “not migrated yet” must not be interpreted as “does not need migration permanently”

### 7.2 Priority Order

Future migration can be prioritized in this order:

1. resources that are frequently mutated by reopen/load and directly consumed by readers
2. resources that already expose mixed-state / residue / readiness-mismatch risk
3. resources strongly coupled with field/index publication
4. resources that need additional concurrency redesign before they can move

From the long-term perspective, all of the following should be on an explicit migration path:

- `vector_indexings_`
- `text_indexes_`
- `json_indices` / `json_stats_`
- any future reader-visible mutable runtime state

### 7.3 Reader Mutex Special Case

`reader_` is a special object:

- its current correctness depends on explicit mutex coverage during use
- simply placing it inside runtime snapshots is not enough to solve its concurrency semantics automatically

Therefore, `reader_` may migrate later than other resources, but it should still be treated as a dedicated concurrency / ownership redesign problem, not as a permanent exception to the COW model.

---

## 8. Design Rule for Future `ChunkedSegmentSealedImpl` Changes

Any future change to `ChunkedSegmentSealedImpl` reopen/load/update logic should, by default, pass the following checks:

1. Is this state mutated while readers may still observe the segment?
2. Can this state be prepared first in cloned runtime state?
3. Can readers consume it from `PublishedSegmentState::runtime`?
4. After publish, can old readers and new readers safely coexist through shared ownership?
5. Can readiness / schema / runtime visibility still be described by one published truth?

The default answer should be “yes.”

If the answer is “no,” the change should be treated as an exception that must be justified explicitly, not as a new default implementation style.

This rule especially applies to:

- future vector index work
- future text index work
- future json runtime/index work
- any newly added reader-visible mutable runtime helper state

---

## 9. Verification Principles

Validation should not only prove that one helper is internally self-consistent. It must validate the full publication semantics.

### 9.1 What Must Be Verified

1. **Atomic visibility**
   - readers observe either the old snapshot or the new snapshot
   - readers never observe mixed state

2. **Prepare isolation**
   - new runtime state built during prepare is invisible before publish

3. **Lifetime safety**
   - old readers can still safely use the old runtime graph after publication

4. **Drop / replace safety**
   - new readers do not see old resources after publication
   - old readers can still access old resources until they release the old snapshot

5. **Readiness consistency**
   - readiness always corresponds to the same published world as the runtime graph

### 9.2 Regression Strategy

Validation should not only cover the resources already migrated today. It should cover the feature paths most likely to expose publication bugs, such as:

- grouped column / child field paths
- reopen + drop field paths
- reopen + replace field/index paths
- scalar/vector/text/json accessor paths
- search / retrieve / refine / take paths
- default-filled / array-offset / auxiliary-state paths

The core goal is to prove:

> This model holds for all reader-visible mutable runtime state, not just for one field category or one index category.

---

## 10. Expected End State

The final target state of `ChunkedSegmentSealedImpl` should be:

- reader-visible runtime state is primarily snapshot-owned
- reopen/load/update prepares on cloned runtime state
- metadata and runtime are published atomically together
- finalize performs only post-publication cleanup and follow-up actions
- old and new snapshots coexist naturally through shared ownership
- new features default to the same COW publication model
- remaining live-owned runtime state is treated as migration backlog rather than as a permanent parallel architecture

In other words, the end state is not “a collection of local fixes,” but:

> `ChunkedSegmentSealedImpl` evolves under one stable consistency model in which all reader-visible mutable runtime state defaults to the COW runtime publication framework.

---

## 11. Final Principle

For any future change that touches reader-visible mutable runtime state in `ChunkedSegmentSealedImpl`, the default design principle should be:

> prefer cloned runtime preparation + atomic snapshot publication + post-publication finalize over direct live-member mutation.

That is the final goal this design document establishes.
