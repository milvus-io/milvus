# GIS Geometry Filter Optimization: Coarse/Refine Split + Same-Column Fusion

## 1. Background and Problem

Vector search with geometry filters (filtered search whose expression contains `ST_INTERSECTS` / `ST_WITHIN` / other geometric predicates) wastes a large amount of CPU. A CPU profile of the C++ segcore shows:

- The actual geometric intersection computation (`GEOSPreparedIntersects` etc.) takes **< 1%** of CPU.
- Constructing/parsing/destructing GEOS geometry objects (`Geometry::Geometry` / `~Polygon` / `~LinearRing` / `GeometryFactory`) takes **~20%** of CPU — more than the vector search itself.

Root cause: `PhyGISFunctionFilterExpr` constructs a GEOS object from WKB on the fly — **per geometry predicate, per row** — and destroys it right after use. If a query has K same-column geometry predicates, the same row's geometry is constructed K times. `enableGeometryCache` is off by default (2.6.5); enabling it inflates memory 5–10x (a particularly bad trade for bbox-style data), so it is not a general solution.

### Two concrete issues in the current implementation (confirmed in code)

- **Issue 1 — wrong scheduling order**: the GIS expression as a whole (cheap R-Tree coarse filter + expensive exact refine) falls into the `indexed_expr` bucket (`Expr.cpp` `ReorderConjunctExpr`), which is ordered very early in the conjunction reorder. The expensive refine is therefore dragged along with the cheap coarse filter and runs *before* the scalar predicates.
- **Issue 2 — no pruning**: the refine step does not consume `bitmap_input` (`GISFunctionFilterExpr.cpp::EvalForIndexSegment`): `collect_hits` takes the entire coarse bitmap, constructs a geometry for every coarse candidate row (with segment-level caching), and never intersects with the upstream scalar predicates' result.

Combined effect: no matter how selective the scalar conditions (`work_model` / `experience` / ...) are, the refine constructs geometry objects row by row for everything the coarse filter hit. This is the core waste.

## 2. Goals (three in one)

- **Split**: for each geometry column, split out `Coarse` (R-Tree) and `Refine` (exact) as two independent tree nodes.
- **Fix bitmap_input**: `Refine` consumes the upstream `bitmap_input` and only exact-evaluates the rows that survived *all* cheaper predicates.
- **Fusion**: for multiple geometry predicates on the same column, the `Refine` stage reads the WKB / constructs the GEOS object only once per surviving row and applies all predicates to it (K→1).

## 3. Algebraic Foundation (why it is also correct across OR)

Let a geometry block be `B = g1 ⊕ ... ⊕ gk` (⊕ is the block's AND or OR). Let `Ri` be the exact predicate and `Ci` its R-Tree coarse filter, satisfying `Ci ⊇ Ri`. Define `B_coarse = ⊕ Ci` and `B_refine = ⊕ Ri`; then `B_coarse ⊇ B_refine`, hence:

```
scalars ∧ B  =  scalars ∧ B_refine  ≡  scalars ∧ B_coarse ∧ B_refine
```

So `B_coarse` can be hoisted to the very front as an extra AND child (cheap, selective, prunes everyone else), and `B_refine` pushed to the very end (expensive, pruned by everyone else). This holds for both AND blocks and OR blocks — which is why the split remains correct across OR.

## 4. Architecture: Two New Operators + One Shared State

```cpp
// One per (segment, geometry block), shared by Coarse and Refine
struct GISGroupState {
  FieldId field_id;
  bool    is_and;                 // combination within the block: AND / OR
  struct Pred {
    GISOp            op;
    Geometry         query_geom;  // query constant, constructed once and reused
    PreparedGeometry prepared;    // prepared once and reused (fixes per-batch rebuild)
    bool             has_index;
    TargetBitmap     coarse;      // this predicate's R-Tree result, computed once
  };
  std::vector<Pred> preds;
  std::shared_ptr<TargetBitmap> coarse_candidates; // B_coarse, filled by Coarse, cached once
  std::atomic<bool> coarse_done{false};
};

class PhyGISCoarseConjunctExpr : public SegmentExpr { std::shared_ptr<GISGroupState> st_; }; // -> indexed bucket (early)
class PhyGISRefineConjunctExpr : public SegmentExpr { std::shared_ptr<GISGroupState> st_; }; // -> heavy bucket (last)
```

## 5. Optimizer Rule (mirrors LIKE's `SetLikeIndices` + builds nodes at run time)

1. Walk the conjunction children and recognize "geometry blocks on the same field": either direct GIS leaves (`PhyGISFunctionFilterExpr`), or an AND/OR subtree consisting solely of GIS predicates on that field (record the combination as ⊕).
2. For each block, build a `GISGroupState` (collect `preds` and `is_and`; construct `query_geom` + `prepared` once up front).
3. Emit the `Coarse` and `Refine` nodes (sharing the same state) and remove the original GIS nodes from the children.
4. Bucketing: `Coarse` → `indexed_expr` (early); `Refine` → `heavy_conjunct_expr` (last). Reuses the existing reorder machinery; no new mechanism needed.
5. Degradation: if a block contains non-geometry siblings, or is nested in a way that cannot be made purely geometric, the block is not split and the original `PhyGISFunctionFilterExpr` is kept (correctness first). Combined with query rewriting that normalizes filters into "scalars AND (same-column ST OR group)", the hit rate is highest.

## 6. Coarse Node Eval (once per segment + slicing)

```cpp
void PhyGISCoarseConjunctExpr::Eval(EvalCtx& ctx, VectorPtr& result) {
  auto bs = GetNextBatchSize(); if (bs == 0) { result = nullptr; return; }
  if (!st_->coarse_done) {                          // per segment, only once
    TargetBitmap cand(active_count_, st_->is_and);  // AND -> all ones / OR -> all zeros
    for (auto& p : st_->preds) {
      if (p.has_index) RunRTreeQuery(p);            // reuses existing idx_ptr->Query(ds)
      else             p.coarse = TargetBitmap(active_count_, true); // no index -> full set
      st_->is_and ? (cand &= p.coarse) : (cand |= p.coarse);
    }
    st_->coarse_candidates = std::make_shared<TargetBitmap>(std::move(cand));
    st_->coarse_done = true;
  }
  TargetBitmap out; out.append(*st_->coarse_candidates, current_pos_, bs);
  current_pos_ += bs;
  result = std::make_shared<ColumnVector>(std::move(out), TargetBitmap(bs, true));
}
```

## 7. Refine Node Eval (consumes bitmap_input + one construction per row + fusion)

```cpp
void PhyGISRefineConjunctExpr::Eval(EvalCtx& ctx, VectorPtr& result) {
  auto bs = GetNextBatchSize(); if (bs == 0) { result = nullptr; return; }
  TargetBitmap res(bs, false);

  const auto& pre = ctx.get_bitmap_input();        // == scalars ∧ B_coarse (the fix)
  TargetBitmap survivors(bs, true);
  if (!pre.empty()) survivors &= pre;
  survivors &= slice(*st_->coarse_candidates, current_pos_, bs); // belt and braces

  if (!survivors.none()) {
    auto hits = collect_hits(survivors);
    auto* gcache = SimpleGeometryCacheManager::Instance()
                     .GetCache(segment_->get_segment_id(), st_->field_id);
    auto wkb = gcache ? nullptr
                      : segment_->bulk_subscript(st_->field_id, hits); // one bulk fetch
    for (size i : hits) {
      const Geometry& left = gcache
          ? *gcache->GetByOffsetUnsafe(i)           // cache on: zero construction
          : Geometry(ctx_, wkb[i]...);              // cache off: construct ONCE
      bool bit = st_->is_and;                       // apply all predicates to one left (fusion)
      for (auto& p : st_->preds) {
        bool r = EvalPrepared(p, left);             // within/contains semantics swapped, reuses existing code
        bit = st_->is_and ? (bit && r) : (bit || r);
        if (st_->is_and ^ bit) break;               // short circuit
      }
      res[local(i)] = bit;
    }
  }
  current_pos_ += bs;
  result = std::make_shared<ColumnVector>(std::move(res), TargetBitmap(bs, true));
}
```

All three wins land at once: consuming `bitmap_input` → exact evaluation only on rows that passed "all scalars ∧ coarse"; one construction per row → K predicates share one `left` (K→1); `prepared` reuse → the query geometry is constructed only once.

## 8. Final Shape Inside the Conjunction

```
input_order_: [ numeric... , indexed(incl. B_coarse) , string... , heavy scalars... , heavy(B_refine last) , compare... ]
```

The `bitmap_input` chain: `B_coarse` contributes early → intermediate scalars keep narrowing the set → `B_refine` finally receives the smallest set for exact evaluation. `CanSkipFollowingExprs` still applies (when coarse / scalars zero out the result, refine is skipped entirely).

## 9. Correctness / Edge Cases

- Invariant `Ci ⊇ Ri` (bbox intersection ⊇ exact intersection; within is conservative in the same way): the existing refine already relies on it; the split does not break it.
- OR blocks: hoisting `B_coarse` / `B_refine` as outer AND children is justified by the identity in Section 3.
- `Refine` returns false for non-surviving rows; the conjunction ANDs the results again, so no error.
- within/contains semantics swap: reuses the existing `evaluate_geometry_prepared`.
- Null geometry: `GetByOffsetUnsafe` returns nullptr → false according to the op.
- No R-Tree index: coarse = full set (no pruning), but refine still consumes `bitmap_input` and fuses, so the win is not lost.
- growing / mmap: keeps the existing `std::string` vs `std::string_view` branches.
- Orthogonal to the cache: this design is fast for bbox data even with the cache off; it does not depend on `enableGeometryCache`.

## 10. Delivery Phases

- **P1**: split + bitmap_input fix (coarse early / refine last, consuming bitmap_input). A large win is already visible with a single predicate; low risk.
- **P2**: same-column fusion in Refine (K→1) + prepared reuse.
- **P3**: more general nested-block recognition (beyond "scalars AND same-column OR/AND group").

## 11. Testing and Rollout

- Feature flag: `queryNode.segcore.enableGISSplitFusion`, default off, for gradual rollout.
- Equivalence tests `GISCoarseRefineExprTest`: full-matrix equivalence against the original per-predicate Eval (AND/OR, intersects/within/contains, null, index/no-index, growing/sealed/mmap, empty bitmap_input).
- Bench: reproduce multi-predicate geo queries; verify that the `Geometry::Geometry` CPU share drops from ~20% to ~0, the refine candidate row count shrinks, and p99 falls back from seconds.
