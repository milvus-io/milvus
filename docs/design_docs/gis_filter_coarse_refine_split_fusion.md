# GIS 地理过滤优化：粗筛/精排拆分 + 同列融合

## 1. 背景与问题

带地理过滤的向量检索（filtered search，过滤含 `ST_INTERSECTS` / `ST_WITHIN` 等几何谓词）存在严重的 CPU 浪费。C++ segcore 的 CPU profile 显示：

- 实际几何相交计算（`GEOSPreparedIntersects` 等）占 CPU **< 1%**。
- GEOS 几何对象的构造/解析/析构（`Geometry::Geometry` / `~Polygon` / `~LinearRing` / `GeometryFactory`）占 CPU **约 20%**，比向量检索还多。

根因：`PhyGISFunctionFilterExpr` 对**每个几何谓词、每一行**都从 WKB 现场构造一个 GEOS 对象、用完销毁。一条查询若有 K 个同列几何谓词，同一行的几何对象就被构造 K 次。`enableGeometryCache` 默认关闭（2.6.5），开启则内存膨胀 5~10 倍（对 bbox 数据尤其不划算），并非通用解。

### 当前实现的两个问题（已在代码确认）

- **问题 1 — 顺序错**：GIS 整体（coarse 粗筛 + refine 精排）落入 `indexed_expr` 桶（`Expr.cpp` `ReorderConjunctExpr`），在 conjunction 重排里排得很靠前，导致昂贵的精排跟着便宜的粗筛一起被排到标量谓词之前。
- **问题 2 — 不剪枝**：精排不消费 `bitmap_input`（`GISFunctionFilterExpr.cpp::EvalForIndexSegment`）：`collect_hits` 取整个 coarse 位图，对全部 coarse 候选逐行构造几何并做段级缓存，从不与上游标量谓词的结果求交。

两者叠加 = 无论 `work_model` / `experience` 等标量条件怎么筛，精排都对粗筛命中的所有行逐行构造几何对象。这是核心浪费。

## 2. 目标（三者合一）

- **拆分**：每个几何列拆出 `Coarse`(R-Tree) 和 `Refine`(精确) 两个独立 tree 节点。
- **修 bitmap_input**：`Refine` 消费上游 `bitmap_input`，只精算"全部更便宜谓词都通过"的幸存行。
- **融合**：同一列的多个几何谓词，`Refine` 阶段对每个幸存行只读一次 WKB / 只构造一次 GEOS 对象，套用全部谓词（K→1）。

## 3. 代数基础（为什么跨 OR 也正确）

设几何块 `B = g1 ⊕ ... ⊕ gk`（⊕ 为该块的 AND 或 OR）。`Ri` 为精确谓词，`Ci` 为其 R-Tree 粗筛，满足 `Ci ⊇ Ri`。令 `B_coarse = ⊕ Ci`，`B_refine = ⊕ Ri`，则 `B_coarse ⊇ B_refine`，故：

```
scalars ∧ B  =  scalars ∧ B_refine  ≡  scalars ∧ B_coarse ∧ B_refine
```

于是可把 `B_coarse` 作为额外的 AND 子节点提到最前（便宜、选择性强、剪枝别人），把 `B_refine` 提到最后（昂贵、被所有人剪枝）。AND 块与 OR 块都成立——这是拆分跨 OR 仍正确的依据。

## 4. 架构：两个新算子 + 一份共享状态

```cpp
// 每个 (segment, 几何块) 一份，Coarse 与 Refine 共享
struct GISGroupState {
  FieldId field_id;
  bool    is_and;                 // 块内组合：AND / OR
  struct Pred {
    GISOp            op;
    Geometry         query_geom;  // 查询常量，构造一次复用
    PreparedGeometry prepared;    // 预编译一次复用（修掉每 batch 重建）
    bool             has_index;
    TargetBitmap     coarse;      // 该谓词 R-Tree 结果，算一次
  };
  std::vector<Pred> preds;
  std::shared_ptr<TargetBitmap> coarse_candidates; // B_coarse，Coarse 节点填，缓存一次
  std::atomic<bool> coarse_done{false};
};

class PhyGISCoarseConjunctExpr : public SegmentExpr { std::shared_ptr<GISGroupState> st_; }; // -> indexed 桶（早）
class PhyGISRefineConjunctExpr : public SegmentExpr { std::shared_ptr<GISGroupState> st_; }; // -> heavy 桶（最后）
```

## 5. Optimizer 规则（镜像 LIKE 的 `SetLikeIndices` + 运行期建节点）

1. 遍历 conjunction 子节点，识别"同一 field 的几何块"：直接的 GIS 叶子（`PhyGISFunctionFilterExpr`），或仅由该 field 的 GIS 谓词构成的 AND/OR 子树（用 ⊕ 记下组合方式）。
2. 对每个块构造 `GISGroupState`（收集 `preds`、`is_and`，一次性建好 `query_geom` + `prepared`）。
3. 生成 `Coarse` 与 `Refine` 两节点（共享同一 state），从原 children 移除原始 GIS 节点。
4. 分桶：`Coarse` → `indexed_expr`（早）；`Refine` → `heavy_conjunct_expr`（最后）。复用现有 reorder 顺序，无需新机制。
5. 退化：块内含非几何兄弟或复杂嵌套无法纯几何化 → 该块不拆，保留原 `PhyGISFunctionFilterExpr`（正确性优先）。配合查询改写归一成"标量 AND (同列 ST OR 组)"命中率最高。

## 6. Coarse 节点 Eval（段级一次 + 切片）

```cpp
void PhyGISCoarseConjunctExpr::Eval(EvalCtx& ctx, VectorPtr& result) {
  auto bs = GetNextBatchSize(); if (bs == 0) { result = nullptr; return; }
  if (!st_->coarse_done) {                          // 段级，只一次
    TargetBitmap cand(active_count_, st_->is_and);  // AND -> 全1 / OR -> 全0
    for (auto& p : st_->preds) {
      if (p.has_index) RunRTreeQuery(p);            // 复用现有 idx_ptr->Query(ds)
      else             p.coarse = TargetBitmap(active_count_, true); // 无索引 -> 全集
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

## 7. Refine 节点 Eval（吃 bitmap_input + 逐行一次构造 + 融合）

```cpp
void PhyGISRefineConjunctExpr::Eval(EvalCtx& ctx, VectorPtr& result) {
  auto bs = GetNextBatchSize(); if (bs == 0) { result = nullptr; return; }
  TargetBitmap res(bs, false);

  const auto& pre = ctx.get_bitmap_input();        // == scalars ∧ B_coarse （修复点）
  TargetBitmap survivors(bs, true);
  if (!pre.empty()) survivors &= pre;
  survivors &= slice(*st_->coarse_candidates, current_pos_, bs); // 双保险

  if (!survivors.none()) {
    auto hits = collect_hits(survivors);
    auto* gcache = SimpleGeometryCacheManager::Instance()
                     .GetCache(segment_->get_segment_id(), st_->field_id);
    auto wkb = gcache ? nullptr
                      : segment_->bulk_subscript(st_->field_id, hits); // 一次批量取
    for (size i : hits) {
      const Geometry& left = gcache
          ? *gcache->GetByOffsetUnsafe(i)           // 缓存开：零构造
          : Geometry(ctx_, wkb[i]...);              // 缓存关：构造【一次】
      bool bit = st_->is_and;                       // 一个 left 套全部谓词（融合）
      for (auto& p : st_->preds) {
        bool r = EvalPrepared(p, left);             // within/contains 语义对调，复用现有
        bit = st_->is_and ? (bit && r) : (bit || r);
        if (st_->is_and ^ bit) break;               // 短路
      }
      res[local(i)] = bit;
    }
  }
  current_pos_ += bs;
  result = std::make_shared<ColumnVector>(std::move(res), TargetBitmap(bs, true));
}
```

三处收益同时拿到：吃 `bitmap_input` → 只在"标量全过 ∧ coarse"的幸存行上精算；逐行一次构造 → K 谓词共享 `left`（K→1）；`prepared` 复用 → 查询几何只构造一次。

## 8. 在 conjunction 里的最终形态

```
input_order_: [ numeric... , indexed(含 B_coarse) , string... , 标量heavy... , heavy(含 B_refine 最后) , compare... ]
```

`bitmap_input` 链：`B_coarse` 早早贡献 → 中间标量继续收窄 → `B_refine` 最后拿到最小集合精算。`CanSkipFollowingExprs` 仍生效（coarse / 标量把结果打成全 0 时，refine 直接跳过）。

## 9. 正确性 / 边界

- 不变式 `Ci ⊇ Ri`（bbox 相交 ⊇ 精确相交；within 同理保守），现有 refine 已依赖，拆分不破坏。
- OR 块：用第 3 节恒等式提升 `B_coarse` / `B_refine` 为外层 AND 子节点，合法。
- `Refine` 对非幸存行返回 false，conjunction 再 AND，无误。
- within/contains 语义对调：复用现有 `evaluate_geometry_prepared`。
- null 几何：`GetByOffsetUnsafe` 返回 nullptr → 按 op 取 false。
- 无 R-Tree 索引：coarse = 全集（不剪枝），refine 仍吃 `bitmap_input` + 融合，收益不失。
- growing / mmap：沿用现有 `std::string` vs `std::string_view` 分支。
- 缓存正交：本方案对 bbox 数据缓存关着也很快，不依赖 `enableGeometryCache`。

## 10. 落地分期

- **P1**：拆分 + bitmap_input 修复（coarse 早 / refine 晚吃 bitmap_input）。单谓词即可见大收益，风险低。
- **P2**：Refine 同列融合（K→1）+ prepared 复用。
- **P3**：更通用的嵌套块识别（超出"标量 AND 同列 OR/AND 组"）。

## 11. 测试与灰度

- Feature flag：`queryNode.segcore.enableGISSplitFusion`，默认关，灰度。
- 等价测试 `GISCoarseRefineExprTest`：与"逐谓词原始 Eval"做全矩阵等价校验（AND/OR、intersects/within/contains、null、index/no-index、growing/sealed/mmap、空 bitmap_input）。
- Bench：复现多谓词地理查询，验证 `Geometry::Geometry` 占比 20% → 约 0、refine 候选行数下降、p99 从秒级回落。
