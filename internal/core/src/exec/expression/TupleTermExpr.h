// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

// Correlated multi-column exact membership: `[a, b] in [[v1,w1], ...]`. See
// design doc docs/design-docs/design_docs/20260901-tuple-term-membership-expression.md.
//
// v1 is deliberately a correctness-first, brute-force implementation: every
// active/valid row is probed directly against the tuple set (raw field data
// required for every participating column; no scalar-index coarse filter, no
// index-only fallback). This mirrors how bloom_match shipped its data-path
// probe first and deferred index acceleration to Future Work. It also does
// NOT extend SegmentExpr (the base every single-column filter expr in this
// directory uses): SegmentExpr's chunk cursor is bound to exactly one field
// at construction, which cannot represent N independently-chunked columns.
// PhyCompareFilterExpr (column-vs-column comparison) is the right precedent
// instead, generalized here from 2 columns to N. Unlike PhyCompareFilterExpr,
// this v1 does not keep a per-column "last pinned chunk" cache across
// consecutive rows -- every (row, column) pair re-pins its chunk. That is a
// deliberate simplicity-over-speed trade given this code could not be
// compiled or profiled in the environment it was authored in; revisit if a
// profile shows it matters (see design doc Future work).

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/TupleMembership.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "pb/plan.pb.h"
#include "segcore/SegmentChunkReader.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

class PhyTupleTermFilterExpr : public Expr {
 public:
    PhyTupleTermFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::TupleTermFilterExpr>& expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size);

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    MoveCursor() override;

    std::string
    ToString() const override {
        return expr_->ToString();
    }

    bool
    IsSource() const override {
        return true;
    }

    // Unlike a single-column filter, a tuple predicate has no single answer
    // to "which column does this source from" -- mirrors
    // PhyCompareFilterExpr, which returns std::nullopt for the same reason.
    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return std::nullopt;
    }

    // v1 has no index-native execution path (raw field data is required for
    // every column; see the class comment), so this must stay batched, like
    // every other membership-shaped filter in this directory.
    bool
    CanExecuteAllAtOnce() const override {
        return false;
    }

    // The cache key derives from ToString(), a slim summary that cannot
    // distinguish two distinct tuple sets of equal size -- caching would let
    // one query's result bitmap leak into another's. Mirrors
    // PhyMembershipFilterExpr::IsCacheable() for the same reason.
    bool
    IsCacheable() const override {
        return false;
    }

 private:
    // Resolves (chunk_id, chunk_offset) for a global row offset against
    // field, uniformly across growing and sealed/chunked segments. Growing
    // segments compute this arithmetically from SizePerChunk(); sealed
    // segments delegate to the segment's own chunk index. Mirrors
    // PhyCompareFilterExpr's get_chunk_id_and_offset lambda.
    std::pair<int64_t, int64_t>
    GetChunkIdAndOffset(FieldId field, int64_t offset) const;

    // Builds, once per constructed physical expr, one closure per
    // participating column: given a global row offset, resolve the column's
    // value for that row, append its canonical encoding to key, and return
    // false (leaving key's prior content appended-but-irrelevant, since the
    // caller aborts the row on a false return) iff the value is NULL. The
    // concrete C++ storage type is fixed per column at construction time
    // (from the column's declared DataType), so the per-row hot loop below
    // never re-branches on DataType.
    using ColumnReader = std::function<bool(int64_t row, std::string& key)>;

    template <typename T>
    ColumnReader
    MakeColumnReader(const milvus::expr::ColumnInfo& column) const;

    ColumnReader
    BuildColumnReader(const milvus::expr::ColumnInfo& column) const;

    VectorPtr
    ExecVisitorImpl(EvalCtx& context);

 private:
    std::shared_ptr<const milvus::expr::TupleTermFilterExpr> expr_;
    const segcore::SegmentChunkReader segment_chunk_reader_;
    int64_t batch_size_;
    // Sequential-scan cursor used only when this expr has no offset input
    // (context.get_offset_input() == nullptr), i.e. it is the first/only
    // predicate driving row selection rather than consuming an
    // already-pruned candidate list.
    int64_t current_row_{0};
    std::vector<ColumnReader> column_readers_;
};

}  // namespace exec
}  // namespace milvus
