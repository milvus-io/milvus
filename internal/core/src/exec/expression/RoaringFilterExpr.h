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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

// roaring_match(field, {bitmap}): exact integer membership.
//
// Unlike bloom_match this is exact, so it is usable in delete and needs no
// recheck. The bitmap is decoded once per logical expression (see
// milvus::expr::RoaringFilterExpr) and shared by every per-segment physical
// expression, so this class only probes it.
//
// INDEX-ONLY FALLBACK: a sealed field may have no raw data loaded. A Roaring
// membership test cannot be pushed into a scalar index, so the probe must still
// see values. DetermineExecPath() commits to ExprExecPath::ScalarIndex *only*
// when raw data is absent but a reverse-lookup-capable scalar index is pinned;
// the probe then recovers each value via ScalarIndex::Reverse_Lookup and probes
// it exactly as it would raw data. When raw data exists it stays on RawData.
class PhyRoaringFilterExpr : public SegmentExpr {
 public:
    PhyRoaringFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::RoaringFilterExpr>& expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size,
        int32_t consistency_level);

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    DetermineExecPath() override {
        if (has_field_data_at_init_) {
            exec_path_ = ExprExecPath::RawData;
            return;
        }
        // No raw data. Try to pin a scalar index that can reverse-look-up the
        // stored values; HasCompatibleScalarIndex() may report true for a
        // vector/binlog-index-only field or a mid-load state where PinIndex()
        // still yields nothing, so verify the pin and its reverse-lookup
        // capability before committing to the index path.
        if (HasCompatibleScalarIndex()) {
            EnsurePinnedIndex();
            if (!pinned_index_.empty() && IndexSupportsReverseLookup()) {
                exec_path_ = ExprExecPath::ScalarIndex;
                return;
            }
        }
        // No raw data and no usable index: keep RawData. ExecVisitorImpl
        // detects this and throws a clear SegcoreError instead of asserting.
        exec_path_ = ExprExecPath::RawData;
    }

    bool
    IsSource() const override {
        return true;
    }

    // roaring_match is never index-native: on the index-only fallback it probes
    // the bitmap per row via ScalarIndex::Reverse_Lookup. The base class treats
    // any non-RawData path as "execute all at once", which would size the batch
    // to the whole segment and materialize an active_count-wide OffsetVector
    // (~400MiB for 100M rows) in one pass. Force batched execution so the
    // reverse-lookup path stays bounded to batch_size, like PhyBloomFilterExpr.
    bool
    CanExecuteAllAtOnce() const override {
        return false;
    }

    // Exclude roaring_match from the FilterBitsNode result cache. The cache key
    // derives from ToString(), a slim summary (column + body size +
    // cardinality) that CANNOT distinguish two distinct bitmaps of equal size
    // and cardinality — caching would let one query reuse another's bitmap and
    // return wrong rows. Non-cacheability propagates up: any predicate holding
    // a roaring_match is not cached. Keeping ToString slim is then safe, and
    // avoids ever hashing or dumping a multi-megabyte bitmap.
    bool
    IsCacheable() const override {
        return false;
    }

    std::string
    ToString() const override;

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override;

 private:
    template <typename T>
    VectorPtr
    ExecVisitorImpl(EvalCtx& context);

    template <typename T>
    VectorPtr
    ExecVisitorImplForIndex(EvalCtx& context);

    // True iff the pinned scalar index is usable for the per-row reverse-lookup
    // probe: it must (1) expose stored values via Reverse_Lookup (HasRawData())
    // AND (2) do so cheaply. roaring_match cannot push the bitmap into an
    // index, so an index without recoverable raw values is useless here; and a
    // BITMAP index without its offset cache reverse-looks-up in O(cardinality)
    // per row, which would turn the index-only probe into an
    // O(rows * cardinality) scan — exclude it so roaring_match falls through to
    // the clear "no usable index" error instead of silently running billions of
    // checks.
    //
    // Unlike bloom_match there is no VARCHAR case: roaring_match accepts only
    // signed integer fields.
    bool
    IndexSupportsReverseLookup() const {
        if (pinned_index_.empty() || pinned_index_[0].get() == nullptr) {
            return false;
        }
        switch (expr_->column_.data_type_) {
            case DataType::INT8:
                return IndexUsableForReverseLookup<int8_t>();
            case DataType::INT16:
                return IndexUsableForReverseLookup<int16_t>();
            case DataType::INT32:
                return IndexUsableForReverseLookup<int32_t>();
            case DataType::INT64:
                return IndexUsableForReverseLookup<int64_t>();
            default:
                return false;
        }
    }

    // Both gates for the reverse-lookup path: recoverable raw values, and a
    // cheap (non-O(cardinality)) per-row Reverse_Lookup.
    template <typename T>
    bool
    IndexUsableForReverseLookup() const {
        return IndexHasRawData<T>() && IndexSupportsFastReverseLookup<T>();
    }

    // Mirrors SegmentExpr::IndexHasRawData<T>() (Expr.h): pins the concrete
    // scalar index and asks whether its per-row Reverse_Lookup is cheap.
    template <typename T>
    bool
    IndexSupportsFastReverseLookup() const {
        typedef std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
                IndexInnerType;
        using Index = index::ScalarIndex<IndexInnerType>;
        auto scalar_index = dynamic_cast<const Index*>(pinned_index_[0].get());
        return scalar_index != nullptr &&
               scalar_index->SupportFastReverseLookup();
    }

    std::shared_ptr<const milvus::expr::RoaringFilterExpr> expr_;
};

}  // namespace exec
}  // namespace milvus
