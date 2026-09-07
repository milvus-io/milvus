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

// Unified execution chain for the membership-filter expression family.
//
// PhyMembershipFilterExpr<LogicalExpr, ProbePolicy> carries ALL control flow —
// exec-path selection, batching, the raw-data and index-fallback skeletons,
// cacheability — while each kind's data plane ("approximate vs exact") stays in
// its probe policy: SplitBlockBloomFilterView for MBF1 blobs,
// RoaringMembership for MRB1 bitmaps. The two aliases below keep the historical
// class names, so the factory and logs are unchanged.
//
// Semantics pinned here (do not fork per kind without a design-doc reason):
//   * Upstream-excluded candidates (bitmap_input) are checked FIRST on the
//     raw-data path and keep their initial (false, valid) even when the
//     field value is NULL, mirroring the framework's index-path helpers —
//     so raw and index-only load states return bit-identical columns. This
//     is the contract master pins for bloom and roaring alike (see
//     BitmapInputPrunesByCandidatePosition,
//     ScalarBitmapInputLeavesExcludedNullCandidatesUntouched). A probed
//     NULL row never matches, under either polarity: res = valid = false.
//   * The index-only fallback routes through the WithMask reverse-lookup
//     helpers; an empty mask degenerates to the unmasked behavior, so one
//     code path serves both.
//   * JSON probing exists only where the policy supports it (bloom kind).

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/EasyAssert.h"
#include "common/RoaringMembership.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/BloomFilterExpr.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

// Probe policy for the approximate bloom kind: zero-copy SBBF view over the
// logical node's MBF1 blob, gated by the blob's declared value domains.
struct BloomMembershipProbe {
    static constexpr const char* kKindName = "membership_match(type=bloom)";
    static constexpr bool kSupportsVarChar = true;
    // JSON probes the value at the column's nested path per row, hashing by
    // the value's runtime type (strictly typed; see ExecVisitorImplJson).
    static constexpr bool kSupportsJson = true;

    SplitBlockBloomFilterView filter;

    // Envelope parsed and validated exactly once per physical expr; throws
    // SegcoreError{ExprInvalid} on malformed blobs. The view aliases
    // expr.filter_blob_, owned by the logical node held via shared_ptr.
    explicit BloomMembershipProbe(const milvus::expr::BloomFilterExpr& expr)
        : filter(SplitBlockBloomFilterView::Parse(expr.filter_blob_)) {
    }

    template <typename T>
    bool
    operator()(const T& v) const {
        return filter.TestScalar(v);
    }

    // JSON probes hash by the stored value's runtime type.
    bool
    TestBytesValue(const void* data, size_t len) const {
        return filter.TestBytes(data, len);
    }

    bool
    TestInt64Value(int64_t v) const {
        return filter.TestInt64(v);
    }
};

// Probe policy for the exact roaring kind: the bitmap was decoded once per
// plan (milvus::expr::RoaringFilterExpr) and shared by every per-segment
// physical expression.
struct RoaringMembershipProbe {
    static constexpr const char* kKindName = "membership_match(type=roaring)";
    static constexpr bool kSupportsVarChar = false;
    static constexpr bool kSupportsJson = false;

    // Declared in namespace milvus (not milvus::expr).
    const RoaringMembership* membership;

    explicit RoaringMembershipProbe(const milvus::expr::RoaringFilterExpr& expr)
        : membership(expr.membership_.get()) {
        AssertInfo(membership != nullptr,
                   "membership_match(type=roaring) physical expression "
                   "requires membership");
    }

    template <typename T>
    bool
    operator()(const T& v) const {
        // Widen through int64_t so a narrow signed value keeps its
        // two's-complement key: INT8(-1) must probe 0xffffffffffffffff,
        // which is where the client's builder put it.
        return membership->Contains(static_cast<int64_t>(v));
    }
};

template <typename LogicalExpr, typename ProbePolicy>
class PhyMembershipFilterExpr : public SegmentExpr {
 public:
    PhyMembershipFilterExpr(const std::vector<std::shared_ptr<Expr>>& input,
                            const std::shared_ptr<const LogicalExpr>& expr,
                            const std::string& name,
                            milvus::OpContext* op_ctx,
                            const segcore::SegmentInternalInterface* segment,
                            int64_t active_count,
                            int64_t batch_size,
                            int32_t consistency_level)
        : SegmentExpr(std::move(input),
                      name,
                      op_ctx,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      DataType::NONE,
                      active_count,
                      batch_size,
                      consistency_level),
          expr_(expr),
          probe_(*expr) {
        switch (expr_->column_.data_type_) {
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
            case DataType::INT64:
                break;
            case DataType::VARCHAR:
                if constexpr (!ProbePolicy::kSupportsVarChar) {
                    ThrowInfo(ExprInvalid,
                              "{} does not support field data type: {}",
                              ProbePolicy::kKindName,
                              expr_->column_.data_type_);
                }
                break;
            case DataType::JSON:
                if constexpr (!ProbePolicy::kSupportsJson) {
                    ThrowInfo(ExprInvalid,
                              "{} does not support field data type: {}",
                              ProbePolicy::kKindName,
                              expr_->column_.data_type_);
                }
                break;
            default:
                ThrowInfo(ExprInvalid,
                          "{} does not support field data type: {}",
                          ProbePolicy::kKindName,
                          expr_->column_.data_type_);
        }
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    // Prefer the raw-data probe. Only when raw field data is absent (a sealed
    // index-only field) fall back to the scalar index's reverse-lookup so the
    // probe still runs; otherwise a forced RawData scan over zero chunks would
    // assert. No membership kind can push its structure into a scalar index,
    // so the index is never used to accelerate the scan — only to recover
    // values on the index-only fallback.
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
        // detects this (num_data_chunk_ == 0, not UseIndexCursor()) and throws
        // a clear SegcoreError instead of asserting.
        exec_path_ = ExprExecPath::RawData;
    }

    bool
    IsSource() const override {
        return true;
    }

    // Membership filters are never index-native: on the index-only fallback
    // they probe per row via ScalarIndex::Reverse_Lookup. The base class
    // treats any non-RawData path as "execute all at once", which would size
    // the batch to the whole segment and materialize an active_count-wide
    // OffsetVector (~400MiB for 100M rows) in a single pass. Force batched
    // execution so the reverse-lookup path stays bounded to batch_size, like
    // MatchExpr.
    bool
    CanExecuteAllAtOnce() const override {
        return false;
    }

    // Exclude membership filters from the FilterBitsNode result cache. The
    // cache key derives from ToString(), a slim summary that CANNOT distinguish
    // two distinct blobs of equal size — caching would let one query reuse
    // another's filter and return wrong rows. Non-cacheability propagates up:
    // any predicate containing a membership filter is not cached. Keeping
    // ToString slim is then safe, and avoids ever hashing/dumping an
    // up-to-tens-of-MiB blob.
    bool
    IsCacheable() const override {
        return false;
    }

    std::string
    ToString() const override {
        return expr_->ToString();
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->column_;
    }

 private:
    template <typename T>
    VectorPtr
    ExecVisitorImpl(EvalCtx& context);

    // Index-only path: recover each value from the scalar index via
    // Reverse_Lookup and probe it exactly as the raw-data path would,
    // reusing the framework's mask-aware reverse-lookup helper.
    template <typename T>
    VectorPtr
    ExecVisitorImplForIndex(EvalCtx& context);

    // Probe a JSON field at the column's nested path, hashing each value by
    // its runtime type. Data-path only: no scalar index offers a per-row
    // reverse lookup for JSON paths, so IndexSupportsReverseLookup() is false
    // for JSON and DetermineExecPath() never commits to ScalarIndex.
    // Instantiated only for policies with kSupportsJson: as a member template
    // it is never implicitly instantiated when its call site was discarded,
    // nor by the class's explicit instantiations.
    template <typename Dummy = void>
    VectorPtr
    ExecVisitorImplJson(EvalCtx& context);

    // True iff the pinned scalar index is usable for the per-row reverse-
    // lookup probe: it must (1) expose stored values via Reverse_Lookup
    // (HasRawData()) AND (2) do so cheaply. A BITMAP index without its offset
    // cache reverse-looks-up in O(cardinality) per row, which would turn the
    // index-only probe into an O(rows * cardinality) scan — exclude it so the
    // filter falls through to the clear "no usable index" error instead of
    // silently running billions of checks.
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
            case DataType::VARCHAR:
                if constexpr (ProbePolicy::kSupportsVarChar) {
                    return IndexUsableForReverseLookup<std::string>();
                }
                return false;
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

 private:
    std::shared_ptr<const LogicalExpr> expr_;
    ProbePolicy probe_;
};

// The two kinds of the membership family, under their historical class names:
// the factory (Expr.cpp) and every log line keep working unchanged.
using PhyBloomFilterExpr =
    PhyMembershipFilterExpr<milvus::expr::BloomFilterExpr,
                            BloomMembershipProbe>;
using PhyRoaringFilterExpr =
    PhyMembershipFilterExpr<milvus::expr::RoaringFilterExpr,
                            RoaringMembershipProbe>;

}  // namespace exec
}  // namespace milvus
