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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/core.h>
#include "xxhash.h"

#include "common/BloomFilterEnvelope.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

// Zero-copy prober over an MBF1-enveloped parquet Split-Block Bloom Filter
// (SBBF) blob. The bit layout and probe algorithm are bit-identical to Arrow
// C++'s parquet::BlockSplitBloomFilter (parquet-format BloomFilter.md) and to
// the Go builder in client/sbbf. Conformance is pinned by the shared golden
// vectors in client/sbbf/testdata/golden_vectors.json.
//
// MBF1 envelope layout (all integers little-endian):
//   offset  size  field
//   0       4     magic "MBF1"
//   4       2     version       (= 1)
//   6       2     algo          (1 = parquet_sbbf_xxh64)
//   8       8     n_declared    (informational)
//   16      8     fpr_declared  (float64, informational)
//   24      4     num_blocks    (body length must equal num_blocks * 32)
//   28      4     reserved      (must be 0)
//   32      ...   body: SBBF blocks
//
// Lifetime: the view aliases the blob bytes owned by the logical
// expr::BloomFilterExpr, which every compiled physical expr holds via
// shared_ptr, so the view never outlives its backing storage.
class SplitBlockBloomFilterView {
 public:
    // Layout constants live in common/BloomFilterEnvelope.h (single C++
    // source of truth for the MBF1 envelope); aliased here for callers.
    static constexpr size_t kHeaderSize = bloom_envelope::kHeaderSize;
    static constexpr size_t kBytesPerBlock = bloom_envelope::kBytesPerBlock;
    static constexpr uint16_t kVersion = bloom_envelope::kVersion;
    static constexpr uint16_t kAlgoParquetSbbfXxh64 =
        bloom_envelope::kAlgoParquetSbbfXxh64;
    static constexpr uint64_t kMaxFilterBytes = bloom_envelope::kMaxFilterBytes;

    SplitBlockBloomFilterView() = default;

    // Validates the MBF1 envelope and returns a zero-copy view over blob.
    // Every header field is checked against the actual blob length before
    // any use, and nothing is allocated from untrusted fields. Malformed
    // input throws SegcoreError{ExprInvalid}: the request content (the
    // client/proxy-supplied blob) is to blame, so this classifies as an
    // input/parameter error, never a system error.
    static SplitBlockBloomFilterView
    Parse(std::string_view blob);

    // Probe: block = ((h >> 32) * num_blocks) >> 32; for word i in 0..7 the
    // checked bit is (uint32(h) * SALT[i]) >> 27; match iff all 8 bits set.
    bool
    Test(uint64_t hash) const {
        const auto block = static_cast<uint32_t>(
            ((hash >> 32) * static_cast<uint64_t>(num_blocks_)) >> 32);
        const uint8_t* block_ptr =
            body_ + static_cast<size_t>(block) * kBytesPerBlock;
        const auto key = static_cast<uint32_t>(hash);
        for (int i = 0; i < 8; ++i) {
            const uint32_t mask = uint32_t(1) << ((key * kSalt[i]) >> 27);
            if ((LoadWordLE(block_ptr + i * 4) & mask) == 0) {
                return false;
            }
        }
        return true;
    }

    // INT8/16/32/64 values are widened to int64 and hashed as their 8-byte
    // little-endian encoding with XXH64(seed=0) — identical to parquet plain
    // encoding for INT64 and to client/sbbf hashInt64.
    bool
    TestInt64(int64_t v) const {
        uint8_t buf[8];
        const auto u = static_cast<uint64_t>(v);
        for (int i = 0; i < 8; ++i) {
            buf[i] = static_cast<uint8_t>(u >> (8 * i));
        }
        return Test(XXH64(buf, sizeof(buf), 0));
    }

    // VARCHAR values hash their raw UTF-8 bytes with XXH64(seed=0) —
    // identical to parquet plain encoding for BYTE_ARRAY and to
    // client/sbbf hashString.
    bool
    TestBytes(const void* data, size_t len) const {
        return Test(XXH64(data, len, 0));
    }

    // Single probe dispatch for a typed scalar value: strings by raw bytes,
    // integers widened to int64. Shared by the raw-data and index-fallback
    // paths so their probe semantics cannot diverge.
    template <typename T>
    bool
    TestScalar(const T& v) const {
        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::string_view>) {
            return TestBytes(v.data(), v.size());
        } else {
            return TestInt64(static_cast<int64_t>(v));
        }
    }

    uint32_t
    num_blocks() const {
        return num_blocks_;
    }

 private:
    static uint32_t
    LoadWordLE(const uint8_t* p) {
        return static_cast<uint32_t>(p[0]) |
               (static_cast<uint32_t>(p[1]) << 8) |
               (static_cast<uint32_t>(p[2]) << 16) |
               (static_cast<uint32_t>(p[3]) << 24);
    }

    // Eight odd constants fixed by the parquet-format spec, mirrored from
    // parquet::BlockSplitBloomFilter::SALT.
    static constexpr uint32_t kSalt[8] = {0x47b6137bU,
                                          0x44974d91U,
                                          0x8824ad5bU,
                                          0xa2b7289dU,
                                          0x705495c7U,
                                          0x2df1424bU,
                                          0x9efc4947U,
                                          0x5c6bfb31U};

    const uint8_t* body_ = nullptr;
    uint32_t num_blocks_ = 0;
};

// Physical expression for `bloom_match` (plan proto BloomFilterExpr).
//
// DATA PATH BY DEFAULT (design doc 20260707, "Probe path"): the probe reads
// raw field data and probes each value against the SBBF, so sealed and growing
// segments behave identically. bloom_match is not an index-native operation
// (the SBBF cannot be pushed into a scalar index), so the index is never used
// to *accelerate* the scan.
//
// INDEX-ONLY FALLBACK: a sealed segment may load a field index-only, leaving
// no raw field data (num_data_chunk_ == 0). In that state a forced RawData
// scan reads zero rows yet the framework still expects active_count_ rows,
// tripping the batch-size assertion (and GetNextBatchSize itself asserts on
// the missing column). To stay correct, DetermineExecPath() commits to
// ExprExecPath::ScalarIndex *only* when raw data is absent but a
// reverse-lookup-capable scalar index is present; the probe then recovers each
// value via ScalarIndex::Reverse_Lookup and probes it exactly as it would raw
// data. When raw data exists (the common case) it stays on RawData.
class PhyBloomFilterExpr : public SegmentExpr {
 public:
    PhyBloomFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::BloomFilterExpr>& expr,
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
          // Envelope parsed and validated exactly once per physical expr;
          // throws SegcoreError{ExprInvalid} on malformed blobs. The view
          // aliases expr_->filter_blob_ (see lifetime note on the view).
          filter_(SplitBlockBloomFilterView::Parse(expr->filter_blob_)) {
        switch (expr_->column_.data_type_) {
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
            case DataType::INT64:
            case DataType::VARCHAR:
            // JSON probes the value at the column's nested path per row,
            // hashing by the value's runtime type (see ExecVisitorImplJson).
            case DataType::JSON:
                break;
            default:
                ThrowInfo(ExprInvalid,
                          "bloom_match does not support field data type: {}",
                          expr_->column_.data_type_);
        }
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    // Prefer the raw-data probe. Only when raw field data is absent (a sealed
    // index-only field) fall back to the scalar index's reverse-lookup so the
    // probe still runs; otherwise a forced RawData scan over zero chunks would
    // assert. See the class comment (INDEX-ONLY FALLBACK).
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

    // bloom_match is never index-native: on the index-only fallback it probes
    // the SBBF per row via ScalarIndex::Reverse_Lookup. The base class treats
    // any non-RawData path as "execute all at once", which would size the batch
    // to the whole segment and materialize an active_count-wide OffsetVector
    // (~400MiB for 100M rows) in a single pass. Force batched execution so the
    // reverse-lookup path stays bounded to batch_size, like MatchExpr.
    bool
    CanExecuteAllAtOnce() const override {
        return false;
    }

    // Exclude bloom_match from the FilterBitsNode result cache. The cache key is
    // derived from ToString(), a slim summary (column + blob length + declared
    // count) that CANNOT distinguish two distinct blobs of equal length and
    // declared count — caching would let one query reuse another's bitmap and
    // return wrong rows. Being non-cacheable propagates up: any predicate that
    // contains a bloom_match is not cached. (Keeping ToString slim is then safe,
    // and avoids ever hashing/dumping the up-to-128 MiB blob.)
    bool
    IsCacheable() const override {
        return false;
    }

    std::string
    ToString() const override {
        return fmt::format("{}", expr_->ToString());
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->column_;
    }

 private:
    template <typename T>
    VectorPtr
    ExecVisitorImpl(EvalCtx& context);

    // Probe the batch via the pinned scalar index's Reverse_Lookup instead of
    // raw field data. Used only on the index-only fallback path
    // (UseIndexCursor()); recovers each value from the index and probes the
    // SBBF, preserving the exact per-row semantics of the raw-data path
    // (NULL never matches). Advances the index cursor by the batch.
    template <typename T>
    VectorPtr
    ExecVisitorImplForIndex(EvalCtx& context);

    // Probe a JSON field at the column's nested path, hashing each value by
    // its runtime type. Data-path only: no scalar index offers a per-row
    // reverse lookup for JSON paths, so IndexSupportsReverseLookup() is false
    // for JSON and DetermineExecPath() never commits to ScalarIndex.
    VectorPtr
    ExecVisitorImplJson(EvalCtx& context);

    // True iff the pinned scalar index is usable for the per-row reverse-lookup
    // probe: it must (1) expose stored values via Reverse_Lookup (HasRawData())
    // AND (2) do so cheaply. bloom_match cannot push the SBBF into an index, so
    // an index without recoverable raw values is useless here; and a BITMAP
    // index without its offset cache reverse-looks-up in O(cardinality) per
    // row, which would turn the index-only probe into an O(rows * cardinality)
    // scan — exclude it so bloom_match falls through to the clear
    // "no usable index" error instead of silently running billions of checks.
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
                return IndexUsableForReverseLookup<std::string>();
            default:
                return false;
        }
    }

    // Both gates for the reverse-lookup path: recoverable raw values, and a
    // cheap (non-O(cardinality)) per-row Reverse_Lookup. STL_SORT (O(1)) and
    // MARISA (O(strlen)) default to fast; BITMAP-without-offset-cache (and a
    // HYBRID wrapping it) report slow and are rejected here.
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
    std::shared_ptr<const milvus::expr::BloomFilterExpr> expr_;
    SplitBlockBloomFilterView filter_;
};

}  // namespace exec
}  // namespace milvus
