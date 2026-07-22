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

#include "BloomFilterExpr.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>

#include "common/Json.h"
#include "common/Types.h"
#include "storage/MmapManager.h"

namespace milvus {
namespace exec {

// Little-endian field loads come from common/BloomFilterEnvelope.h (the single
// C++ copy of the MBF1 layout). The header is untrusted request content: every
// field is validated against the actual blob length before any use and nothing
// is allocated from it.
using bloom_envelope::LoadU16LE;
using bloom_envelope::LoadU32LE;

SplitBlockBloomFilterView
SplitBlockBloomFilterView::Parse(std::string_view blob) {
    if (blob.size() < kHeaderSize) {
        ThrowInfo(ExprInvalid,
                  "bloom filter blob too short: {} bytes, need at least {}",
                  blob.size(),
                  kHeaderSize);
    }
    // Defensive upper bound on the whole envelope, checked before any field
    // is read. The proxy is the operator-tunable gate: it rejects blobs above
    // proxy.maxBloomFilterSize (default 32 MiB) at plan-build time and
    // validates the envelope via sbbf.Parse (same 128 MB format cap mirroring
    // Arrow's kMaximumBloomFilterBytes); in practice the blob is also bounded
    // by the gRPC transport limit. But a hand-crafted plan can reach segcore
    // directly; cap the blob at header + kMaxFilterBytes so a hostile plan
    // cannot force an unbounded body.
    // The request content (the client/proxy-supplied blob) is to blame, so this
    // is an input/parameter error, never a system error.
    constexpr uint64_t kMaxBlobSize = kHeaderSize + kMaxFilterBytes;
    if (blob.size() > kMaxBlobSize) {
        ThrowInfo(ExprInvalid,
                  "bloom filter blob too large: {} bytes, exceeds max {} "
                  "(header {} + body {})",
                  blob.size(),
                  kMaxBlobSize,
                  kHeaderSize,
                  kMaxFilterBytes);
    }
    const auto* p = reinterpret_cast<const uint8_t*>(blob.data());
    if (blob.substr(0, 4) != "MBF1") {
        ThrowInfo(ExprInvalid,
                  "bloom filter blob has invalid magic, expected \"MBF1\"");
    }
    if (auto version = LoadU16LE(p + 4); version != kVersion) {
        ThrowInfo(ExprInvalid,
                  "unsupported bloom filter version {}, expected {}",
                  version,
                  kVersion);
    }
    if (auto algo = LoadU16LE(p + 6); algo != kAlgoParquetSbbfXxh64) {
        ThrowInfo(ExprInvalid,
                  "unsupported bloom filter algo {}, expected {}",
                  algo,
                  kAlgoParquetSbbfXxh64);
    }
    if (auto reserved = LoadU32LE(p + 28); reserved != 0) {
        ThrowInfo(ExprInvalid,
                  "bloom filter reserved field must be 0, got {}",
                  reserved);
    }
    const uint32_t num_blocks = LoadU32LE(p + 24);
    // SBBF invariant (parquet OptimalNumOfBytes): the filter body is a
    // power-of-two number of bytes in [32, kMaxFilterBytes], hence
    // num_blocks is a power of two in [1, kMaxFilterBytes / 32].
    constexpr uint64_t kMaxBlocks = kMaxFilterBytes / kBytesPerBlock;
    if (num_blocks == 0 || (num_blocks & (num_blocks - 1)) != 0 ||
        num_blocks > kMaxBlocks) {
        ThrowInfo(ExprInvalid,
                  "bloom filter num_blocks {} is not a power of two in "
                  "[1, {}]",
                  num_blocks,
                  kMaxBlocks);
    }
    const uint64_t body_len = blob.size() - kHeaderSize;
    if (body_len != static_cast<uint64_t>(num_blocks) * kBytesPerBlock) {
        ThrowInfo(ExprInvalid,
                  "bloom filter body length {} does not match num_blocks {} "
                  "(want {} bytes)",
                  body_len,
                  num_blocks,
                  static_cast<uint64_t>(num_blocks) * kBytesPerBlock);
    }
    SplitBlockBloomFilterView view;
    view.body_ = p + kHeaderSize;
    view.num_blocks_ = num_blocks;
    return view;
}

void
PhyBloomFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    WaitPrefetch();
    // Honor iterative-filter candidate offsets: when the upstream passes an
    // offset input, ExecVisitorImpl must evaluate those candidate rows via
    // ProcessDataByOffsets rather than the first N rows sequentially. Without
    // this, a filtered/iterative search would test the wrong rows and could
    // both drop true members and pass out-of-set rows, breaking the one-sided
    // error guarantee. Mirrors TermExpr.
    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    auto data_type = expr_->column_.data_type_;
    switch (data_type) {
        case DataType::INT8: {
            result = ExecVisitorImpl<int8_t>(context);
            break;
        }
        case DataType::INT16: {
            result = ExecVisitorImpl<int16_t>(context);
            break;
        }
        case DataType::INT32: {
            result = ExecVisitorImpl<int32_t>(context);
            break;
        }
        case DataType::INT64: {
            result = ExecVisitorImpl<int64_t>(context);
            break;
        }
        case DataType::VARCHAR: {
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                result = ExecVisitorImpl<std::string>(context);
            } else {
                result = ExecVisitorImpl<std::string_view>(context);
            }
            break;
        }
        case DataType::JSON: {
            result = ExecVisitorImplJson(context);
            break;
        }
        default:
            ThrowInfo(ExprInvalid,
                      "bloom_match does not support field data type: {}",
                      data_type);
    }
}

template <typename T>
VectorPtr
PhyBloomFilterExpr::ExecVisitorImpl(EvalCtx& context) {
    auto* input = context.get_offset_input();

    // Index-only sealed field: no raw data to scan. DetermineExecPath()
    // committed to ScalarIndex iff a reverse-lookup-capable index was pinned;
    // route the probe through Reverse_Lookup. If it did NOT (no usable index),
    // fail with a clear SegcoreError rather than reading zero rows and tripping
    // the batch-size assertion. Field data being absent is a load/state
    // condition, not the request's fault, so this is a System error (retriable).
    // UseIndexCursor() (== exec_path_ ScalarIndex) is set only in that
    // index-only fallback, so an empty-but-loaded segment never lands here.
    if (UseIndexCursor()) {
        return ExecVisitorImplForIndex<T>(context);
    }
    if (segment_->type() == SegmentType::Sealed && !has_field_data_at_init_) {
        ThrowInfo(
            FieldNotLoaded,
            "bloom_match cannot evaluate field {}: raw field data is not "
            "loaded and no scalar index with a cheap per-row reverse lookup "
            "is available (a BITMAP index without its offset cache is "
            "excluded because it reverse-looks-up in O(cardinality) per row; "
            "load the raw field data or set "
            "queryNode.indexOffsetCacheEnabled=true)",
            field_id_.get());
    }

    const auto& bitmap_input = context.get_bitmap_input();

    auto real_batch_size = GetNextRealBatchSize(input, false);
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    const auto& filter = filter_;
    int processed_cursor = 0;
    auto execute_sub_batch =
        [&processed_cursor, &bitmap_input, &
         filter ]<FilterType filter_type = FilterType::sequential>(
            const T* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res) {
        // data == nullptr means the chunk was skipped upstream; only the
        // bitmap_input cursor needs to advance.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        bool has_bitmap_input = !bitmap_input.empty();
        for (int i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                // NULL never matches, under either bloom_match or
                // not bloom_match — same three-valued behavior as TermExpr.
                res[i] = valid_res[i] = false;
                continue;
            }
            if (has_bitmap_input && !bitmap_input[processed_cursor + i]) {
                continue;
            }
            res[i] = filter.TestScalar(data[offset]);
        }
        processed_cursor += size;
    };

    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<T>(
            execute_sub_batch, std::nullptr_t{}, input, res, valid_res);
    } else {
        processed_size = ProcessDataChunks<T>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename T>
VectorPtr
PhyBloomFilterExpr::ExecVisitorImplForIndex(EvalCtx& context) {
    // Index-only path: recover each value from the scalar index via
    // Reverse_Lookup and probe the SBBF, reusing ProcessIndexLookupByOffsets
    // (the framework's reverse-lookup helper). It applies FilterType::random
    // per row, so the same execute_sub_batch as the raw-data path works here.
    auto* input = context.get_offset_input();

    auto real_batch_size = GetNextRealBatchSize(input, false);
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    const auto& filter = filter_;
    auto execute_sub_batch =
        [&filter]<FilterType filter_type = FilterType::sequential>(
            const T* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res) {
        // data == nullptr means the framework decided this row's result upstream
        // (a NULL reverse lookup, for which it already set res/valid_res=false);
        // nothing to test. NULLs still never match, mirroring the raw path.
        if (data == nullptr) {
            return;
        }
        for (int i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = filter.TestScalar(data[offset]);
        }
    };

    int64_t processed_size;
    if (has_offset_input_) {
        // ProcessDataByOffsets routes to ProcessIndexLookupByOffsets because
        // UseIndexCursor() && num_data_chunk_ == 0, which recovers each value
        // via Reverse_Lookup and drives execute_sub_batch per row.
        processed_size = ProcessDataByOffsets<T>(
            execute_sub_batch, std::nullptr_t{}, input, res, valid_res);
    } else {
        // No offset input: reverse-look-up the contiguous global row range
        // [current_index_chunk_pos_, +real_batch_size) for this batch.
        OffsetVector batch_offsets(real_batch_size);
        auto start = current_index_chunk_pos_;
        for (int64_t i = 0; i < real_batch_size; ++i) {
            batch_offsets[i] = static_cast<int32_t>(start + i);
        }
        processed_size = ProcessIndexLookupByOffsets<T>(
            execute_sub_batch, &batch_offsets, res, valid_res);
        // ProcessIndexLookupByOffsets is stateless; advance the index cursor
        // for the next batch. MoveCursor() honors the has_offset_input_ guard
        // and, on the ScalarIndex path with no raw data, advances only the
        // index cursor.
        MoveCursor();
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: bloom_match index path processed rows {} not "
               "equal expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

VectorPtr
PhyBloomFilterExpr::ExecVisitorImplJson(EvalCtx& context) {
    auto* input = context.get_offset_input();

    // JSON paths are data-path only: DetermineExecPath() never commits to
    // ScalarIndex for JSON (no per-row reverse lookup exists for a JSON path),
    // so an index-only sealed JSON field cannot be probed at all.
    if (segment_->type() == SegmentType::Sealed && !has_field_data_at_init_) {
        ThrowInfo(FieldNotLoaded,
                  "bloom_match cannot evaluate JSON field {}: raw field data "
                  "is not loaded, and a JSON path has no scalar index with a "
                  "per-row reverse lookup; load the raw JSON field data",
                  field_id_.get());
    }

    const auto& bitmap_input = context.get_bitmap_input();

    auto real_batch_size = GetNextRealBatchSize(input, false);
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    const auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    const auto& filter = filter_;
    int processed_cursor = 0;
    auto execute_sub_batch =
        [&processed_cursor, &bitmap_input, &
         filter ]<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string& pointer) {
        // data == nullptr means the chunk was skipped upstream; only the
        // bitmap_input cursor needs to advance.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        bool has_bitmap_input = !bitmap_input.empty();
        for (int i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                // Whole-row NULL never matches, under either polarity.
                res[i] = valid_res[i] = false;
                continue;
            }
            if (has_bitmap_input && !bitmap_input[processed_cursor + i]) {
                continue;
            }
            // STRICTLY TYPED probe: the hash domain has exactly two kinds,
            // int64 (8-byte LE) and raw UTF-8 bytes, and a JSON value is
            // hashed only when it is stored AS that type:
            //   - string -> raw UTF-8 bytes (same as a VARCHAR probe)
            //   - int64  -> 8-byte-LE int64 hash
            //   - double / uint64-beyond-int64 -> NEVER a member (res=false,
            //     valid=true, so `not bloom_match` returns the row). This is a
            //     DELIBERATE divergence from exact `in`, whose JSON semantics
            //     unify 5.0 == 5: probing a stored 5.0 against an int64
            //     member 5 does NOT match here. Rationale: no numeric
            //     canonicalization rule to keep bit-identical across every
            //     prober/SDK forever — data whose integers may be
            //     float-encoded should use a typed field or normalize on
            //     write. (simdjson yields is_int64 for every integer that
            //     fits int64; is_uint64 only appears beyond INT64_MAX.)
            //   - missing key / JSON null / bool / object / array -> the
            //     probe has no value: res=false AND valid=false (three-valued,
            //     same as TermExpr's error semantics), so neither polarity
            //     matches.
            const auto& json = data[offset];
            auto str = json.template at<std::string_view>(pointer);
            if (!str.error()) {
                res[i] =
                    filter.TestBytes(str.value().data(), str.value().size());
                continue;
            }
            auto num = json.at_numeric(pointer);
            if (num.error()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            if (auto n = num.value(); n.is_int64()) {
                res[i] = filter.TestInt64(n.get_int64());
            }
        }
        processed_cursor += size;
    };

    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<milvus::Json>(execute_sub_batch,
                                                            std::nullptr_t{},
                                                            input,
                                                            res,
                                                            valid_res,
                                                            pointer);
    } else {
        processed_size = ProcessDataChunks<milvus::Json>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res, pointer);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: bloom_match json path processed rows {} not "
               "equal expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}
}  // namespace exec
}  // namespace milvus
