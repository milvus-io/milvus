// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "MolFunctionFilterExpr.h"
#include "cachinglayer/Utils.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/MolCache.h"
#include "common/Types.h"
#include "common/mol_c.h"
#include "index/MolPatternIndex.h"
#include "knowhere/comp/brute_force.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "log/Log.h"
#include "pb/plan.pb.h"
#include <fmt/core.h>

namespace milvus {
namespace exec {

void
PhyMolFunctionFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    AssertInfo(expr_->column_.data_type_ == DataType::MOL,
               "unsupported data type: {}",
               expr_->column_.data_type_);
    // MOL evaluation uses its own raw-data scan with an optional pattern-FP
    // pre-filter, so it stays on SegmentExpr's RawData cursor path.
    result = EvalForDataSegment();
}

bool
PhyMolFunctionFilterExpr::TryMolPatternIndex() {
    // Try to pin the mol field's own index (MOL_PATTERN)
    auto pinned = segment_->PinIndex(op_ctx_, field_id_);
    if (pinned.empty()) {
        LOG_DEBUG("TryMolPatternIndex: PinIndex returned empty for field {}",
                 field_id_.get());
        return false;
    }

    const auto* mol_index =
        dynamic_cast<const index::MolPatternIndex<std::string>*>(
            pinned[0].get());
    if (!mol_index) {
        LOG_DEBUG("TryMolPatternIndex: dynamic_cast failed for field {}, "
                 "pinned count: {}",
                 field_id_.get(),
                 pinned.size());
        return false;
    }

    int32_t dim = mol_index->Dim();
    int32_t byte_size = (dim + 7) / 8;

    // Generate query PatternFP from SMILES
    auto fp_result =
        GeneratePatternFingerprint(expr_->smiles_.c_str(), dim);
    if (fp_result.error_code != MOL_SUCCESS || fp_result.data == nullptr ||
        fp_result.size != static_cast<size_t>(byte_size)) {
        FreeMolDataResult(&fp_result);
        // FP generation failed — mark all as candidates (conservative)
        fp_candidates_.set();
        return true;
    }
    query_fingerprint_.assign(fp_result.data, fp_result.data + byte_size);
    FreeMolDataResult(&fp_result);

    // Use knowhere BruteForce RangeSearch for sub/superstructure screening
    knowhere::Json search_cfg;
    search_cfg[knowhere::meta::METRIC_TYPE] =
        (expr_->op_ ==
         proto::plan::MolFunctionFilterExpr_MolOp_Substructure)
            ? knowhere::metric::SUBSTRUCTURE
            : knowhere::metric::SUPERSTRUCTURE;
    search_cfg[knowhere::meta::RADIUS] = 1.0f;
    search_cfg[knowhere::meta::RANGE_SEARCH_K] = -1;

    auto query_ds =
        knowhere::GenDataSet(1, dim, query_fingerprint_.data());
    // Read published row count (acquire) then access fp data.
    // The buffer is pre-allocated, so the pointer is stable.
    auto fp_row_count = mol_index->RowCount();
    auto fp_data = mol_index->GetFPData();
    if (fp_row_count <= 0 || fp_data == nullptr) {
        fp_candidates_.set();
        return true;
    }
    auto base_ds = knowhere::GenDataSet(
        fp_row_count, dim, fp_data);
    BitsetView empty_bitset;

    auto result = knowhere::BruteForce::RangeSearch<knowhere::bin1>(
        base_ds, query_ds, search_cfg, empty_bitset, op_ctx_);
    if (!result.has_value()) {
        LOG_WARN(
            "MolPatternIndex brute-force pre-filter failed, mark all as "
            "candidates: {}, {}",
            knowhere::Status2String(result.error()),
            result.what());
        fp_candidates_.set();
        return true;
    }

    auto lims = result.value()->GetLims();
    auto ids = result.value()->GetIds();
    int64_t hit_count =
        lims != nullptr ? static_cast<int64_t>(lims[1]) : 0;
    for (int64_t i = 0; i < hit_count; ++i) {
        auto offset = ids[i];
        if (offset >= 0 && offset < active_count_) {
            fp_candidates_[offset] = true;
        }
    }

    // Rows beyond index coverage (e.g. nullable/default rows not in binlog)
    // must be conservatively marked as candidates to avoid false negatives.
    for (int64_t i = fp_row_count; i < active_count_; ++i) {
        fp_candidates_[i] = true;
    }

    LOG_DEBUG(
        "MolPatternIndex pre-filter: {}/{} candidates passed "
        "(index covers {}/{})",
        hit_count,
        active_count_,
        fp_row_count,
        active_count_);
    return true;
}

void
PhyMolFunctionFilterExpr::SearchFingerprintIndex() {
    if (fp_candidates_cached_) {
        return;
    }

    fp_candidates_.resize(active_count_);
    fp_candidates_.reset();

    if (!TryMolPatternIndex()) {
        // No index available — conservatively mark all rows as candidates
        fp_candidates_.set();
    }
    fp_candidates_cached_ = true;
}

VectorPtr
PhyMolFunctionFilterExpr::EvalForDataSegment() {
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto res_vec = std::make_shared<ColumnVector>(
        TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    valid_res.set();

    // Cache query molecule handle from SMILES (once, reused across all rows)
    if (!query_mol_) {
        query_mol_ = ParseSMILESToMol(expr_->smiles_.c_str());
        if (!query_mol_) {
            ThrowInfo(ErrorCode::ExprInvalid,
                      "Invalid SMILES in mol_contains: \"{}\"",
                      expr_->smiles_);
        }
    }

    // Run fingerprint pre-filter once per segment
    SearchFingerprintIndex();

    // Track the global offset for fingerprint candidate lookup
    auto current_chunk = current_data_chunk_;
    auto current_chunk_pos = current_data_chunk_pos_;
    int64_t global_offset = 0;
    if (segment_->is_chunked()) {
        global_offset =
            segment_->num_rows_until_chunk(field_id_, current_chunk) +
            current_chunk_pos;
    } else {
        global_offset =
            current_chunk * size_per_chunk_ + current_chunk_pos;
    }

    bool has_fp = fp_candidates_cached_;

    // Try to get lazy ROMol cache for this segment+field
    auto* mol_cache = SimpleMolCacheManager::Instance().GetCache(
        segment_->get_segment_id(), field_id_);

    auto execute_sub_batch =
        [this, has_fp, &global_offset, mol_cache](
                                 const auto* data,
                                 const bool* valid_data,
                                 const int32_t* offsets,
                                 const int size,
                                 TargetBitmapView res,
                                 TargetBitmapView valid_res) {
        if (data == nullptr) {
            return;
        }

        // Acquire read lock once for the entire batch if cache available
        std::optional<std::shared_lock<std::shared_mutex>> cache_lock;
        if (mol_cache) {
            cache_lock.emplace(mol_cache->AcquireReadLock());
        }

        for (int i = 0; i < size; ++i) {
            if (valid_data != nullptr && !valid_data[i]) {
                res[i] = valid_res[i] = false;
                global_offset++;
                continue;
            }
            // Fingerprint pre-filter: skip rows that don't pass
            if (has_fp && global_offset < (int64_t)fp_candidates_.size() &&
                !fp_candidates_[global_offset]) {
                res[i] = false;
                global_offset++;
                continue;
            }

            int match_result;
            if (mol_cache) {
                auto state =
                    mol_cache->GetStateByOffsetUnsafe(global_offset);
                if (state == milvus::exec::MolCacheEntryState::kUnavailable) {
                    res[i] = false;
                    global_offset++;
                    continue;
                }

                auto cached_mol = state ==
                                          milvus::exec::MolCacheEntryState::
                                              kReady
                                      ? mol_cache->GetByOffsetUnsafe(
                                            global_offset)
                                      : nullptr;

                if (state ==
                    milvus::exec::MolCacheEntryState::kUninitialized) {
                    cache_lock->unlock();

                    const auto* row_data =
                        reinterpret_cast<const uint8_t*>(data[i].data());
                    auto row_size = data[i].size();
                    auto parsed_mol = ParsePickleToMol(row_data, row_size);
                    auto [published_mol, published_state] =
                        mol_cache->PublishByOffset(global_offset,
                                                   parsed_mol);

                    cache_lock->lock();

                    if (published_state !=
                            milvus::exec::MolCacheEntryState::kReady ||
                        !published_mol) {
                        res[i] = false;
                        global_offset++;
                        continue;
                    }
                    cached_mol = published_mol;
                }

                if (expr_->op_ ==
                    proto::plan::
                        MolFunctionFilterExpr_MolOp_Substructure) {
                    match_result = HasSubstructMatchHandles(
                        cached_mol, query_mol_);
                } else {
                    match_result = HasSubstructMatchHandles(
                        query_mol_, cached_mol);
                }
            } else {
                // Fallback: deserialize pickle on-the-fly
                const auto* row_data =
                    reinterpret_cast<const uint8_t*>(data[i].data());
                auto row_size = data[i].size();
                if (expr_->op_ ==
                    proto::plan::
                        MolFunctionFilterExpr_MolOp_Substructure) {
                    match_result = HasSubstructMatchWithQuery(
                        row_data, row_size, query_mol_);
                } else {
                    match_result = HasSubstructMatchWithMol(
                        query_mol_, row_data, row_size);
                }
            }
            if (match_result < 0) {
                LOG_WARN("mol_contains exact check error at row {}: code {}", global_offset, match_result);
            }
            res[i] = (match_result == 1);
            global_offset++;
        }
    };

    auto execute_sub_batch_str =
        [&execute_sub_batch](const std::string* data,
                             const bool* valid_data,
                             const int32_t* offsets,
                             const int size,
                             TargetBitmapView res,
                             TargetBitmapView valid_res) {
            execute_sub_batch(data, valid_data, offsets, size, res, valid_res);
        };

    auto execute_sub_batch_sv =
        [&execute_sub_batch](const std::string_view* data,
                             const bool* valid_data,
                             const int32_t* offsets,
                             const int size,
                             TargetBitmapView res,
                             TargetBitmapView valid_res) {
            execute_sub_batch(data, valid_data, offsets, size, res, valid_res);
        };

    int64_t processed_size;
    if (segment_->type() == SegmentType::Growing &&
        !storage::MmapManager::GetInstance()
             .GetMmapConfig()
             .growing_enable_mmap) {
        processed_size = ProcessDataChunks<std::string>(
            execute_sub_batch_str, std::nullptr_t{}, res, valid_res);
    } else {
        processed_size = ProcessDataChunks<std::string_view>(
            execute_sub_batch_sv, std::nullptr_t{}, res, valid_res);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

}  //namespace exec
}  // namespace milvus
