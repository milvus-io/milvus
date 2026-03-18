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
    result = EvalForDataSegment();
}

void
PhyMolFunctionFilterExpr::EnsureQueryFingerprint() {
    if (query_fp_cached_) {
        return;
    }
    if (expr_->fingerprint_type_ != "pattern") {
        return;
    }

    int32_t dim = expr_->fingerprint_dim_;
    int32_t byte_size = (dim + 7) / 8;

    auto result = GeneratePatternFingerprint(expr_->smiles_.c_str(), dim);

    if (result.error_code != MOL_SUCCESS || result.data == nullptr ||
        result.size != static_cast<size_t>(byte_size)) {
        FreeMolDataResult(&result);
        return;  // fingerprint generation failed, skip pre-filter
    }

    query_fingerprint_.assign(result.data, result.data + byte_size);
    FreeMolDataResult(&result);
    query_fp_cached_ = true;
}

void
PhyMolFunctionFilterExpr::FallbackRawDataPreFilter() {
    auto fp_field_id = FieldId(expr_->fingerprint_field_id_);
    int32_t dim = expr_->fingerprint_dim_;
    if (dim <= 0 || dim % 8 != 0) {
        LOG_WARN("Skip fingerprint pre-filter because pattern dim is not byte-aligned: {}",
                 dim);
        fp_candidates_.set();
        return;
    }

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
    BitsetView empty_bitset;

    int64_t num_chunks = segment_->num_chunk(fp_field_id);
    int64_t row_begin = 0;
    for (int64_t chunk_id = 0;
         chunk_id < num_chunks && row_begin < active_count_;
         ++chunk_id) {
        auto chunk_size = segment_->chunk_size(fp_field_id, chunk_id);
        auto rows = std::min<int64_t>(chunk_size, active_count_ - row_begin);
        if (rows <= 0) {
            break;
        }

        auto pw = segment_->chunk_data<uint8_t>(
            op_ctx_, fp_field_id, chunk_id);
        auto span = pw.get();
        auto base_ds = knowhere::GenDataSet(rows, dim, span.data());
        base_ds->SetTensorBeginId(row_begin);

        auto result = knowhere::BruteForce::RangeSearch<knowhere::bin1>(
            base_ds, query_ds, search_cfg, empty_bitset, op_ctx_);
        if (!result.has_value()) {
            LOG_WARN("Fingerprint brute-force pre-filter failed, disable pre-filter for this segment: {}, {}",
                     knowhere::Status2String(result.error()),
                     result.what());
            fp_candidates_.set();
            return;
        }

        auto lims = result.value()->GetLims();
        auto ids = result.value()->GetIds();
        int64_t hit_count = lims != nullptr ? static_cast<int64_t>(lims[1]) : 0;
        for (int64_t i = 0; i < hit_count; ++i) {
            auto offset = ids[i];
            if (offset >= 0 && offset < active_count_) {
                fp_candidates_[offset] = true;
            }
        }

        row_begin += rows;
    }
}

bool
PhyMolFunctionFilterExpr::TryMolPatternIndex() {
    // Try to pin the mol field's own index (MOL_PATTERN)
    auto pinned = segment_->PinIndex(op_ctx_, field_id_);
    if (pinned.empty()) {
        return false;
    }

    const auto* mol_index =
        dynamic_cast<const index::MolPatternIndex<std::string>*>(
            pinned[0].get());
    if (!mol_index) {
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
    query_fp_cached_ = true;

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
    // Snapshot fp data and row count atomically
    auto fp_snap = mol_index->SnapshotFPData();
    if (fp_snap.row_count <= 0 || fp_snap.data == nullptr) {
        fp_candidates_.set();
        return true;
    }
    auto base_ds = knowhere::GenDataSet(
        fp_snap.row_count, dim, fp_snap.data);
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
    for (int64_t i = fp_snap.row_count; i < active_count_; ++i) {
        fp_candidates_[i] = true;
    }

    LOG_DEBUG(
        "MolPatternIndex pre-filter: {}/{} candidates passed "
        "(index covers {}/{})",
        hit_count,
        active_count_,
        fp_snap.row_count,
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

    // Priority 1: try MOL_PATTERN index on the mol field itself
    if (TryMolPatternIndex()) {
        fp_candidates_cached_ = true;
        return;
    }

    // Priority 2: fallback to separate fingerprint field (legacy path)
    if (!expr_->HasFingerprintPreFilter()) {
        return;
    }

    EnsureQueryFingerprint();
    if (!query_fp_cached_) {
        // Could not generate fingerprint, mark all as candidates
        fp_candidates_.set();
        fp_candidates_cached_ = true;
        return;
    }

    FallbackRawDataPreFilter();
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

    // Cache query pickle from SMILES (once per segment)
    if (!query_pickle_cached_) {
        auto result = ConvertSMILESToPickle(expr_->smiles_.c_str());
        AssertInfo(result.error_code == MOL_SUCCESS,
                   "Failed to convert query SMILES to pickle: {}",
                   result.error_msg ? result.error_msg : "unknown error");
        query_pickle_.assign(reinterpret_cast<const char*>(result.data),
                             result.size);
        FreeMolDataResult(&result);
        query_pickle_cached_ = true;
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

    auto execute_sub_batch = [this, has_fp, &global_offset](
                                 const auto* data,
                                 const bool* valid_data,
                                 const int32_t* offsets,
                                 const int size,
                                 TargetBitmapView res,
                                 TargetBitmapView valid_res) {
        if (data == nullptr) {
            return;
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

            const auto* row_data =
                reinterpret_cast<const uint8_t*>(data[i].data());
            auto row_size = data[i].size();
            const auto* query_data =
                reinterpret_cast<const uint8_t*>(query_pickle_.data());
            auto query_size = query_pickle_.size();

            int match_result;
            if (expr_->op_ ==
                proto::plan::MolFunctionFilterExpr_MolOp_Substructure) {
                match_result = HasSubstructMatch(
                    row_data, row_size, query_data, query_size);
            } else {
                match_result = HasSubstructMatch(
                    query_data, query_size, row_data, row_size);
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
