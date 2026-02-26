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
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Types.h"
#include "common/mol_c.h"
#include "index/VectorIndex.h"
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
    const auto& fp_type = expr_->fingerprint_type_;
    int32_t dim = expr_->fingerprint_dim_;
    int32_t byte_size = (dim + 7) / 8;

    MolDataResult result;
    if (fp_type == "morgan") {
        result = GenerateMorganFingerprint(
            expr_->smiles_.c_str(), expr_->morgan_radius_, dim);
    } else if (fp_type == "maccs") {
        result = GenerateMACCSFingerprint(expr_->smiles_.c_str());
    } else if (fp_type == "rdkit") {
        result = GenerateRDKitFingerprint(
            expr_->smiles_.c_str(),
            expr_->rdkit_min_path_,
            expr_->rdkit_max_path_,
            dim);
    } else {
        return;  // unknown type, skip pre-filter
    }

    if (result.error_code != MOL_SUCCESS) {
        FreeMolDataResult(&result);
        return;  // fingerprint generation failed, skip pre-filter
    }

    query_fingerprint_.assign(result.data, result.data + byte_size);
    FreeMolDataResult(&result);
    query_fp_cached_ = true;
}

void
PhyMolFunctionFilterExpr::FallbackRawDataPreFilter() {
    // Read raw BinaryVector data and do inline bit subset check.
    auto fp_field_id = FieldId(expr_->fingerprint_field_id_);
    int32_t dim = expr_->fingerprint_dim_;
    int32_t byte_size = (dim + 7) / 8;

    int64_t num_chunks = segment_->num_chunk(fp_field_id);
    int64_t row_offset = 0;
    for (int64_t chunk_id = 0; chunk_id < num_chunks; ++chunk_id) {
        auto chunk_size = segment_->chunk_size(fp_field_id, chunk_id);
        auto pw = segment_->chunk_data<uint8_t>(
            op_ctx_, fp_field_id, chunk_id);
        auto span = pw.get();
        const uint8_t* chunk_data = span.data();

        for (int64_t j = 0; j < chunk_size && row_offset < active_count_;
             ++j, ++row_offset) {
            const uint8_t* row_fp = chunk_data + j * byte_size;
            // SUBSTRUCTURE: query is substructure of row
            //   → query fp bits must be subset of row fp bits
            //   → (query & row) == query
            // SUPERSTRUCTURE: row is substructure of query
            //   → row fp bits must be subset of query fp bits
            //   → (query & row) == row
            bool match = true;
            bool is_sub = (expr_->op_ ==
                proto::plan::MolFunctionFilterExpr_MolOp_Substructure);
            for (int32_t b = 0; b < byte_size; ++b) {
                uint8_t q = query_fingerprint_[b];
                uint8_t r = row_fp[b];
                const uint8_t& ref = is_sub ? q : r;
                if ((q & r) != ref) {
                    match = false;
                    break;
                }
            }
            if (match) {
                fp_candidates_[row_offset] = true;
            }
        }
    }
}

void
PhyMolFunctionFilterExpr::SearchFingerprintIndex() {
    if (fp_candidates_cached_ || !expr_->HasFingerprintPreFilter()) {
        return;
    }

    fp_candidates_.resize(active_count_);
    fp_candidates_.reset();
    EnsureQueryFingerprint();
    if (!query_fp_cached_) {
        // Could not generate fingerprint, mark all as candidates
        fp_candidates_.resize(active_count_);
        fp_candidates_.set();
        fp_candidates_cached_ = true;
        return;
    }

    auto fp_field_id = FieldId(expr_->fingerprint_field_id_);

    // Try to use vector index for fast pre-filter
    auto* indexing_record = segment_->GetSealedIndexingRecord();
    if (indexing_record == nullptr || !indexing_record->is_ready(fp_field_id)) {
        FallbackRawDataPreFilter();
        fp_candidates_cached_ = true;
        return;
    }

    auto field_indexing = indexing_record->get_field_indexing(fp_field_id);
    auto accessor =
        SemiInlineGet(field_indexing->indexing_->PinCells(nullptr, {0}));
    auto* vec_index =
        dynamic_cast<index::VectorIndex*>(accessor->get_cell_of(0));

    if (vec_index == nullptr) {
        FallbackRawDataPreFilter();
        fp_candidates_cached_ = true;
        return;
    }

    // Construct Range Search parameters
    int32_t dim = expr_->fingerprint_dim_;
    SearchInfo search_info;
    search_info.field_id_ = fp_field_id;
    search_info.topk_ = active_count_;
    search_info.metric_type_ =
        (expr_->op_ ==
         proto::plan::MolFunctionFilterExpr_MolOp_Substructure)
            ? knowhere::metric::SUBSTRUCTURE
            : knowhere::metric::SUPERSTRUCTURE;
    // radius=1.0 triggers range search path.
    // SUBSTRUCTURE match → distance=0.0, range_filter defaults to 0.
    // Condition: range_filter(0) <= distance(0) < radius(1) → pass.
    search_info.search_params_[knowhere::meta::RADIUS] = 1.0f;

    auto dataset =
        knowhere::GenDataSet(1, dim, query_fingerprint_.data());
    BitsetView empty_bitset;
    SearchResult result;
    vec_index->Query(dataset, search_info, empty_bitset, op_ctx_, result);

    // Convert results to bitmap
    for (auto offset : result.seg_offsets_) {
        if (offset >= 0 && offset < active_count_) {
            fp_candidates_[offset] = true;
        }
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

    bool has_fp = fp_candidates_cached_ &&
                  expr_->HasFingerprintPreFilter();

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