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
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/mol_c.h"
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

    auto execute_sub_batch = [this](const auto* data,
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