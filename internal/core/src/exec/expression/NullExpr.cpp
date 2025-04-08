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

#include "NullExpr.h"
#include <memory>
#include <utility>
#include "common/Array.h"
#include "common/Types.h"
#include "log/Log.h"
#include "query/Utils.h"
namespace milvus {
namespace exec {

void
PhyNullExpr::Eval(EvalCtx& context, VectorPtr& result) {
    auto input = context.get_offset_input();
    switch (expr_->column_.data_type_) {
        case DataType::BOOL: {
            result = ExecVisitorImpl<bool>(input);
            break;
        }
        case DataType::INT8: {
            result = ExecVisitorImpl<int8_t>(input);
            break;
        }
        case DataType::INT16: {
            result = ExecVisitorImpl<int16_t>(input);
            break;
        }
        case DataType::INT32: {
            result = ExecVisitorImpl<int32_t>(input);
            break;
        }
        case DataType::INT64: {
            result = ExecVisitorImpl<int64_t>(input);
            break;
        }
        case DataType::FLOAT: {
            result = ExecVisitorImpl<float>(input);
            break;
        }
        case DataType::DOUBLE: {
            result = ExecVisitorImpl<double>(input);
            break;
        }
        case DataType::VARCHAR: {
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                result = ExecVisitorImpl<std::string>(input);
            } else {
                result = ExecVisitorImpl<std::string_view>(input);
            }
            break;
        }
        case DataType::JSON: {
            result = ExecVisitorImpl<Json>(input);
            break;
        }
        case DataType::ARRAY: {
            result = ExecVisitorImpl<ArrayView>(input);
            break;
        }
        default:
            PanicInfo(DataTypeInvalid,
                      "unsupported data type: {}",
                      expr_->column_.data_type_);
    }
}

template <typename T>
VectorPtr
PhyNullExpr::ExecVisitorImpl(OffsetVector* input) {
    if (auto res = PreCheckNullable(input)) {
        return res;
    }
    auto valid_res =
        (input != nullptr)
            ? ProcessChunksForValidByOffsets<T>(is_index_mode_, *input)
            : ProcessChunksForValid<T>(is_index_mode_);
    TargetBitmap res = valid_res.clone();
    if (expr_->op_ == proto::plan::NullExpr_NullOp_IsNull) {
        res.flip();
    }
    auto res_vec =
        std::make_shared<ColumnVector>(std::move(res), std::move(valid_res));
    return res_vec;
}

// if nullable is false, no need to process chunks
// res is all false when is null, and is all true when is not null
ColumnVectorPtr
PhyNullExpr::PreCheckNullable(OffsetVector* input) {
    if (expr_->column_.nullable_) {
        return nullptr;
    }

    int64_t batch_size;
    if (input != nullptr) {
        batch_size = input->size();
    } else {
        batch_size = precheck_pos_ + batch_size_ >= active_count_
                         ? active_count_ - precheck_pos_
                         : batch_size_;
        precheck_pos_ += batch_size;
    }
    if (cached_precheck_res_ != nullptr &&
        cached_precheck_res_->size() == batch_size) {
        return cached_precheck_res_;
    }

    auto res_vec = std::make_shared<ColumnVector>(TargetBitmap(batch_size),
                                                  TargetBitmap(batch_size));
    TargetBitmapView res(res_vec->GetRawData(), batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), batch_size);
    valid_res.set();
    switch (expr_->op_) {
        case proto::plan::NullExpr_NullOp_IsNull: {
            res.reset();
            break;
        }
        case proto::plan::NullExpr_NullOp_IsNotNull: {
            res.set();
            break;
        }
        default:
            PanicInfo(ExprInvalid,
                      "unsupported null expr type {}",
                      proto::plan::NullExpr_NullOp_Name(expr_->op_));
    }
    cached_precheck_res_ = res_vec;
    return cached_precheck_res_;
}

}  //namespace exec
}  // namespace milvus
