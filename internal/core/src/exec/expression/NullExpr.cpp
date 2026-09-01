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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/type_c.h"
#include "opentelemetry/trace/span.h"
#include "pb/plan.pb.h"
#include "segcore/SegmentInterface.h"
#include "storage/MmapManager.h"
#include "storage/Types.h"

namespace milvus {
namespace exec {

void
PhyNullExpr::Eval(EvalCtx& context, VectorPtr& result) {
    WaitPrefetch();
    tracer::AutoSpan span("PhyNullExpr::Eval", tracer::GetRootSpan(), true);
    span.GetSpan()->SetAttribute("data_type",
                                 static_cast<int>(expr_->column_.data_type_));

    auto input = context.get_offset_input();
    SetHasOffsetInput(input != nullptr);
    auto data_type = expr_->column_.data_type_;
    if (expr_->column_.element_level_) {
        data_type = expr_->column_.element_type_;
    }
    switch (data_type) {
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
        case DataType::TIMESTAMPTZ: {
            result = ExecVisitorImpl<int64_t>(input);
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
        case DataType::VECTOR_ARRAY: {
            result = ExecVisitorImpl<VectorArray>(input);
            break;
        }
        case DataType::GEOMETRY: {
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
        case DataType::VECTOR_FLOAT:
        case DataType::VECTOR_BINARY:
        case DataType::VECTOR_FLOAT16:
        case DataType::VECTOR_BFLOAT16:
        case DataType::VECTOR_SPARSE_U32_F32:
        case DataType::VECTOR_INT8: {
            result = ExecVectorNull(input);
            break;
        }
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported data type: {}",
                      expr_->column_.data_type_);
    }
}

void
PhyNullExpr::DetermineExecPath() {
    if (IsVectorDataType(expr_->column_.data_type_)) {
        exec_path_ = ExprExecPath::RawData;
        return;
    }

    SegmentExpr::DetermineExecPath();
    if (PinnedIndexIsNested()) {
        exec_path_ = ExprExecPath::RawData;
        pinned_index_.clear();
        num_index_chunk_ = 0;
    }
}

void
PhyNullExpr::MoveCursor() {
    if (IsOrdinaryVectorDataType(expr_->column_.data_type_)) {
        if (!has_offset_input_ && !execute_all_at_once_) {
            current_data_global_pos_ +=
                std::min(batch_size_, active_count_ - current_data_global_pos_);
        }
        return;
    }

    SegmentExpr::MoveCursor();
}

bool
PhyNullExpr::CanExecuteAllAtOnce() const {
    if (IsOrdinaryVectorDataType(expr_->column_.data_type_)) {
        return false;
    }
    return SegmentExpr::CanExecuteAllAtOnce();
}

void
PhyNullExpr::PrefetchRawData() {
    if (IsOrdinaryVectorDataType(expr_->column_.data_type_)) {
        return;
    }
    SegmentExpr::PrefetchRawData();
}

VectorPtr
PhyNullExpr::ExecVectorNull(OffsetVector* input) {
    if (!expr_->column_.nullable_) {
        const auto batch_size =
            input != nullptr
                ? static_cast<int64_t>(input->size())
                : std::min(batch_size_,
                           active_count_ - current_data_global_pos_);
        if (input == nullptr) {
            current_data_global_pos_ += batch_size;
        }
        return BuildNullResult(TargetBitmap(batch_size, true));
    }

    if (input != nullptr) {
        TargetBitmap valid_res(input->size(), true);
        std::vector<int64_t> offsets(input->begin(), input->end());
        segment_->ApplyFieldValidDataByOffsets(op_ctx_,
                                               field_id_,
                                               offsets.data(),
                                               offsets.size(),
                                               TargetBitmapView(valid_res));
        return BuildNullResult(std::move(valid_res));
    }

    const auto batch_size =
        std::min(batch_size_, active_count_ - current_data_global_pos_);
    TargetBitmap valid_res(batch_size, true);
    std::vector<int64_t> offsets(batch_size);
    for (int64_t i = 0; i < batch_size; ++i) {
        offsets[i] = current_data_global_pos_ + i;
    }
    segment_->ApplyFieldValidDataByOffsets(op_ctx_,
                                           field_id_,
                                           offsets.data(),
                                           offsets.size(),
                                           TargetBitmapView(valid_res));
    current_data_global_pos_ += batch_size;
    return BuildNullResult(std::move(valid_res));
}

template <typename T>
VectorPtr
PhyNullExpr::ExecVisitorImpl(OffsetVector* input) {
    if (auto res = PreCheckNullable(input)) {
        return res;
    }
    auto valid_res =
        (input != nullptr)
            ? ProcessChunksForValidByOffsets<T>(UseIndexCursor(), *input)
            : ProcessChunksForValid<T>(UseIndexCursor());
    return BuildNullResult(std::move(valid_res));
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

    return BuildNullResult(TargetBitmap(batch_size, true));
}

ColumnVectorPtr
PhyNullExpr::BuildNullResult(TargetBitmap&& field_valid) const {
    auto size = field_valid.size();
    switch (expr_->op_) {
        case proto::plan::NullExpr_NullOp_IsNull: {
            field_valid.flip();
            break;
        }
        case proto::plan::NullExpr_NullOp_IsNotNull: {
            break;
        }
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported null expr type {}",
                      proto::plan::NullExpr_NullOp_Name(expr_->op_));
    }
    return std::make_shared<ColumnVector>(std::move(field_valid),
                                          TargetBitmap(size, true));
}

}  //namespace exec
}  // namespace milvus
