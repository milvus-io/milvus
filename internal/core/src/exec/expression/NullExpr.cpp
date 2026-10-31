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
    span.SetAttribute("data_type", static_cast<int>(expr_->column_.data_type_));

    auto input = context.get_offset_input();
    SetHasOffsetInput(input != nullptr);
    if (expr_->column_.element_level_) {
        result = ExecElementLevelNull(input);
        return;
    }
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
    // Element validity is read from raw data; a field without null elements
    // is not read at all.
    if (expr_->column_.element_level_ ||
        IsVectorDataType(expr_->column_.data_type_)) {
        exec_path_ = ExprExecPath::RawData;
        return;
    }

    auto req = MakeIndexRequirement(RequiredReader::Null);
    req.value_type = expr_->column_.data_type_;
    SelectAndPinIndex(req);
    if (PinnedIndexIsNested()) {
        ClearSelectedIndex();
        exec_path_ = ExprExecPath::RawData;
    }
}

void
PhyNullExpr::MoveCursor() {
    if (!expr_->column_.element_level_ &&
        IsOrdinaryVectorDataType(expr_->column_.data_type_)) {
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
    if (expr_->column_.element_level_ ||
        IsOrdinaryVectorDataType(expr_->column_.data_type_)) {
        return false;
    }
    return SegmentExpr::CanExecuteAllAtOnce();
}

void
PhyNullExpr::PrefetchRawData() {
    if (expr_->column_.element_level_) {
        if (is_element_nullable_) {
            SegmentExpr::PrefetchRawData();
        }
        return;
    }
    if (IsOrdinaryVectorDataType(expr_->column_.data_type_)) {
        return;
    }
    SegmentExpr::PrefetchRawData();
}

// `$[x] IS NULL` / `IS NOT NULL` on one struct element: true or false from
// the element's own validity, never UNKNOWN. Elements of a NULL struct row
// do not exist, so row validity plays no part.
VectorPtr
PhyNullExpr::ExecElementLevelNull(OffsetVector* input) {
    auto next_batch_size = GetNextRealBatchSize(input, true);
    if (!next_batch_size.has_value()) {
        return nullptr;
    }
    const auto elem_count = *next_batch_size;
    if (auto res = AdvanceEmptyElementBatch(input, true, elem_count)) {
        return res;
    }
    return BuildNullResult(ProcessElementValidity(input, elem_count));
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
    segment_->ApplyFieldValidDataByRange(op_ctx_,
                                         field_id_,
                                         current_data_global_pos_,
                                         batch_size,
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
