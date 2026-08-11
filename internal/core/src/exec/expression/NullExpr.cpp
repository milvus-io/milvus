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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

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

namespace {

bool
IsArrayElementNullExpr(const milvus::expr::ColumnInfo& column) {
    return column.data_type_ == DataType::ARRAY &&
           !column.nested_path_.empty();
}

template <bool ElementNullable>
struct ArrayElementNullExecutor {
    int index_;
    bool is_null_;

    template <FilterType filter_type = FilterType::sequential,
              typename ArrayType>
    void
    operator()(const ArrayType* data,
               const bool* valid_data,
               const int32_t* offsets,
               const int size,
               TargetBitmapView res,
               TargetBitmapView valid_res) const {
        if (data == nullptr) {
            return;
        }
        for (int i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = offsets ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            if (index_ < 0 || index_ >= data[offset].length()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            if constexpr (ElementNullable) {
                const bool element_valid =
                    data[offset].is_element_valid(index_);
                res[i] = is_null_ ? !element_valid : element_valid;
            } else {
                res[i] = !is_null_;
            }
        }
    }

    void
    operator()(std::nullptr_t,
               std::nullptr_t,
               std::nullptr_t,
               int64_t,
               TargetBitmapView,
               TargetBitmapView) const {
    }
};

}  // namespace

void
PhyNullExpr::Eval(EvalCtx& context, VectorPtr& result) {
    WaitPrefetch();
    tracer::AutoSpan span("PhyNullExpr::Eval", tracer::GetRootSpan(), true);
    span.GetSpan()->SetAttribute("data_type",
                                 static_cast<int>(expr_->column_.data_type_));

    auto input = context.get_offset_input();
    if (IsArrayElementNullExpr(expr_->column_)) {
        result = ExecArrayElementNull(input);
        return;
    }
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
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported data type: {}",
                      expr_->column_.data_type_);
    }
}

void
PhyNullExpr::DetermineExecPath() {
    if (IsArrayElementNullExpr(expr_->column_)) {
        exec_path_ = ExprExecPath::RawData;
        return;
    }
    if (expr_->column_.data_type_ == DataType::VECTOR_ARRAY) {
        exec_path_ = ExprExecPath::RawData;
        return;
    }

    SegmentExpr::DetermineExecPath();
    if (expr_->column_.element_level_ && !PinnedIndexIsNested()) {
        exec_path_ = ExprExecPath::RawData;
    }
}

VectorPtr
PhyNullExpr::ExecArrayElementNull(OffsetVector* input) {
    auto real_batch_size = GetNextRealBatchSize(input, false);
    if (real_batch_size == 0) {
        return nullptr;
    }

    const auto index = std::stoi(expr_->column_.nested_path_[0]);
    const bool is_null =
        expr_->op_ == proto::plan::NullExpr_NullOp_IsNull;
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    auto run = [&]<bool ElementNullable>() {
        auto execute_sub_batch =
            ArrayElementNullExecutor<ElementNullable>{index, is_null};
        if (input != nullptr) {
            return ProcessDataByOffsets<ArrayView>(execute_sub_batch,
                                                   std::nullptr_t{},
                                                   input,
                                                   res,
                                                   valid_res);
        }
        return ProcessDataChunks<ArrayView>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res);
    };

    const auto processed_size = element_nullable_
                                    ? run.template operator()<true>()
                                    : run.template operator()<false>();
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal expect "
               "batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename T>
VectorPtr
PhyNullExpr::ExecVisitorImpl(OffsetVector* input) {
    if (auto res = PreCheckNullable(input)) {
        return res;
    }
    auto valid_res =
        (input != nullptr)
            ? ProcessChunksForValidByOffsets<T>(
                  UseIndexCursor(), *input, expr_->column_.element_level_)
            : ProcessChunksForValid<T>(UseIndexCursor(),
                                       expr_->column_.element_level_);
    TargetBitmap res = valid_res.clone();
    if (expr_->op_ == proto::plan::NullExpr_NullOp_IsNull) {
        res.flip();
    }
    auto res_vec = std::make_shared<ColumnVector>(
        std::move(res), TargetBitmap(valid_res.size(), true));
    return res_vec;
}

// if nullable is false, no need to process chunks
// res is all false when is null, and is all true when is not null
ColumnVectorPtr
PhyNullExpr::PreCheckNullable(OffsetVector* input) {
    const bool nullable = expr_->column_.element_level_
                              ? element_nullable_
                              : expr_->column_.nullable_;
    if (nullable) {
        return nullptr;
    }

    int64_t batch_size;
    if (input != nullptr) {
        batch_size = input->size();
    } else if (expr_->column_.element_level_) {
        auto [_, elem_count] = GetNextBatchSizeForElementLevel();
        batch_size = elem_count;
        MoveCursor();
    } else {
        batch_size = precheck_pos_ + batch_size_ >= active_count_
                         ? active_count_ - precheck_pos_
                         : batch_size_;
        precheck_pos_ += batch_size;
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
            ThrowInfo(UnexpectedError,
                      "unsupported null expr type {}",
                      proto::plan::NullExpr_NullOp_Name(expr_->op_));
    }
    return res_vec;
}

}  //namespace exec
}  // namespace milvus
