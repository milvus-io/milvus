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

#include "ValueExpr.h"
#include "common/Vector.h"

namespace milvus {
namespace exec {

void
PhyValueExpr::Eval(EvalCtx& context, VectorPtr& result) {
    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    int64_t real_batch_size = has_offset_input_
                                  ? input->size()
                                  : (current_pos_ + batch_size_ >= active_count_
                                         ? active_count_ - current_pos_
                                         : batch_size_);

    if (real_batch_size == 0) {
        result = nullptr;
        return;
    }

    switch (expr_->type()) {
        case DataType::NONE:
            // null
            result = std::make_shared<ConstantVector<bool>>(
                expr_->type(), real_batch_size, false, 1);
            break;
        case DataType::BOOL:
            result = std::make_shared<ConstantVector<bool>>(
                expr_->type(),
                real_batch_size,
                expr_->GetGenericValue().bool_val());
            break;
        case DataType::INT8:
            result = std::make_shared<ConstantVector<int8_t>>(
                expr_->type(),
                real_batch_size,
                expr_->GetGenericValue().int64_val());
            break;
        case DataType::INT16:
            result = std::make_shared<ConstantVector<int16_t>>(
                expr_->type(),
                real_batch_size,
                expr_->GetGenericValue().int64_val());
            break;
        case DataType::INT32:
            result = std::make_shared<ConstantVector<int32_t>>(
                expr_->type(),
                real_batch_size,
                expr_->GetGenericValue().int64_val());
            break;
        case DataType::INT64:
            result = std::make_shared<ConstantVector<int64_t>>(
                expr_->type(),
                real_batch_size,
                expr_->GetGenericValue().int64_val());
            break;
        case DataType::FLOAT:
            result = std::make_shared<ConstantVector<float>>(
                expr_->type(),
                real_batch_size,
                expr_->GetGenericValue().float_val());
            break;
        case DataType::DOUBLE:
            result = std::make_shared<ConstantVector<double>>(
                expr_->type(),
                real_batch_size,
                expr_->GetGenericValue().float_val());
            break;
        case DataType::STRING:
        case DataType::VARCHAR:
            result = std::make_shared<ConstantVector<std::string>>(
                expr_->type(),
                real_batch_size,
                expr_->GetGenericValue().string_val());
            break;
        // TODO: json and array type
        case DataType::ARRAY:
        case DataType::JSON:
        default:
            PanicInfo(DataTypeInvalid,
                      "PhyValueExpr not support data type " +
                          GetDataTypeName(expr_->type()));
    }
    current_pos_ += real_batch_size;
}

}  //namespace exec
}  // namespace milvus
