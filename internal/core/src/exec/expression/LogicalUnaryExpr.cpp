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

#include "LogicalUnaryExpr.h"

namespace milvus {
namespace exec {

void
PhyLogicalUnaryExpr::Eval(EvalCtx& context, VectorPtr& result) {
    AssertInfo(inputs_.size() == 1,
               "logical unary expr must has one input, but now {}",
               inputs_.size());

    inputs_[0]->Eval(context, result);
    if (expr_->op_type_ == milvus::expr::LogicalUnaryExpr::OpType::LogicalNot) {
        auto flat_vec = GetColumnVector(result);
        TargetBitmapView data(flat_vec->GetRawData(), flat_vec->size());
        data.flip();
        if (context.get_apply_valid_data_after_flip()) {
            TargetBitmapView valid_data(flat_vec->GetValidRawData(),
                                        flat_vec->size());
            data &= valid_data;
        }
    }
}

}  //namespace exec
}  // namespace milvus
