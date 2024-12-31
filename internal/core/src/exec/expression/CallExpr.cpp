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

#include "common/FieldDataInterface.h"
#include "common/Vector.h"
#include "exec/expression/CallExpr.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/function/FunctionFactory.h"

#include <utility>
#include <vector>

namespace milvus {
namespace exec {

void
PhyCallExpr::Eval(EvalCtx& context, VectorPtr& result) {
    auto offset_input = context.get_offset_input();
    SetHasOffsetInput(offset_input != nullptr);
    AssertInfo(inputs_.size() == expr_->inputs().size(),
               "logical call expr needs {} inputs, but {} inputs are provided",
               expr_->inputs().size(),
               inputs_.size());
    std::vector<VectorPtr> args;
    for (auto& input : this->inputs_) {
        VectorPtr arg_result;
        input->Eval(context, arg_result);
        args.push_back(std::move(arg_result));
    }
    RowVector row_vector(std::move(args));
    this->expr_->function_ptr()(row_vector, result);
}

}  // namespace exec
}  // namespace milvus
