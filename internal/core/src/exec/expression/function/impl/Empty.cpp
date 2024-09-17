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

#include "exec/expression/function/impl/Empty.h"

#include <boost/variant/get.hpp>
#include <string>
#include "common/EasyAssert.h"
#include "exec/expression/Expr.h"
#include "exec/expression/function/FunctionFactory.h"

namespace milvus {
namespace exec {
namespace expression {
namespace function {

void
EmptyVarchar(EvalCtx& context,
             const std::vector<FilterFunctionParameter>& args,
             FilterFunctionReturn& result) {
    Assert(args.size() == 1);
    VectorPtr arg_result;
    args[0]->Eval(context, arg_result);

    if (auto arg = std::dynamic_pointer_cast<ConstantVector>(arg_result)) {
        bool empty = arg->GetGenericValue().string_val().empty();
        result =
            std::make_shared<ColumnVector>(TargetBitmap(arg->size(), empty));
    } else if (auto arg =
                   std::dynamic_pointer_cast<LazySegmentVector>(arg_result)) {
        TargetBitmap result_vec(arg->size());
        // TODO: simd support
        for (size_t i = 0; i < arg->size(); ++i) {
            auto str = arg->GetValue<std::string>(i);
            result_vec.set(i, str.empty());
        }
        result = std::make_shared<ColumnVector>(std::move(result_vec));
    } else {
        PanicInfo(ExprInvalid, "unsupported vector type");
    }
}

}  // namespace function
}  // namespace expression
}  // namespace exec
}  // namespace milvus
