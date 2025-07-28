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

#include "exec/expression/function/FunctionImplUtils.h"
#include "exec/expression/function/impl/StringFunctions.h"

#include <boost/variant/get.hpp>
#include <string>
#include "common/EasyAssert.h"
#include "exec/expression/function/FunctionFactory.h"

namespace milvus {
namespace exec {
namespace expression {
namespace function {

void
EmptyVarchar(const RowVector& args, FilterFunctionReturn& result) {
    if (args.childrens().size() != 1) {
        ThrowInfo(ExprInvalid,
                  "invalid argument count, expect 1, actual {}",
                  args.childrens().size());
    }
    auto arg = args.child(0);
    auto vec = std::dynamic_pointer_cast<SimpleVector>(arg);
    Assert(vec != nullptr);
    CheckVarcharOrStringType(vec);
    TargetBitmap bitmap(vec->size(), false);
    TargetBitmap valid_bitmap(vec->size(), true);
    for (size_t i = 0; i < vec->size(); ++i) {
        if (vec->ValidAt(i)) {
            bitmap[i] = reinterpret_cast<std::string*>(
                            vec->RawValueAt(i, sizeof(std::string)))
                            ->empty();
        } else {
            valid_bitmap[i] = false;
        }
    }
    result = std::make_shared<ColumnVector>(std::move(bitmap),
                                            std::move(valid_bitmap));
}

}  // namespace function
}  // namespace expression
}  // namespace exec
}  // namespace milvus
