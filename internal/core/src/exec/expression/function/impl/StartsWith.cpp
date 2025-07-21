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
StartsWithVarchar(const RowVector& args, FilterFunctionReturn& result) {
    if (args.childrens().size() != 2) {
        ThrowInfo(ExprInvalid,
                  "invalid argument count, expect 2, actual {}",
                  args.childrens().size());
    }
    auto strs = std::dynamic_pointer_cast<SimpleVector>(args.child(0));
    Assert(strs != nullptr);
    CheckVarcharOrStringType(strs);
    auto prefixes = std::dynamic_pointer_cast<SimpleVector>(args.child(1));
    Assert(prefixes != nullptr);
    CheckVarcharOrStringType(prefixes);

    TargetBitmap bitmap(strs->size(), false);
    TargetBitmap valid_bitmap(strs->size(), true);
    for (size_t i = 0; i < strs->size(); ++i) {
        if (strs->ValidAt(i) && prefixes->ValidAt(i)) {
            auto* str_ptr = reinterpret_cast<std::string*>(
                strs->RawValueAt(i, sizeof(std::string)));
            auto* prefix_ptr = reinterpret_cast<std::string*>(
                prefixes->RawValueAt(i, sizeof(std::string)));
            bitmap.set(i, str_ptr->find(*prefix_ptr) == 0);
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
