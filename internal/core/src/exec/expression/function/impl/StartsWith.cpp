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

#include "exec/expression/function/impl/StringFunctions.h"

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
StartsWithVarchar(const RowVector& args, FilterFunctionReturn& result) {
    Assert(args.childrens().size() == 2);
    auto strs =
        std::dynamic_pointer_cast<ValueVector<std::string>>(args.child(0));
    if (strs == nullptr) {
        PanicInfo(ExprInvalid, "invalid vector type");
    }
    auto prefixes =
        std::dynamic_pointer_cast<ValueVector<std::string>>(args.child(1));
    if (prefixes == nullptr) {
        PanicInfo(ExprInvalid, "invalid vector type");
    }
    TargetBitmap bitmap(strs->size());
    for (size_t i = 0; i < strs->size(); ++i) {
        const std::string& str = strs->GetValueAt(i);
        const std::string& prefix = prefixes->GetValueAt(i);
        bitmap.set(i, str.find(prefix) == 0);
    }
    result = std::make_shared<ColumnVector>(std::move(bitmap));
}

}  // namespace function
}  // namespace expression
}  // namespace exec
}  // namespace milvus
