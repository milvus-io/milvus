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

#include "AlwaysTrueExpr.h"

#include <algorithm>

#include "exec/expression/EvalCtx.h"

namespace milvus {
namespace exec {

void
PhyAlwaysTrueExpr::Eval(EvalCtx& context, VectorPtr& result) {
    auto input = context.get_offset_input();
    has_offset_input_ = (input != nullptr);
    const int64_t batch_size =
        has_offset_input_ ? static_cast<int64_t>(input->size())
                          : std::min(batch_size_, active_count_ - current_pos_);
    if (batch_size == 0) {
        result = nullptr;
        return;
    }

    const auto count = context.get_input_batch_size().value_or(batch_size);
    result = std::make_shared<ColumnVector>(TargetBitmap(count, true),
                                            TargetBitmap(count, true));
    if (!has_offset_input_) {
        current_pos_ += batch_size;
    }
}

}  //namespace exec
}  // namespace milvus
