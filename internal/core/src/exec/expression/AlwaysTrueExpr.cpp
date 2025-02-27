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

namespace milvus {
namespace exec {

void
PhyAlwaysTrueExpr::Eval(EvalCtx& context, VectorPtr& result) {
    auto input = context.get_offset_input();
    has_offset_input_ = (input != nullptr);
    int64_t real_batch_size = (has_offset_input_)
                                  ? input->size()
                                  : (current_pos_ + batch_size_ >= active_count_
                                         ? active_count_ - current_pos_
                                         : batch_size_);

    // always true no need to skip null
    if (real_batch_size == 0) {
        result = nullptr;
        return;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    res.set();
    valid_res.set();

    result = res_vec;
    current_pos_ += real_batch_size;
}

}  //namespace exec
}  // namespace milvus
