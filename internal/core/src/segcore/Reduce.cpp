// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <cstdint>
#include <vector>
#include <algorithm>

#include "Reduce.h"

namespace milvus::segcore {
Status
merge_into(int64_t queries,
           int64_t topk,
           float* distances,
           int64_t* uids,
           const float* new_distances,
           const int64_t* new_uids) {
    for (int64_t qn = 0; qn < queries; ++qn) {
        auto base = qn * topk;
        auto src2_dis = distances + base;
        auto src2_uids = uids + base;

        auto src1_dis = new_distances + base;
        auto src1_uids = new_uids + base;

        std::vector<float> buf_dis(topk);
        std::vector<int64_t> buf_uids(topk);

        auto it1 = 0;
        auto it2 = 0;

        for (auto buf = 0; buf < topk; ++buf) {
            if (src1_dis[it1] <= src2_dis[it2]) {
                buf_dis[buf] = src1_dis[it1];
                buf_uids[buf] = src1_uids[it1];
                ++it1;
            } else {
                buf_dis[buf] = src2_dis[it2];
                buf_uids[buf] = src2_uids[it2];
                ++it2;
            }
        }
        std::copy_n(buf_dis.data(), topk, src2_dis);
        std::copy_n(buf_uids.data(), topk, src2_uids);
    }
    return Status::OK();
}
}  // namespace milvus::segcore
