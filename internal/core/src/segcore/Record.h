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

#pragma once

namespace milvus::segcore {

template <typename RecordType>
inline int64_t
get_barrier(const RecordType& record, Timestamp timestamp) {
    auto& vec = record.timestamps();
    int64_t beg = 0;
    int64_t end = record.size();
    while (beg < end) {
        auto mid = (beg + end) / 2;
        if (vec[mid] <= timestamp) {
            beg = mid + 1;
        } else {
            end = mid;
        }
    }
    return beg;
}

}  // namespace milvus::segcore
