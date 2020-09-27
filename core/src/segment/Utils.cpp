// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "segment/Utils.h"

#include <algorithm>
#include <set>
#include <unordered_set>
#include <utility>
#include <vector>

#include "utils/Log.h"

namespace milvus {
namespace segment {

bool
CalcCopyRangesWithOffset(const std::vector<int32_t>& offsets, int64_t row_count, CopyRanges& copy_ranges,
                         int64_t& delete_count) {
    copy_ranges.clear();
    if (offsets.empty() || row_count <= 0) {
        return false;
    }

    // arrange offsets
    std::set<int32_t> new_offsets;
    for (auto offset : offsets) {
        if (offset < 0 || offset >= row_count) {
            continue;
        }
        new_offsets.insert(offset);
    }
    delete_count = new_offsets.size();
    if (delete_count == 0) {
        return true;
    }

    // if the first offset is not zero, add a range [0, first]
    int32_t first = *new_offsets.begin();
    if (first > 0) {
        copy_ranges.push_back(std::make_pair(0, first));
    }

    // calculate inner range
    int32_t prev = *new_offsets.begin();
    for (auto offset : new_offsets) {
        if (offset - prev == 1) {
            prev = offset;
            continue;
        } else {
            if (prev != offset) {
                copy_ranges.push_back(std::make_pair(prev + 1, offset));
            }
        }
        prev = offset;
    }

    // if the last offset is not the last row, add a range [last + 1, row_count]
    int32_t last = *new_offsets.rbegin();
    if (last < row_count - 1) {
        copy_ranges.push_back(std::make_pair(last + 1, row_count));
    }

    return true;
}

bool
CopyDataWithRanges(const std::vector<uint8_t>& src_data, int64_t row_width, const CopyRanges& copy_ranges,
                   std::vector<uint8_t>& target_data) {
    target_data.clear();
    if (src_data.empty() || copy_ranges.empty() || row_width <= 0) {
        return false;
    }

    // calculate result bytes
    int64_t bytes = 0;
    for (auto& pair : copy_ranges) {
        if (pair.second <= pair.first) {
            continue;
        }
        bytes += (pair.second - pair.first) * row_width;
    }
    target_data.resize(bytes);

    // copy data to result
    size_t poz = 0;
    for (auto& pair : copy_ranges) {
        size_t len = (pair.second - pair.first) * row_width;
        memcpy(target_data.data() + poz, src_data.data() + pair.first * row_width, len);
        poz += len;
    }

    return true;
}

void
GetIDWithoutDeleted(const engine::IDNumbers& entity_ids, const std::vector<int32_t>& offsets,
                    engine::IDNumbers& result_ids) {
    result_ids.clear();
    if (offsets.empty()) {
        result_ids = entity_ids;
        return;  // do nothing
    }

    // typically, the id array can be split into three parts
    // assume there is a id array: [1, 2, 3, 4, 5, ... , 99, 100]
    // and the offsets is: [20, 30, 40, 50], means 4 ids will be erased
    // the id array can be split into three parts: [1 ~ 19], [20 ~ 50], [51 ~ 100]
    // we can quickly copy the first part and third part
    // for the second part, do a sort-merge to pick out ids that not been deleted
    // this approach performance is better than std::vector::erase() method, especially for large number id array
    std::vector<int32_t> delete_offsets = offsets;
    std::sort(delete_offsets.begin(), delete_offsets.end());

    int64_t left_count = entity_ids.size() - delete_offsets.size();
    if (left_count <= 0) {
        return;  // all id deleted
    }
    result_ids.resize(left_count);

    int32_t min_delete_offset = *delete_offsets.begin();
    int32_t max_delete_offset = *delete_offsets.rbegin();

    // copy the first part
    if (min_delete_offset > 0) {
        memcpy(result_ids.data(), entity_ids.data(), min_delete_offset * sizeof(int64_t));
    }

    // copy the third part
    if (max_delete_offset < entity_ids.size() - 1) {
        int64_t copy_count = entity_ids.size() - 1 - max_delete_offset;
        memcpy(result_ids.data() + left_count - copy_count, entity_ids.data() + max_delete_offset + 1,
               copy_count * sizeof(int64_t));
    }

    // sort-merge the second part
    for (int64_t k = min_delete_offset, n = 0, index = min_delete_offset; n < delete_offsets.size();) {
        if (k == delete_offsets[n]) {
            ++k;
            ++n;
        } else {
            if (index >= left_count) {
                LOG_ENGINE_ERROR_ << "offsets array contains duplicate items";
                break;  // duplicate offsets, avoid out of bounds
            }
            result_ids[index] = entity_ids[k];
            ++index;
            ++k;
        }
    }
}

}  // namespace segment
}  // namespace milvus
