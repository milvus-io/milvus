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

#pragma once

#include <ctime>
#include <string>
#include <utility>
#include <vector>

namespace milvus {
namespace segment {

using CopyRanges = std::vector<std::pair<int32_t, int32_t>>;

// calculate copy range according to deleted offsets
// for example:
// segment row count is 100, the deleted offsets is: {1,2,3, 6, 9,10}
// the copy ranges will be:
// {
//    {0, 1}
//    {4, 6}
//    {7, 9}
//    {11, 100}
// }
bool
CalcCopyRangesWithOffset(const std::vector<int32_t>& offsets, int64_t row_count, CopyRanges& copy_ranges,
                         int64_t& delete_count);

// copy data from source data according to copy ranges
// for example:
// each row_with is 8 bytes
// src_data has 100 rows, means 800 bytes
// the copy ranges is:
// {
//    {0, 10}
//    {50, 90}
// }
// then the target_data will have (10 - 0) * 8 + (90 - 50) * 8 = 400 bytes copied from src_data
bool
CopyDataWithRanges(const std::vector<uint8_t>& src_data, int64_t row_width, const CopyRanges& copy_ranges,
                   std::vector<uint8_t>& target_data);

}  // namespace segment
}  // namespace milvus
