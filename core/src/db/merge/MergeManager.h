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

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/Types.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

// 1. SIMPLE
//    merge in old way, merge files one by one, stop merge until file size exceed index_file_size
// 2. LAYERED
//    distribute files to several groups according to file size
//    firstly, define layers by file size: 4MB, 16MB, 64MB, 256MB, 1024MB
//    if file size between 0MB~4MB, put it into layer "4"
//    if file size between 4MB~16MB, put it into layer "16"
//    if file size between 16MB~64MB, put it into layer "64"
//    if file size between 64MB~256MB, put it into layer "256"
//    if file size between 256MB~1024MB, put it into layer "1024"
//    secondly, merge files for each group
//    third, if some file's create time is 30 seconds ago, and it still un-merged, force merge with upper layer files
// 3. ADAPTIVE
//    Pick files that sum of size is close to index_file_size, merge them
enum class MergeStrategyType {
    SIMPLE = 1,
    LAYERED = 2,
    ADAPTIVE = 3,
};

class MergeManager {
 public:
    virtual Status
    MergeFiles(int64_t collection_id, MergeStrategyType type = MergeStrategyType::SIMPLE) = 0;
};  // MergeManager

using MergeManagerPtr = std::shared_ptr<MergeManager>;

}  // namespace engine
}  // namespace milvus
