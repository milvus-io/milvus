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
#include "db/meta/FilesHolder.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

using MergeFilesGroups = std::vector<meta::SegmentsSchema>;

class MergeStrategy {
 public:
    virtual Status
    RegroupFiles(meta::FilesHolder& files_holder, MergeFilesGroups& files_groups) = 0;
};  // MergeStrategy

using MergeStrategyPtr = std::shared_ptr<MergeStrategy>;

}  // namespace engine
}  // namespace milvus
