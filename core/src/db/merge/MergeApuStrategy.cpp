//  Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
//  with the License. You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software distributed under the License
//  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
//  or implied. See the License for the specific language governing permissions and limitations under the License.
//

#include "db/merge/MergeApuStrategy.h"

#include <algorithm>

namespace milvus {
namespace engine {

Status
MergeApuStrategy::RegroupFiles(meta::FilesHolder& files_holder, MergeFilesGroups& files_groups) {
    meta::SegmentsSchema& files = files_holder.HoldFiles();

    // arrange files by ascending creation date order
    std::sort(files.begin(), files.end(), [](const meta::SegmentSchema& left, const meta::SegmentSchema& right) {
        return left.created_on_ < right.created_on_;
    });

    if (!files.empty()) {
        files_groups.emplace_back(files);
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
