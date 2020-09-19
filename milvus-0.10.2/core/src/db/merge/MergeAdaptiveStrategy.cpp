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

#include "db/merge/MergeAdaptiveStrategy.h"
#include "utils/Log.h"

#include <algorithm>
#include <vector>

namespace milvus {
namespace engine {

Status
MergeAdaptiveStrategy::RegroupFiles(meta::FilesHolder& files_holder, MergeFilesGroups& files_groups) {
    meta::SegmentsSchema sort_files, ignore_files;
    meta::SegmentsSchema& files = files_holder.HoldFiles();
    for (meta::SegmentsSchema::reverse_iterator iter = files.rbegin(); iter != files.rend(); ++iter) {
        meta::SegmentSchema& file = *iter;
        if (file.index_file_size_ > 0 && (int64_t)file.file_size_ > file.index_file_size_) {
            // file that no need to merge
            ignore_files.push_back(file);
            continue;
        }
        sort_files.push_back(file);
    }

    files_holder.UnmarkFiles(ignore_files);

    // no need to merge single file
    if (sort_files.size() < 2) {
        return Status::OK();
    }

    // two files, simply merge them
    if (sort_files.size() == 2) {
        files_groups.emplace_back(sort_files);
        return Status::OK();
    }

    // arrange files by file size in descending order
    std::sort(sort_files.begin(), sort_files.end(),
              [](const meta::SegmentSchema& left, const meta::SegmentSchema& right) {
                  return left.file_size_ > right.file_size_;
              });

    // pick files to merge
    int64_t index_file_size = sort_files[0].index_file_size_;
    while (true) {
        meta::SegmentsSchema temp_group;
        int64_t sum_size = 0;
        for (auto iter = sort_files.begin(); iter != sort_files.end();) {
            meta::SegmentSchema& file = *iter;
            if (sum_size + (int64_t)(file.file_size_) <= index_file_size) {
                temp_group.push_back(file);
                sum_size += file.file_size_;
                iter = sort_files.erase(iter);
            } else {
                if ((iter + 1 == sort_files.end()) && sum_size < index_file_size) {
                    temp_group.push_back(file);
                    sort_files.erase(iter);
                    break;
                } else {
                    ++iter;
                }
            }
        }

        if (!temp_group.empty()) {
            files_groups.emplace_back(temp_group);
        }

        if (sort_files.empty()) {
            break;
        }
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
