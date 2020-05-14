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

#include "db/merge/MergeLayeredStrategy.h"
#include "db/Utils.h"
#include "db/meta/MetaConsts.h"
#include "utils/Log.h"

#include <map>
#include <vector>

namespace milvus {
namespace engine {

const int64_t FORCE_MERGE_THREASHOLD = 30;  // force merge files older this time(in second)

Status
MergeLayeredStrategy::RegroupFiles(meta::FilesHolder& files_holder, MergeFilesGroups& files_groups) {
    using LayerGroups = std::map<uint64_t, meta::SegmentsSchema>;
    // distribute files to groups according to file size(in byte)
    LayerGroups layers = {
        {1UL << 22, meta::SegmentsSchema()},  // 4MB
        {1UL << 24, meta::SegmentsSchema()},  // 16MB
        {1UL << 26, meta::SegmentsSchema()},  // 64MB
        {1UL << 28, meta::SegmentsSchema()},  // 256MB
        {1UL << 30, meta::SegmentsSchema()},  // 1GB
    };

    meta::SegmentsSchema& files = files_holder.HoldFiles();
    meta::SegmentsSchema huge_files;
    // iterater from end, because typically the files_holder get files in order from largest to smallest
    for (meta::SegmentsSchema::reverse_iterator iter = files.rbegin(); iter != files.rend(); ++iter) {
        meta::SegmentSchema& file = *iter;
        if (file.index_file_size_ > 0 && file.file_size_ > file.index_file_size_) {
            // release file that no need to merge
            files_holder.UnmarkFile(file);
            continue;
        }

        bool match = false;
        for (auto& pair : layers) {
            if ((*iter).file_size_ < pair.first) {
                pair.second.push_back(file);
                match = true;
                break;
            }
        }

        if (!match) {
            huge_files.push_back(file);
        }
    }

    auto now = utils::GetMicroSecTimeStamp();
    meta::SegmentsSchema force_merge_file;
    for (auto& pair : layers) {
        // skip empty layer
        if (pair.second.empty()) {
            continue;
        }

        // layer has multiple files, merge along with the force_merge_file
        if (!force_merge_file.empty()) {
            for (auto& file : force_merge_file) {
                pair.second.push_back(file);
            }
            force_merge_file.clear();
        }

        // layer only has one file, if the file is too old, force merge it, else no need to merge it
        if (pair.second.size() == 1) {
            if (now - pair.second[0].created_on_ > FORCE_MERGE_THREASHOLD * meta::US_PS) {
                force_merge_file.push_back(pair.second[0]);
                pair.second.clear();
            }
        }
    }

    // if force_merge_file is not allocated by any layer, combine it to huge_files
    if (!force_merge_file.empty() && !huge_files.empty()) {
        for (auto& file : force_merge_file) {
            huge_files.push_back(file);
        }
        force_merge_file.clear();
    }

    // return result
    for (auto& pair : layers) {
        if (pair.second.size() == 1) {
            // release file that no need to merge
            files_holder.UnmarkFile(pair.second[0]);
        } else if (pair.second.size() > 1) {
            // create group
            meta::SegmentsSchema temp_files;
            temp_files.swap(pair.second);
            files_groups.emplace_back(temp_files);
        }
    }

    if (huge_files.size() >= 1) {
        meta::SegmentsSchema temp_files;
        temp_files.swap(huge_files);
        for (auto& file : force_merge_file) {
            temp_files.push_back(file);
        }

        if (temp_files.size() >= 2) {
            // create group
            files_groups.emplace_back(temp_files);
        } else {
            for (auto& file : huge_files) {
                // release file that no need to merge
                files_holder.UnmarkFile(file);
            }
            for (auto& file : force_merge_file) {
                // release file that no need to merge
                files_holder.UnmarkFile(file);
            }
        }
    } else {
        for (auto& file : force_merge_file) {
            // release file that no need to merge
            files_holder.UnmarkFile(file);
        }
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
