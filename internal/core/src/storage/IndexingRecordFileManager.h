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

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>
#include <memory>
#include "log/Log.h"

#include "storage/LocalChunkManagerSingleton.h"

namespace milvus::storage {
/**
 * @brief This IndexingRecordFileManager handle all temperate growing index files in indexingrecord of one segment;
 * If growing index uses compress solution, these temperate growing index files will hold all raw data for getVectors in local.
 * the index files control by growing index itself.
 */
class IndexingRecordFileManager {
 public:
    IndexingRecordFileManager(const int64_t segment_id) {
        cm_ = storage::LocalChunkManagerSingleton::GetInstance()
                  .GetChunkManager();
        AssertInfo(
            cm_ != nullptr,
            "Fail to get LocalChunkManager, LocalChunkManagerSPtr is null");
        indexing_record_data_prefix_ = GenSegmentPathPrefix(segment_id);
        if (cm_->Exist(indexing_record_data_prefix_)) {
            cm_->RemoveDir(indexing_record_data_prefix_);
        }
        cm_->CreateDir(indexing_record_data_prefix_);
    }
    ~IndexingRecordFileManager() {
        cm_->RemoveDir(indexing_record_data_prefix_);
    }
    std::string
    GetAndCreateFieldDir(const uint64_t field_id) {
        auto file_prefix = GenFieldPathPrefix(field_id);
        if (cm_->Exist(file_prefix)) {
            cm_->RemoveDir(file_prefix);
        }
        cm_->CreateDir(file_prefix);
        return file_prefix;
    }

 private:
    LocalChunkManagerSPtr cm_;
    std::string indexing_record_data_prefix_;
    std::string
    GenSegmentPathPrefix(const int64_t segment_id) {
        return cm_->GetRootPath() + "/" + std::string(GROWING_INDEX_DATA_PATH) +
               "/" + std::to_string(segment_id) + "/";
    }
    std::string
    GenFieldPathPrefix(const int64_t field_id) {
        return indexing_record_data_prefix_ + std::to_string(field_id) + "/";
    }
};
using IndexingRecordFileManagerPtr =
    std::shared_ptr<milvus::storage::IndexingRecordFileManager>;
}  // namespace milvus::storage