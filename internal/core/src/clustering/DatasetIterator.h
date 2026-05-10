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

// Copyright (c) KIOXIA Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "clustering/types.h"

namespace milvus::clustering {

enum class DatasetPurpose { TRAIN, ASSIGNMENT };

struct DatasetPart {
    std::vector<std::string> files;
    int64_t max_bytes = 0;
    int64_t offset_rows = 0;
    Config storage_config;
};

template <typename T>
class DatasetIterator {
 public:
    DatasetIterator(
        DatasetPurpose purpose,
        const std::vector<int64_t>& segment_ids,
        const std::map<int64_t, std::vector<std::string>>& segment_files,
        const std::map<int64_t, int64_t>& num_rows,
        const std::map<std::string, int64_t>& file_sizes_map,
        int64_t dim,
        int64_t max_batch_bytes,
        int64_t total_sample_bytes,
        int storage_version,
        bool random_sample);

    bool
    HasNext() const;
    DatasetPart
    Next();
    void
    notifyEOF();
    void
    increaseRemaining(size_t increase) {
        remaining_sample_bytes_ += increase;
    }

 private:
    DatasetPurpose purpose_;
    std::vector<int64_t> segment_ids_;
    std::map<int64_t, std::vector<std::string>> segment_files_;
    std::map<int64_t, int64_t> num_rows_;
    std::map<std::string, int64_t> file_sizes_map_;

    int64_t dim_;
    int64_t max_batch_bytes_;
    int64_t remaining_sample_bytes_;
    int storage_version_;
    bool random_sample_;

    size_t cur_segment_idx_ = 0;
    size_t cur_file_idx_ = 0;
    int64_t cur_file_offset_rows_ = 0;
    bool file_has_progressed_ = false;
};

}  // namespace milvus::clustering
