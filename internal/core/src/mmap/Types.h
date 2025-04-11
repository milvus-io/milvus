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

#include <unistd.h>
#include <memory>
#include <string>
#include <vector>
#include "arrow/record_batch.h"
#include "common/FieldData.h"
#include "storage/DataCodec.h"

namespace milvus {

struct FieldDataInfo {
    FieldDataInfo() {
        arrow_reader_channel = std::make_shared<ArrowReaderChannel>();
    }

    FieldDataInfo(int64_t field_id,
                  size_t row_count,
                  std::string mmap_dir_path = "")
        : field_id(field_id),
          row_count(row_count),
          mmap_dir_path(std::move(mmap_dir_path)) {
        arrow_reader_channel = std::make_shared<ArrowReaderChannel>();
    }

    FieldDataInfo(
        int64_t field_id,
        size_t row_count,
        const std::vector<std::shared_ptr<milvus::ArrowDataWrapper>>& batch)
        : field_id(field_id), row_count(row_count) {
        arrow_reader_channel = std::make_shared<ArrowReaderChannel>();
        for (auto& data : batch) {
            arrow_reader_channel->push(data);
        }
        arrow_reader_channel->close();
    }

    int64_t field_id;
    size_t row_count;
    std::string mmap_dir_path;
    std::shared_ptr<ArrowReaderChannel> arrow_reader_channel;
};
}  // namespace milvus
