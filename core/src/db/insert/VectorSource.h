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
#include <string>
#include <unordered_map>
#include <vector>

#include "db/IDGenerator.h"
#include "db/insert/MemManager.h"
#include "segment/Segment.h"
#include "segment/SegmentWriter.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

class VectorSource {
 public:
    explicit VectorSource(const DataChunkPtr& chunk, idx_t op_id);

    Status
    Add(const segment::SegmentWriterPtr& segment_writer_ptr, const int64_t& num_attrs_to_add, int64_t& num_attrs_added);

    bool
    AllAdded();

    idx_t
    OperationID() const {
        return op_id_;
    }

 private:
    DataChunkPtr chunk_;
    idx_t op_id_ = 0;
    int64_t current_num_added_ = 0;
};

using VectorSourcePtr = std::shared_ptr<VectorSource>;

}  // namespace engine
}  // namespace milvus
