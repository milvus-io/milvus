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
#include "db/engine/ExecutionEngine.h"
#include "db/meta/Meta.h"
#include "segment/SegmentWriter.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

// TODO(zhiru): this class needs to be refactored once attributes are added

class VectorSource {
 public:
    explicit VectorSource(VectorsData vectors);

    VectorSource(VectorsData vectors, const std::unordered_map<std::string, uint64_t>& attr_nbytes,
                 const std::unordered_map<std::string, uint64_t>& attr_size,
                 const std::unordered_map<std::string, std::vector<uint8_t>>& attr_data);

    Status
    Add(const segment::SegmentWriterPtr& segment_writer_ptr, const meta::SegmentSchema& table_file_schema,
        const size_t& num_vectors_to_add, size_t& num_vectors_added);

    Status
    AddEntities(const segment::SegmentWriterPtr& segment_writer_ptr, const meta::SegmentSchema& collection_file_schema,
                const size_t& num_attrs_to_add, size_t& num_attrs_added);

    size_t
    GetNumVectorsAdded();

    size_t
    SingleVectorSize(uint16_t dimension);

    size_t
    SingleEntitySize(uint16_t dimension);

    bool
    AllAdded();

    IDNumbers
    GetVectorIds();

 private:
    VectorsData vectors_;
    IDNumbers vector_ids_;
    const std::unordered_map<std::string, uint64_t> attr_nbytes_;
    std::unordered_map<std::string, uint64_t> attr_size_;
    std::unordered_map<std::string, std::vector<uint8_t>> attr_data_;

    size_t current_num_vectors_added;
    size_t current_num_attrs_added;
};  // VectorSource

using VectorSourcePtr = std::shared_ptr<VectorSource>;

}  // namespace engine
}  // namespace milvus
