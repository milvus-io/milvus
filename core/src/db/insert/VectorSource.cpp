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

#include "db/insert/VectorSource.h"

#include <utility>
#include <vector>

#include "db/engine/EngineFactory.h"
#include "db/engine/ExecutionEngine.h"
#include "metrics/Metrics.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace engine {

VectorSource::VectorSource(VectorsData vectors) : vectors_(std::move(vectors)) {
    current_num_vectors_added = 0;
}

VectorSource::VectorSource(milvus::engine::VectorsData vectors,
                           const std::unordered_map<std::string, uint64_t>& attr_nbytes,
                           const std::unordered_map<std::string, uint64_t>& attr_size,
                           const std::unordered_map<std::string, std::vector<uint8_t>>& attr_data)
    : vectors_(std::move(vectors)), attr_nbytes_(attr_nbytes), attr_size_(attr_size), attr_data_(attr_data) {
    current_num_vectors_added = 0;
    current_num_attrs_added = 0;
}

Status
VectorSource::Add(const segment::SegmentWriterPtr& segment_writer_ptr, const meta::SegmentSchema& table_file_schema,
                  const size_t& num_vectors_to_add, size_t& num_vectors_added) {
    uint64_t n = vectors_.vector_count_;
    server::CollectAddMetrics metrics(n, table_file_schema.dimension_);

    num_vectors_added =
        current_num_vectors_added + num_vectors_to_add <= n ? num_vectors_to_add : n - current_num_vectors_added;
    IDNumbers vector_ids_to_add;
    if (vectors_.id_array_.empty()) {
        SafeIDGenerator& id_generator = SafeIDGenerator::GetInstance();
        Status status = id_generator.GetNextIDNumbers(num_vectors_added, vector_ids_to_add);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << LogOut("[%s][%ld]", "insert", 0) << "Generate ids fail: " << status.message();
            return status;
        }
    } else {
        vector_ids_to_add.resize(num_vectors_added);
        for (size_t pos = current_num_vectors_added; pos < current_num_vectors_added + num_vectors_added; pos++) {
            vector_ids_to_add[pos - current_num_vectors_added] = vectors_.id_array_[pos];
        }
    }

    Status status;
    if (!vectors_.float_data_.empty()) {
        LOG_ENGINE_DEBUG_ << LogOut("[%s][%ld]", "insert", 0) << "Insert float data into segment";
        auto size = num_vectors_added * table_file_schema.dimension_ * sizeof(float);
        float* ptr = vectors_.float_data_.data() + current_num_vectors_added * table_file_schema.dimension_;
        status = segment_writer_ptr->AddVectors(table_file_schema.file_id_, (uint8_t*)ptr, size, vector_ids_to_add);
    } else if (!vectors_.binary_data_.empty()) {
        LOG_ENGINE_DEBUG_ << LogOut("[%s][%ld]", "insert", 0) << "Insert binary data into segment";
        std::vector<uint8_t> vectors;
        auto size = num_vectors_added * SingleVectorSize(table_file_schema.dimension_) * sizeof(uint8_t);
        uint8_t* ptr =
            vectors_.binary_data_.data() + current_num_vectors_added * SingleVectorSize(table_file_schema.dimension_);
        status = segment_writer_ptr->AddVectors(table_file_schema.file_id_, ptr, size, vector_ids_to_add);
    }

    // Clear vector data
    if (status.ok()) {
        current_num_vectors_added += num_vectors_added;
        // TODO(zhiru): remove
        vector_ids_.insert(vector_ids_.end(), std::make_move_iterator(vector_ids_to_add.begin()),
                           std::make_move_iterator(vector_ids_to_add.end()));
    } else {
        LOG_ENGINE_ERROR_ << LogOut("[%s][%ld]", "insert", 0) << "VectorSource::Add failed: " + status.ToString();
    }

    return status;
}

Status
VectorSource::AddEntities(const milvus::segment::SegmentWriterPtr& segment_writer_ptr,
                          const milvus::engine::meta::SegmentSchema& collection_file_schema,
                          const size_t& num_entities_to_add, size_t& num_entities_added) {
    // TODO: n = vectors_.vector_count_;???
    uint64_t n = vectors_.vector_count_;
    num_entities_added =
        current_num_attrs_added + num_entities_to_add <= n ? num_entities_to_add : n - current_num_attrs_added;
    IDNumbers vector_ids_to_add;
    if (vectors_.id_array_.empty()) {
        SafeIDGenerator& id_generator = SafeIDGenerator::GetInstance();
        Status status = id_generator.GetNextIDNumbers(num_entities_added, vector_ids_to_add);
        if (!status.ok()) {
            return status;
        }
    } else {
        vector_ids_to_add.resize(num_entities_added);
        for (size_t pos = current_num_attrs_added; pos < current_num_attrs_added + num_entities_added; pos++) {
            vector_ids_to_add[pos - current_num_attrs_added] = vectors_.id_array_[pos];
        }
    }

    Status status;
    status =
        segment_writer_ptr->AddAttrs(collection_file_schema.collection_id_, attr_size_, attr_data_, vector_ids_to_add);

    if (status.ok()) {
        current_num_attrs_added += num_entities_added;
    } else {
        LOG_ENGINE_ERROR_ << LogOut("[%s][%ld]", "insert", 0) << "Generate ids fail: " << status.message();
        return status;
    }

    std::vector<uint8_t> vectors;
    auto size = num_entities_added * collection_file_schema.dimension_ * sizeof(float);
    vectors.resize(size);
    memcpy(vectors.data(), vectors_.float_data_.data() + current_num_vectors_added * collection_file_schema.dimension_,
           size);
    LOG_ENGINE_DEBUG_ << LogOut("[%s][%ld]", "insert", 0) << "Insert into segment";
    status = segment_writer_ptr->AddVectors(collection_file_schema.file_id_, vectors, vector_ids_to_add);
    if (status.ok()) {
        current_num_vectors_added += num_entities_added;
        vector_ids_.insert(vector_ids_.end(), std::make_move_iterator(vector_ids_to_add.begin()),
                           std::make_move_iterator(vector_ids_to_add.end()));
    }

    // don't need to add current_num_attrs_added again
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << LogOut("[%s][%ld]", "insert", 0) << "VectorSource::Add failed: " + status.ToString();
        return status;
    }

    return status;
}

size_t
VectorSource::GetNumVectorsAdded() {
    return current_num_vectors_added;
}

size_t
VectorSource::SingleVectorSize(uint16_t dimension) {
    if (!vectors_.float_data_.empty()) {
        return dimension * FLOAT_TYPE_SIZE;
    } else if (!vectors_.binary_data_.empty()) {
        return dimension / 8;
    }

    return 0;
}

size_t
VectorSource::SingleEntitySize(uint16_t dimension) {
    // TODO(yukun) add entity type and size compute
    size_t size = 0;
    size += dimension * FLOAT_TYPE_SIZE;
    auto nbyte_it = attr_nbytes_.begin();
    for (; nbyte_it != attr_nbytes_.end(); ++nbyte_it) {
        size += nbyte_it->second;
    }
    return size;
}

bool
VectorSource::AllAdded() {
    return (current_num_vectors_added == vectors_.vector_count_);
}

IDNumbers
VectorSource::GetVectorIds() {
    return vector_ids_;
}

}  // namespace engine
}  // namespace milvus
