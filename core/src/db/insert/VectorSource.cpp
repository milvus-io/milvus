// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "db/insert/VectorSource.h"
#include "db/engine/EngineFactory.h"
#include "db/engine/ExecutionEngine.h"
#include "metrics/Metrics.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {

VectorSource::VectorSource(VectorsData& vectors)
    : vectors_(vectors), id_generator_(std::make_shared<SimpleIDGenerator>()) {
    current_num_vectors_added = 0;
}

Status
VectorSource::Add(const ExecutionEnginePtr& execution_engine, const meta::TableFileSchema& table_file_schema,
                  const size_t& num_vectors_to_add, size_t& num_vectors_added) {
    uint64_t n = vectors_.vector_count_;
    server::CollectAddMetrics metrics(n, table_file_schema.dimension_);

    num_vectors_added =
        current_num_vectors_added + num_vectors_to_add <= n ? num_vectors_to_add : n - current_num_vectors_added;
    IDNumbers vector_ids_to_add;
    if (vectors_.id_array_.empty()) {
        id_generator_->GetNextIDNumbers(num_vectors_added, vector_ids_to_add);
    } else {
        vector_ids_to_add.resize(num_vectors_added);
        for (int pos = current_num_vectors_added; pos < current_num_vectors_added + num_vectors_added; pos++) {
            vector_ids_to_add[pos - current_num_vectors_added] = vectors_.id_array_[pos];
        }
    }

    Status status;
    if (!vectors_.float_data_.empty()) {
        status = execution_engine->AddWithIds(
            num_vectors_added, vectors_.float_data_.data() + current_num_vectors_added * table_file_schema.dimension_,
            vector_ids_to_add.data());
    } else if (!vectors_.binary_data_.empty()) {
        status = execution_engine->AddWithIds(
            num_vectors_added,
            vectors_.binary_data_.data() + current_num_vectors_added * SingleVectorSize(table_file_schema.dimension_),
            vector_ids_to_add.data());
    }

    if (status.ok()) {
        current_num_vectors_added += num_vectors_added;
        vector_ids_.insert(vector_ids_.end(), std::make_move_iterator(vector_ids_to_add.begin()),
                           std::make_move_iterator(vector_ids_to_add.end()));
    } else {
        ENGINE_LOG_ERROR << "VectorSource::Add failed: " + status.ToString();
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
