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

#include "db/insert/SSVectorSource.h"

#include <utility>
#include <vector>

#include "db/engine/EngineFactory.h"
#include "db/engine/ExecutionEngine.h"
#include "metrics/Metrics.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace engine {

SSVectorSource::SSVectorSource(const DataChunkPtr& chunk) : chunk_(chunk) {
}

Status
SSVectorSource::Add(const milvus::segment::SegmentWriterPtr& segment_writer_ptr, const size_t& num_entities_to_add,
                    size_t& num_entities_added) {
    // TODO: n = vectors_.vector_count_;???
    uint64_t n = chunk_->count_;
    num_entities_added = current_num_added_ + num_entities_to_add <= n ? num_entities_to_add : n - current_num_added_;
    //    IDNumbers vector_ids_to_add;
    //    if (vectors_.id_array_.empty()) {
    //        SafeIDGenerator& id_generator = SafeIDGenerator::GetInstance();
    //        Status status = id_generator.GetNextIDNumbers(num_entities_added, vector_ids_to_add);
    //        if (!status.ok()) {
    //            return status;
    //        }
    //    } else {
    //        vector_ids_to_add.resize(num_entities_added);
    //        for (size_t pos = current_num_attrs_added; pos < current_num_attrs_added + num_entities_added; pos++) {
    //            vector_ids_to_add[pos - current_num_attrs_added] = vectors_.id_array_[pos];
    //        }
    //    }
    //
    //    Status status;
    //    status = segment_writer_ptr->AddAttrs("", attr_size_, attr_data_, vector_ids_to_add);
    //
    //    if (status.ok()) {
    //        current_num_attrs_added += num_entities_added;
    //    } else {
    //        LOG_ENGINE_ERROR_ << LogOut("[%s][%ld]", "insert", 0) << "Generate ids fail: " << status.message();
    //        return status;
    //    }
    //
    //    std::vector<uint8_t> vectors;
    //    auto size = num_entities_added * dimension * sizeof(float);
    //    vectors.resize(size);
    //    memcpy(vectors.data(), vectors_.float_data_.data() + current_num_vectors_added * dimension, size);
    //    LOG_ENGINE_DEBUG_ << LogOut("[%s][%ld]", "insert", 0) << "Insert into segment";
    //    status = segment_writer_ptr->AddVectors("", vectors, vector_ids_to_add);
    //    if (status.ok()) {
    //        current_num_vectors_added += num_entities_added;
    //        vector_ids_.insert(vector_ids_.end(), std::make_move_iterator(vector_ids_to_add.begin()),
    //                           std::make_move_iterator(vector_ids_to_add.end()));
    //    }
    //
    //    // don't need to add current_num_attrs_added again
    //    if (!status.ok()) {
    //        LOG_ENGINE_ERROR_ << LogOut("[%s][%ld]", "insert", 0) << "SSVectorSource::Add failed: " +
    //        status.ToString(); return status;
    //    }
    //
    //    return status;
    return Status::OK();
}

bool
SSVectorSource::AllAdded() {
    return (current_num_added_ >= chunk_->count_);
}

}  // namespace engine
}  // namespace milvus
