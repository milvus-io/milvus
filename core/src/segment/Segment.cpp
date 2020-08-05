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

#include "segment/Segment.h"
#include "utils/Log.h"

#include <utility>

namespace milvus {
namespace engine {

const char* COLLECTIONS_FOLDER = "/collections";

Status
Segment::AddField(const std::string& field_name, DataType field_type, int64_t field_width) {
    if (field_types_.find(field_name) != field_types_.end()) {
        return Status(DB_ERROR, "duplicate field: " + field_name);
    }

    int64_t real_field_width = 0;
    switch (field_type) {
        case DataType::BOOL:
            real_field_width = sizeof(bool);
            break;
        case DataType::DOUBLE:
            real_field_width = sizeof(double);
            break;
        case DataType::FLOAT:
            real_field_width = sizeof(float);
            break;
        case DataType::INT8:
            real_field_width = sizeof(uint8_t);
            break;
        case DataType::INT16:
            real_field_width = sizeof(uint16_t);
            break;
        case DataType::INT32:
            real_field_width = sizeof(uint32_t);
            break;
        case DataType::INT64:
            real_field_width = sizeof(uint64_t);
            break;
        case DataType::VECTOR_FLOAT:
        case DataType::VECTOR_BINARY: {
            if (field_width <= 0) {
                std::string msg = "vecor field dimension required: " + field_name;
                LOG_SERVER_ERROR_ << msg;
                return Status(DB_ERROR, msg);
            }

            real_field_width = field_width;
            break;
        }
    }

    field_types_.insert(std::make_pair(field_name, field_type));
    fixed_fields_width_.insert(std::make_pair(field_name, real_field_width));

    return Status::OK();
}

Status
Segment::AddChunk(const DataChunkPtr& chunk_ptr) {
    if (chunk_ptr == nullptr || chunk_ptr->count_ == 0) {
        return Status(DB_ERROR, "invalid input");
    }

    return AddChunk(chunk_ptr, 0, chunk_ptr->count_);
}

Status
Segment::AddChunk(const DataChunkPtr& chunk_ptr, int64_t from, int64_t to) {
    if (chunk_ptr == nullptr || from < 0 || to < 0 || from > chunk_ptr->count_ || to > chunk_ptr->count_ ||
        from >= to) {
        return Status(DB_ERROR, "invalid input");
    }

    // check input
    for (auto& iter : chunk_ptr->fixed_fields_) {
        if (iter.second == nullptr) {
            return Status(DB_ERROR, "illegal field: " + iter.first);
        }

        auto width_iter = fixed_fields_width_.find(iter.first);
        if (width_iter == fixed_fields_width_.end()) {
            return Status(DB_ERROR, "field not yet defined: " + iter.first);
        }

        if (iter.second->Size() != width_iter->second * chunk_ptr->count_) {
            return Status(DB_ERROR, "illegal field: " + iter.first);
        }
    }

    // consume
    int64_t add_count = to - from;
    for (auto& width_iter : fixed_fields_width_) {
        auto input = chunk_ptr->fixed_fields_.find(width_iter.first);
        auto& data = fixed_fields_[width_iter.first];
        if (data == nullptr) {
            fixed_fields_[width_iter.first] = input->second;
            continue;
        }

        size_t origin_bytes = data->data_.size();
        int64_t add_bytes = add_count * width_iter.second;
        int64_t previous_bytes = row_count_ * width_iter.second;
        int64_t target_bytes = previous_bytes + add_bytes;
        data->data_.resize(target_bytes);
        if (input == chunk_ptr->fixed_fields_.end()) {
            // this field is not provided, complicate by 0
            memset(data->data_.data() + origin_bytes, 0, target_bytes - origin_bytes);
        } else {
            // complicate by 0
            if (origin_bytes < previous_bytes) {
                memset(data->data_.data() + origin_bytes, 0, previous_bytes - origin_bytes);
            }
            // copy input into this field
            memcpy(data->data_.data() + previous_bytes, input->second->data_.data() + from * width_iter.second,
                   add_bytes);
        }
    }

    row_count_ += add_count;

    return Status::OK();
}

Status
Segment::DeleteEntity(int64_t offset) {
    if (offset > row_count_) {
        return Status(DB_ERROR, "Invalid input");
    }

    for (auto& pair : fixed_fields_) {
        int64_t width = fixed_fields_width_[pair.first];
        if (width != 0) {
            auto step = offset * width;
            BinaryDataPtr& data = pair.second;
            if (data == nullptr) {
                continue;
            }

            data->data_.erase(data->data_.begin() + step, data->data_.begin() + step + width);
        }
    }
    row_count_--;

    return Status::OK();
}

Status
Segment::GetFieldType(const std::string& field_name, DataType& type) {
    auto iter = field_types_.find(field_name);
    if (iter == field_types_.end()) {
        return Status(DB_ERROR, "invalid field name: " + field_name);
    }

    type = iter->second;
    return Status::OK();
}

Status
Segment::GetFixedFieldWidth(const std::string& field_name, int64_t& width) {
    auto iter = fixed_fields_width_.find(field_name);
    if (iter == fixed_fields_width_.end()) {
        return Status(DB_ERROR, "invalid field name: " + field_name);
    }

    width = iter->second;
    return Status::OK();
}

Status
Segment::GetFixedFieldData(const std::string& field_name, BinaryDataPtr& data) {
    auto iter = fixed_fields_.find(field_name);
    if (iter == fixed_fields_.end()) {
        return Status(DB_ERROR, "invalid field name: " + field_name);
    }

    data = iter->second;
    return Status::OK();
}

Status
Segment::SetFixedFieldData(const std::string& field_name, BinaryDataPtr& data) {
    if (data == nullptr) {
        return Status(DB_ERROR, "Could not set null pointer");
    }

    int64_t width = 0;
    auto status = GetFixedFieldWidth(field_name, width);
    if (!status.ok()) {
        return status;
    }

    fixed_fields_[field_name] = data;
    if (row_count_ == 0) {
        row_count_ = data->Size() / width;
    }
    return Status::OK();
}

Status
Segment::GetVectorIndex(const std::string& field_name, knowhere::VecIndexPtr& index) {
    index = nullptr;
    auto iter = vector_indice_.find(field_name);
    if (iter == vector_indice_.end()) {
        return Status(DB_ERROR, "Invalid field name: " + field_name);
    }

    index = iter->second;
    return Status::OK();
}

Status
Segment::SetVectorIndex(const std::string& field_name, const knowhere::VecIndexPtr& index) {
    vector_indice_[field_name] = index;
    return Status::OK();
}

Status
Segment::GetStructuredIndex(const std::string& field_name, knowhere::IndexPtr& index) {
    index = nullptr;
    auto iter = structured_indice_.find(field_name);
    if (iter == structured_indice_.end()) {
        return Status(DB_ERROR, "invalid field name: " + field_name);
    }

    index = iter->second;
    return Status::OK();
}

Status
Segment::SetStructuredIndex(const std::string& field_name, const knowhere::IndexPtr& index) {
    structured_indice_[field_name] = index;
    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
