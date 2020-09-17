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
#include "db/SnapshotUtils.h"
#include "db/snapshot/Snapshots.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "segment/Utils.h"
#include "utils/Log.h"

#include <algorithm>
#include <functional>
#include <utility>

namespace milvus {
namespace engine {

const char* COLLECTIONS_FOLDER = "/collections";

Status
Segment::CopyOutRawData(SegmentPtr& target) {
    if (target == nullptr) {
        target = std::make_shared<engine::Segment>();
    }

    target->field_types_ = this->field_types_;
    target->fixed_fields_width_ = this->fixed_fields_width_;
    target->row_count_ = this->row_count_;
    target->fixed_fields_.clear();
    target->variable_fields_.clear();

    for (auto& pair : fixed_fields_) {
        engine::BinaryDataPtr& raw_data = pair.second;
        size_t data_size = raw_data->data_.size();
        engine::BinaryDataPtr new_raw_data = std::make_shared<engine::BinaryData>();
        new_raw_data->data_.resize(data_size);
        memcpy(new_raw_data->data_.data(), raw_data->data_.data(), data_size);
        target->fixed_fields_.insert(std::make_pair(pair.first, new_raw_data));
    }

    for (auto& pair : variable_fields_) {
        engine::VaribleDataPtr& raw_data = pair.second;
        size_t data_size = raw_data->data_.size();
        size_t offset_size = raw_data->offset_.size();
        engine::VaribleDataPtr new_raw_data = std::make_shared<engine::VaribleData>();
        new_raw_data->data_.resize(data_size);
        memcpy(new_raw_data->data_.data(), raw_data->data_.data(), data_size);
        new_raw_data->offset_.resize(offset_size);
        memcpy(new_raw_data->offset_.data(), raw_data->offset_.data(), offset_size);
        target->variable_fields_.insert(std::make_pair(pair.first, new_raw_data));
    }

    return Status::OK();
}

Status
Segment::ShareToChunkData(DataChunkPtr& chunk_ptr) {
    if (chunk_ptr == nullptr) {
        chunk_ptr = std::make_shared<engine::DataChunk>();
    }

    chunk_ptr->fixed_fields_ = this->fixed_fields_;
    chunk_ptr->variable_fields_ = this->variable_fields_;
    chunk_ptr->count_ = this->row_count_;

    return Status::OK();
}

Status
Segment::SetFields(int64_t collection_id) {
    snapshot::ScopedSnapshotT ss;
    STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_id));

    auto& fields = ss->GetResources<snapshot::Field>();
    for (auto& kv : fields) {
        const snapshot::FieldPtr& field = kv.second.Get();
        STATUS_CHECK(AddField(field));
    }

    return Status::OK();
}

Status
Segment::AddField(const snapshot::FieldPtr& field) {
    if (field == nullptr) {
        return Status(DB_ERROR, "Field is null pointer");
    }

    std::string name = field->GetName();
    auto ftype = static_cast<DataType>(field->GetFtype());
    if (IsVectorField(field)) {
        json params = field->GetParams();
        if (params.find(knowhere::meta::DIM) == params.end()) {
            std::string msg = "Vector field params must contain: dimension";
            LOG_SERVER_ERROR_ << msg;
            return Status(DB_ERROR, msg);
        }

        int64_t field_width = 0;
        int64_t dimension = params[knowhere::meta::DIM];
        if (ftype == DataType::VECTOR_BINARY) {
            field_width += (dimension / 8);
        } else {
            field_width += (dimension * sizeof(float));
        }
        AddField(name, ftype, field_width);
    } else {
        AddField(name, ftype);
    }

    return Status::OK();
}

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
        default:
            break;
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
    AppendChunk(chunk_ptr, from, to);

    return Status::OK();
}

Status
Segment::Reserve(const std::vector<std::string>& field_names, int64_t count) {
    if (count <= 0) {
        return Status(DB_ERROR, "Invalid input fot segment resize");
    }

    if (field_names.empty()) {
        for (auto& width_iter : fixed_fields_width_) {
            int64_t resize_bytes = count * width_iter.second;

            auto& data = fixed_fields_[width_iter.first];
            if (data == nullptr) {
                data = std::make_shared<BinaryData>();
            }
            data->data_.resize(resize_bytes);
        }
    } else {
        for (const auto& name : field_names) {
            auto iter_width = fixed_fields_width_.find(name);
            if (iter_width == fixed_fields_width_.end()) {
                return Status(DB_ERROR, "Invalid input fot segment resize");
            }

            int64_t resize_bytes = count * iter_width->second;

            auto& data = fixed_fields_[name];
            if (data == nullptr) {
                data = std::make_shared<BinaryData>();
            }
            data->data_.resize(resize_bytes);
        }
    }

    return Status::OK();
}

Status
Segment::AppendChunk(const DataChunkPtr& chunk_ptr, int64_t from, int64_t to) {
    if (chunk_ptr == nullptr || from < 0 || to < 0 || from > to) {
        return Status(DB_ERROR, "Invalid input fot segment append");
    }

    int64_t add_count = to - from;
    if (add_count == 0) {
        add_count = 1;  // n ~ n also means append the No.n
    }
    for (auto& width_iter : fixed_fields_width_) {
        auto input = chunk_ptr->fixed_fields_.find(width_iter.first);
        if (input == chunk_ptr->fixed_fields_.end()) {
            continue;
        }
        auto& data = fixed_fields_[width_iter.first];
        if (data == nullptr) {
            fixed_fields_[width_iter.first] = input->second;
            continue;
        }

        size_t origin_bytes = data->data_.size();
        int64_t add_bytes = add_count * width_iter.second;
        int64_t previous_bytes = row_count_ * width_iter.second;
        int64_t target_bytes = previous_bytes + add_bytes;
        if (data->data_.size() < target_bytes) {
            data->data_.resize(target_bytes);
        }
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
Segment::DeleteEntity(std::vector<offset_t>& offsets) {
    if (offsets.size() == 0) {
        return Status::OK();
    }

    // calculate copy ranges
    int64_t delete_count = 0;
    segment::CopyRanges copy_ranges;
    segment::CalcCopyRangesWithOffset(offsets, row_count_, copy_ranges, delete_count);

    // delete entity data from max offset to min offset
    for (auto& pair : fixed_fields_) {
        int64_t width = fixed_fields_width_[pair.first];
        if (width == 0 || pair.second == nullptr) {
            continue;
        }

        auto& data = pair.second;

        std::vector<uint8_t> new_data;
        segment::CopyDataWithRanges(data->data_, width, copy_ranges, new_data);
        data->data_.swap(new_data);
    }

    // reset row count
    row_count_ -= delete_count;

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
    if (row_count_ == 0 && width != 0) {
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
