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

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "FieldMeta.h"
#include "pb/schema.pb.h"

namespace milvus {

class Schema {
 public:
    FieldId
    AddDebugField(const std::string& name, DataType data_type) {
        static int64_t debug_id = 1000;
        auto field_id = FieldId(debug_id);
        debug_id += 2;
        this->AddField(FieldName(name), field_id, data_type);
        return field_id;
    }

    // auto gen field_id for convenience
    FieldId
    AddDebugField(const std::string& name, DataType data_type, int64_t dim, std::optional<MetricType> metric_type) {
        static int64_t debug_id = 2001;
        auto field_id = FieldId(debug_id);
        debug_id += 2;
        auto field_meta = FieldMeta(FieldName(name), field_id, data_type, dim, metric_type);
        this->AddField(std::move(field_meta));
        return field_id;
    }

    // scalar type
    void
    AddField(const FieldName& name, const FieldId id, DataType data_type) {
        auto field_meta = FieldMeta(name, id, data_type);
        this->AddField(std::move(field_meta));
    }

    // vector type
    void
    AddField(const FieldName& name,
             const FieldId id,
             DataType data_type,
             int64_t dim,
             std::optional<MetricType> metric_type) {
        auto field_meta = FieldMeta(name, id, data_type, dim, metric_type);
        this->AddField(std::move(field_meta));
    }

    void
    set_auto_id(bool is_auto_id) {
        is_auto_id_ = is_auto_id;
    }

    void
    set_primary_key(FieldOffset field_offset) {
        is_auto_id_ = false;
        this->primary_key_offset_opt_ = field_offset;
    }

    bool
    get_is_auto_id() const {
        return is_auto_id_;
    }

    auto
    begin() const {
        return fields_.begin();
    }

    auto
    end() const {
        return fields_.end();
    }

    int
    size() const {
        return fields_.size();
    }

    const FieldMeta&
    operator[](FieldOffset field_offset) const {
        Assert(field_offset.get() >= 0);
        Assert(field_offset.get() < fields_.size());
        return fields_[field_offset.get()];
    }

    auto
    get_total_sizeof() const {
        return total_sizeof_;
    }

    const std::vector<int64_t>&
    get_sizeof_infos() const {
        return sizeof_infos_;
    }

    FieldOffset
    get_offset(const FieldName& field_name) const {
        Assert(name_offsets_.count(field_name));
        return name_offsets_.at(field_name);
    }

    FieldOffset
    get_offset(const FieldId& field_id) const {
        Assert(id_offsets_.count(field_id));
        return id_offsets_.at(field_id);
    }

    const std::vector<FieldMeta>&
    get_fields() const {
        return fields_;
    }

    const FieldMeta&
    operator[](const FieldName& field_name) const {
        auto offset_iter = name_offsets_.find(field_name);
        AssertInfo(offset_iter != name_offsets_.end(), "Cannot find field_name: " + field_name.get());
        auto offset = offset_iter->second;
        return (*this)[offset];
    }

    std::optional<FieldOffset>
    get_primary_key_offset() const {
        return primary_key_offset_opt_;
    }

 public:
    static std::shared_ptr<Schema>
    ParseFrom(const milvus::proto::schema::CollectionSchema& schema_proto);

    void
    AddField(FieldMeta&& field_meta) {
        auto offset = fields_.size();
        AssertInfo(!name_offsets_.count(field_meta.get_name()), "duplicated field name");
        name_offsets_.emplace(field_meta.get_name(), offset);
        AssertInfo(!id_offsets_.count(field_meta.get_id()), "duplicated field id");
        id_offsets_.emplace(field_meta.get_id(), offset);

        auto field_sizeof = field_meta.get_sizeof();
        sizeof_infos_.push_back(std::move(field_sizeof));
        fields_.emplace_back(std::move(field_meta));
        total_sizeof_ += field_sizeof;
    }

 private:
    // this is where data holds
    std::vector<FieldMeta> fields_;

    // a mapping for random access
    std::unordered_map<FieldName, FieldOffset> name_offsets_;  // field_name -> offset
    std::unordered_map<FieldId, FieldOffset> id_offsets_;      // field_id -> offset
    std::vector<int64_t> sizeof_infos_;
    int total_sizeof_ = 0;
    bool is_auto_id_ = true;
    std::optional<FieldOffset> primary_key_offset_opt_;
};

using SchemaPtr = std::shared_ptr<Schema>;

}  // namespace milvus
