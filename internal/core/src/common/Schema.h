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

#include <boost/stacktrace.hpp>
#include "FieldMeta.h"
#include "boost/stacktrace/frame.hpp"
#include "boost/stacktrace/stacktrace_fwd.hpp"
#include "pb/schema.pb.h"
#include "log/Log.h"
#include "Consts.h"

#include "arrow/type.h"

namespace milvus {

using ArrowSchemaPtr = std::shared_ptr<arrow::Schema>;
static int64_t debug_id = START_USER_FIELDID;

class Schema {
 public:
    FieldId
    AddDebugField(const std::string& name,
                  DataType data_type,
                  bool nullable = false) {
        auto field_id = FieldId(debug_id);
        debug_id++;
        this->AddField(
            FieldName(name), field_id, data_type, nullable, std::nullopt);
        return field_id;
    }

    FieldId
    AddDebugFieldWithDefaultValue(const std::string& name,
                                  DataType data_type,
                                  DefaultValueType value,
                                  bool nullable = true) {
        auto field_id = FieldId(debug_id);
        debug_id++;
        this->AddField(
            FieldName(name), field_id, data_type, nullable, std::move(value));
        return field_id;
    }

    FieldId
    AddDebugField(const std::string& name,
                  DataType data_type,
                  DataType element_type,
                  bool nullable = false) {
        auto field_id = FieldId(debug_id);
        debug_id++;
        this->AddField(
            FieldName(name), field_id, data_type, element_type, nullable);
        return field_id;
    }

    FieldId
    AddDebugArrayField(const std::string& name,
                       DataType element_type,
                       bool nullable) {
        auto field_id = FieldId(debug_id);
        debug_id++;
        this->AddField(
            FieldName(name), field_id, DataType::ARRAY, element_type, nullable);
        return field_id;
    }

    // auto gen field_id for convenience
    FieldId
    AddDebugField(const std::string& name,
                  DataType data_type,
                  int64_t dim,
                  std::optional<knowhere::MetricType> metric_type) {
        auto field_id = FieldId(debug_id);
        debug_id++;
        auto field_meta = FieldMeta(FieldName(name),
                                    field_id,
                                    data_type,
                                    dim,
                                    metric_type,
                                    false,
                                    std::nullopt);
        this->AddField(std::move(field_meta));
        return field_id;
    }

    // scalar type
    void
    AddField(const FieldName& name,
             const FieldId id,
             DataType data_type,
             bool nullable,
             std::optional<DefaultValueType> default_value) {
        auto field_meta =
            FieldMeta(name, id, data_type, nullable, std::move(default_value));
        this->AddField(std::move(field_meta));
    }

    // array type
    void
    AddField(const FieldName& name,
             const FieldId id,
             DataType data_type,
             DataType element_type,
             bool nullable) {
        auto field_meta = FieldMeta(
            name, id, data_type, element_type, nullable, std::nullopt);
        this->AddField(std::move(field_meta));
    }

    // string type
    void
    AddField(const FieldName& name,
             const FieldId id,
             DataType data_type,
             int64_t max_length,
             bool nullable,
             std::optional<DefaultValueType> default_value) {
        auto field_meta = FieldMeta(name,
                                    id,
                                    data_type,
                                    max_length,
                                    nullable,
                                    std::move(default_value));
        this->AddField(std::move(field_meta));
    }

    // string type
    void
    AddField(const FieldName& name,
             const FieldId id,
             DataType data_type,
             int64_t max_length,
             bool nullable,
             bool enable_match,
             bool enable_analyzer,
             std::map<std::string, std::string>& params,
             std::optional<DefaultValueType> default_value) {
        auto field_meta = FieldMeta(name,
                                    id,
                                    data_type,
                                    max_length,
                                    nullable,
                                    enable_match,
                                    enable_analyzer,
                                    params,
                                    std::move(default_value));
        this->AddField(std::move(field_meta));
    }

    // vector type
    void
    AddField(const FieldName& name,
             const FieldId id,
             DataType data_type,
             int64_t dim,
             std::optional<knowhere::MetricType> metric_type,
             bool nullable) {
        auto field_meta = FieldMeta(
            name, id, data_type, dim, metric_type, false, std::nullopt);
        this->AddField(std::move(field_meta));
    }

    void
    set_primary_field_id(FieldId field_id) {
        this->primary_field_id_opt_ = field_id;
    }

    void
    set_dynamic_field_id(FieldId field_id) {
        this->dynamic_field_id_opt_ = field_id;
    }

    void
    set_schema_version(uint64_t version) {
        this->schema_version_ = version;
    }

    uint64_t
    get_schema_version() const {
        return this->schema_version_;
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
    operator[](FieldId field_id) const {
        Assert(field_id.get() >= 0);
        AssertInfo(fields_.find(field_id) != fields_.end(),
                   "Cannot find field with field_id: " +
                       std::to_string(field_id.get()));
        return fields_.at(field_id);
    }

    FieldId
    get_field_id(const FieldName& field_name) const {
        AssertInfo(name_ids_.count(field_name), "Cannot find field_name");
        return name_ids_.at(field_name);
    }

    const std::unordered_map<FieldId, FieldMeta>&
    get_fields() const {
        return fields_;
    }

    const std::vector<FieldId>&
    get_field_ids() const {
        return field_ids_;
    }

    const FieldMeta&
    operator[](const FieldName& field_name) const {
        auto id_iter = name_ids_.find(field_name);
        AssertInfo(id_iter != name_ids_.end(),
                   "Cannot find field with field_name: " + field_name.get());
        return fields_.at(id_iter->second);
    }

    std::optional<FieldId>
    get_primary_field_id() const {
        return primary_field_id_opt_;
    }

    std::optional<FieldId>
    get_dynamic_field_id() const {
        return dynamic_field_id_opt_;
    }

    const ArrowSchemaPtr
    ConvertToArrowSchema() const;

 public:
    static std::shared_ptr<Schema>
    ParseFrom(const milvus::proto::schema::CollectionSchema& schema_proto);

    void
    AddField(FieldMeta&& field_meta) {
        auto field_name = field_meta.get_name();
        auto field_id = field_meta.get_id();
        AssertInfo(!name_ids_.count(field_name), "duplicated field name");
        AssertInfo(!id_names_.count(field_id), "duplicated field id");
        name_ids_.emplace(field_name, field_id);
        id_names_.emplace(field_id, field_name);

        fields_.emplace(field_id, field_meta);
        field_ids_.emplace_back(field_id);
    }

    std::unique_ptr<std::vector<FieldMeta>>
    absent_fields(Schema& old_schema) const;

 private:
    int64_t debug_id = START_USER_FIELDID;
    std::vector<FieldId> field_ids_;

    // this is where data holds
    std::unordered_map<FieldId, FieldMeta> fields_;

    // a mapping for random access
    std::unordered_map<FieldName, FieldId> name_ids_;  // field_name -> field_id
    std::unordered_map<FieldId, FieldName> id_names_;  // field_id -> field_name

    std::optional<FieldId> primary_field_id_opt_;
    std::optional<FieldId> dynamic_field_id_opt_;

    // schema_version_, currently marked with update timestamp
    uint64_t schema_version_;
};

using SchemaPtr = std::shared_ptr<Schema>;

}  // namespace milvus
