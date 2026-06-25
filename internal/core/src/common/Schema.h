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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "Consts.h"
#include "FieldMeta.h"
#include "NamedType/named_type_impl.hpp"
#include "NamedType/underlying_functionalities.hpp"
#include "arrow/type.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "knowhere/comp/index_param.h"
#include "pb/schema.pb.h"

namespace milvus {

using ArrowSchemaPtr = std::shared_ptr<arrow::Schema>;
static int64_t debug_id = START_USER_FIELDID;

std::optional<FieldId>
ParseFieldIdColumnName(const std::string& column_name);

bool
IsMilvusTableExternalSpec(const std::string& external_spec);

// Physical column mapping for a schema field in external storage manifests.
struct PhysicalColumnMapping {
    std::string schema_column_name;
    std::string storage_column_name;
    bool is_external_column = false;
};

PhysicalColumnMapping
ResolvePhysicalColumnMapping(
    bool is_milvus_table,
    const milvus::proto::schema::FieldSchema& field_schema);

inline std::optional<std::string>
GetStructNameForArrayField(const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();
    if (data_type != DataType::ARRAY && data_type != DataType::VECTOR_ARRAY) {
        return std::nullopt;
    }

    const auto& field_name = field_meta.get_name().get();
    auto left = field_name.find('[');
    auto right = field_name.find(']', left == std::string::npos ? 0 : left);
    if (left == std::string::npos || right == std::string::npos || left == 0) {
        return std::nullopt;
    }

    return field_name.substr(0, left);
}

class Schema {
 public:
    FieldId
    AddDebugField(const std::string& name,
                  DataType data_type,
                  bool nullable = false) {
        auto field_id = FieldId(debug_id);
        debug_id++;
        if (IsStringDataType(data_type)) {
            // String types require max_length to be set for proper serialization
            constexpr int64_t kDefaultMaxLength = 65535;
            this->AddField(FieldName(name),
                           field_id,
                           data_type,
                           kDefaultMaxLength,
                           nullable,
                           std::nullopt);
        } else {
            this->AddField(
                FieldName(name), field_id, data_type, nullable, std::nullopt);
        }
        return field_id;
    }

    FieldId
    AddDebugFieldWithDefaultValue(const std::string& name,
                                  DataType data_type,
                                  DefaultValueType value,
                                  bool nullable = true) {
        auto field_id = FieldId(debug_id);
        debug_id++;
        if (IsStringDataType(data_type)) {
            // String types require max_length to be set for proper serialization
            constexpr int64_t kDefaultMaxLength = 65535;
            this->AddField(FieldName(name),
                           field_id,
                           data_type,
                           kDefaultMaxLength,
                           nullable,
                           std::move(value));
        } else {
            this->AddField(FieldName(name),
                           field_id,
                           data_type,
                           nullable,
                           std::move(value));
        }
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
                  std::optional<knowhere::MetricType> metric_type,
                  bool nullable = false) {
        auto field_id = FieldId(debug_id);
        debug_id++;
        auto field_meta = FieldMeta(FieldName(name),
                                    field_id,
                                    data_type,
                                    dim,
                                    metric_type,
                                    nullable,
                                    std::nullopt);
        this->AddField(std::move(field_meta));
        return field_id;
    }

    // array of vector type
    FieldId
    AddDebugVectorArrayField(const std::string& name,
                             DataType element_type,
                             int64_t dim,
                             std::optional<knowhere::MetricType> metric_type,
                             bool nullable = false) {
        auto field_id = FieldId(debug_id);
        debug_id++;
        auto field_meta = FieldMeta(FieldName(name),
                                    field_id,
                                    DataType::VECTOR_ARRAY,
                                    element_type,
                                    dim,
                                    metric_type,
                                    nullable);
        this->AddField(std::move(field_meta));
        return field_id;
    }

    // string type
    FieldId
    AddDebugVarcharField(const FieldName& name,
                         DataType data_type,
                         int64_t max_length,
                         bool nullable,
                         bool enable_match,
                         bool enable_analyzer,
                         std::map<std::string, std::string>& params,
                         std::optional<DefaultValueType> default_value) {
        auto field_id = FieldId(debug_id);
        debug_id++;
        auto field_meta = FieldMeta(name,
                                    field_id,
                                    data_type,
                                    max_length,
                                    nullable,
                                    enable_match,
                                    enable_analyzer,
                                    params,
                                    std::move(default_value));
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
            name, id, data_type, dim, metric_type, nullable, std::nullopt);
        this->AddField(std::move(field_meta));
    }

    void
    set_primary_field_id(FieldId field_id) {
        this->primary_field_id_opt_ = field_id;
    }

    void
    set_ttl_field_id(FieldId field_id) {
        this->ttl_field_id_opt_ = field_id;
    }

    void
    set_dynamic_field_id(FieldId field_id) {
        this->dynamic_field_id_opt_ = field_id;
    }

    void
    set_namespace_field_id(FieldId field_id) {
        this->namespace_field_id_opt_ = field_id;
    }

    void
    set_schema_version(uint64_t version) {
        this->schema_version_ = version;
    }

    std::optional<FieldId>
    get_namespace_field_id() const {
        return this->namespace_field_id_opt_;
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

    size_t
    get_field_id_bitset_size() const {
        size_t bitset_size = 0;
        for (const auto& field_id : field_ids_) {
            if (field_id.get() < START_USER_FIELDID) {
                continue;
            }
            auto required_size =
                static_cast<size_t>(field_id.get() - START_USER_FIELDID + 1);
            if (required_size > bitset_size) {
                bitset_size = required_size;
            }
        }
        return bitset_size;
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
        AssertInfo(name_ids_.count(field_name),
                   "Cannot find field_name:{}",
                   field_name.get());
        return name_ids_.at(field_name);
    }

    const std::unordered_map<FieldId, FieldMeta>&
    get_fields() const {
        return fields_;
    }

    bool
    has_field(FieldId field_id) const {
        return fields_.count(field_id) > 0;
    }

    const std::unordered_map<FieldId, FieldMeta>
    get_field_metas(const std::vector<FieldId>& field_ids) {
        std::unordered_map<FieldId, FieldMeta> field_metas;
        for (const auto& field_id : field_ids) {
            field_metas.emplace(field_id, operator[](field_id));
        }
        return field_metas;
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

    std::optional<FieldId>
    get_ttl_field_id() const {
        return ttl_field_id_opt_;
    }

    bool
    is_external_collection() const {
        return !external_source_.empty();
    }

    const std::string&
    get_external_source() const {
        return external_source_;
    }

    const std::string&
    get_external_spec() const {
        return external_spec_;
    }

    void
    set_external_source(const std::string& source) {
        external_source_ = source;
    }

    void
    set_external_spec(const std::string& spec);

    bool
    is_milvus_table_external_collection() const {
        return is_external_collection() && is_milvus_table_external_;
    }

    const ArrowSchemaPtr
    ConvertToArrowSchema() const;

    /// Convert to Arrow schema with field ID strings as field names,
    /// used by Loon FFI / milvus-storage Reader. Internal StorageV3 TEXT
    /// column groups store encoded LOB references as binary bytes, so callers
    /// loading those column groups can request TEXT fields as Binary.
    const ArrowSchemaPtr
    ConvertToLoonArrowSchema(bool text_lob_as_binary = false) const;

    // Mirrors pkg/util/typeutil.StorageColumnResolver in Go. Keep both sides
    // aligned when changing external physical-column rules.
    //
    // Get the list of physical columns for external collections. Source
    // fields use their external storage column names, while Milvus-generated
    // function outputs use numeric field IDs because they are stored by
    // Milvus, not read from the original external source schema.
    std::shared_ptr<std::vector<std::string>>
    GetExternalColumnNames() const;

    // Whether the field is backed by the user's external source data.
    // For milvus-table collections, source fields are addressed by source
    // field ID strings; generated function outputs are not source fields.
    bool
    IsExternalDataField(FieldId field_id) const;

    // Whether an external segment reader can read the field from the segment
    // manifest. This includes source-backed external fields and
    // Milvus-generated function outputs stored under numeric field IDs.
    bool
    IsExternalManifestStoredField(FieldId field_id) const;

    // Real-PK milvus-table segments import source delete logs, so they must
    // also read the source insert timestamp column to keep delete/reinsert
    // ordering identical to the source Milvus segment.
    bool
    RequiresSourceInsertTimestamps() const;

    // Return the physical column name used by external readers. Milvus-table
    // source fields use field ID strings; mapped external fields use
    // external_field; function outputs and internal storage columns use field
    // ID strings.
    std::string
    GetPhysicalColumnName(FieldId field_id) const;

    // Resolve a column group column name to a FieldId.
    // Normal collections: column name is the numeric field ID string.
    // External collections: milvus-table source columns prefer numeric field
    // ID strings; other source formats use external_field metadata; function
    // outputs use numeric field ID strings.
    FieldId
    ResolveColumnFieldId(const std::string& column_name) const;

    proto::schema::CollectionSchema
    ToProto() const;

    void
    UpdateLoadFields(const std::vector<int64_t>& field_ids) {
        load_fields_.clear();
        for (auto field_id : field_ids) {
            load_fields_.emplace(field_id);
        }
    }

    bool
    ShouldLoadField(FieldId field_id) {
        if (bm25_function_output_fields_.count(field_id) > 0) {
            return false;
        }
        auto it = fields_.find(field_id);
        if (it != fields_.end() && !it->second.NeedLoad()) {
            return false;
        }
        return load_fields_.empty() || load_fields_.count(field_id) > 0;
    }

    std::vector<int64_t>
    load_fields() {
        auto fields = std::vector<int64_t>();
        for (auto field_id : field_ids_) {
            fields.emplace_back(field_id.get());
        }
        return fields;
    }

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

        // Build struct_array_field_cache_ for ARRAY/VECTOR_ARRAY fields.
        if (auto struct_name = GetStructNameForArrayField(field_meta);
            struct_name.has_value()) {
            // Only cache the first array field for each struct.
            if (struct_array_field_cache_.find(*struct_name) ==
                struct_array_field_cache_.end()) {
                struct_array_field_cache_[*struct_name] = field_id;
            }
        }

        fields_.emplace(field_id, std::move(field_meta));
        field_ids_.emplace_back(field_id);
    }

    std::unique_ptr<std::vector<FieldMeta>>
    AbsentFields(Schema& old_schema) const;

    /**
     * @brief Determines whether the specified field should use mmap for data loading.
     *
     * This function checks mmap settings at the field level first. If no field-level
     * setting is found, it falls back to the collection-level mmap configuration.
     *
     * @param field The field ID to check mmap settings for.
     *
     * @return A pair of booleans:
     *         - first:  Whether an mmap setting exists (at field or collection level).
     *         - second: Whether mmap is enabled (only meaningful when first is true).
     *
     * @note If no mmap setting exists at any level, first will be false and second
     *       should be ignored.
     */
    std::pair<bool, bool>
    MmapEnabled(const FieldId& field) const;

    // Find the first array field belonging to a struct (cached)
    const FieldMeta&
    GetFirstArrayFieldInStruct(const std::string& struct_name) const;

    // Resolve the array field a MATCH_*/element_filter targets: accepts a scalar
    // ARRAY/VECTOR_ARRAY field name (returns that field) or a struct array name
    // (returns its first array sub-field).
    const FieldMeta&
    ResolveArrayElementField(const std::string& name) const;

    DataType
    GetFieldType(const FieldId& field_id) const {
        AssertInfo(fields_.count(field_id),
                   "field_id:{} does not exist in the schema",
                   field_id.get());
        auto& meta = fields_.at(field_id);
        return meta.get_data_type();
    }

    const std::string&
    GetFieldName(const FieldId& field_id) const {
        AssertInfo(fields_.count(field_id),
                   "field_id:{} does not exist in the schema",
                   field_id.get());
        auto& meta = fields_.at(field_id);
        return meta.get_name().get();
    }

    /**
     * @brief Get the warmup policy for a specific field.
     *
     * This function checks warmup policy at the field level first. If no field-level
     * setting is found, it falls back to the appropriate collection-level warmup
     * configuration based on whether the field is vector/scalar and index/field.
     *
     * @param field The field ID to check warmup policy for.
     * @param is_vector Whether the field is a vector field.
     * @param is_index Whether this is for index loading (vs raw field data).
     *
     * @return A pair where:
     *         - first:  Whether a warmup policy exists (at field or collection level).
     *         - second: The warmup policy string ("disable", "sync", etc.).
     *                   Only meaningful when first is true.
     *
     * @note If no warmup policy exists at any level, first will be false and second
     *       should be ignored (use global config fallback).
     */
    std::pair<bool, std::string>
    WarmupPolicy(const FieldId& field, bool is_vector, bool is_index) const;

    // True if the field carries FieldSchema::is_function_output.
    bool
    is_function_output(const FieldId& field_id) const {
        return function_output_field_ids_.count(field_id) > 0;
    }

    const std::unordered_set<FieldId>&
    function_output_field_ids() const {
        return function_output_field_ids_;
    }

    void
    add_function_output_field_id(const FieldId& field_id) {
        function_output_field_ids_.insert(field_id);
    }

    // Storage column name used by packed manifests for a given field.
    std::string
    get_storage_column_name(const FieldId& field_id) const {
        return GetPhysicalColumnName(field_id);
    }

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
    std::optional<FieldId> namespace_field_id_opt_;
    std::optional<FieldId> ttl_field_id_opt_;

    // field partial load list
    // work as hint now
    std::unordered_set<FieldId> load_fields_;
    std::unordered_set<FieldId> bm25_function_output_fields_;

    // schema_version_, currently marked with update timestamp
    uint64_t schema_version_;

    // mmap settings
    bool has_mmap_setting_ = false;
    bool mmap_enabled_ = false;
    std::unordered_map<FieldId, bool> mmap_fields_;

    // Cache for struct_name -> first array field mapping (built during AddField)
    std::unordered_map<std::string, FieldId> struct_array_field_cache_;

    // warmup policy settings
    // Valid values: "disable", "sync", "async" (empty string means no setting)
    // Collection-level warmup policies for different data types
    std::optional<std::string> warmup_vector_index_ = std::nullopt;
    std::optional<std::string> warmup_scalar_index_ = std::nullopt;
    std::optional<std::string> warmup_scalar_field_ = std::nullopt;
    std::optional<std::string> warmup_vector_field_ = std::nullopt;
    // Per-field warmup policy (key: "warmup" in field type_params)
    std::unordered_map<FieldId, std::string> warmup_fields_;

    // External collection properties
    std::string
        external_source_;  // External data source identifier (e.g., S3 path, table name)
    std::string
        external_spec_;  // External data source specification (JSON format)

    // Field IDs marked as function outputs by FieldSchema::is_function_output.
    std::unordered_set<FieldId> function_output_field_ids_;

    bool is_milvus_table_external_ = false;
};

using SchemaPtr = std::shared_ptr<Schema>;
using SafeSchemaPtr = std::atomic<SchemaPtr*>;
}  // namespace milvus
