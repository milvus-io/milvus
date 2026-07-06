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

#include <algorithm>
#include <charconv>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <tuple>

#include "Schema.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"
#include "common/Consts.h"
#include "common/FieldMeta.h"
#include "common/SystemProperty.h"
#include "common/VirtualPK.h"
#include "milvus-storage/common/constants.h"
#include "nlohmann/json.hpp"
#include "pb/common.pb.h"
#include "protobuf_utils.h"

namespace milvus {

using std::string;
const std::string namespace_field_name = "$namespace_id";

std::optional<FieldId>
ParseFieldIdColumnName(const std::string& column_name) {
    if (column_name.empty()) {
        return std::nullopt;
    }
    if (column_name.size() > 1 && column_name[0] == '0') {
        return std::nullopt;
    }
    for (const auto c : column_name) {
        if (c < '0' || c > '9') {
            return std::nullopt;
        }
    }

    int64_t field_id = 0;
    const auto* begin = column_name.data();
    const auto* end = begin + column_name.size();
    auto [ptr, err] = std::from_chars(begin, end, field_id);
    if (err != std::errc() || ptr != end) {
        return std::nullopt;
    }
    return FieldId(field_id);
}

bool
IsMilvusTableExternalSpec(const std::string& external_spec) {
    if (external_spec.empty()) {
        return false;
    }
    try {
        auto spec = nlohmann::json::parse(external_spec);
        return spec.value("format", "parquet") == "milvus-table";
    } catch (const std::exception&) {
        return false;
    }
}

namespace {

bool
IsMilvusTableExternalDataField(FieldId field_id,
                               const std::string& field_name,
                               bool is_function_output) {
    return !SystemProperty::Instance().IsSystem(field_id) &&
           !is_function_output && field_name != VIRTUAL_PK_FIELD_NAME;
}

}  // namespace

PhysicalColumnMapping
ResolvePhysicalColumnMapping(
    bool is_milvus_table,
    const milvus::proto::schema::FieldSchema& field_schema) {
    PhysicalColumnMapping mapping;
    mapping.schema_column_name = field_schema.name();
    mapping.storage_column_name = std::to_string(field_schema.fieldid());
    if (is_milvus_table &&
        IsMilvusTableExternalDataField(FieldId(field_schema.fieldid()),
                                       field_schema.name(),
                                       field_schema.is_function_output())) {
        mapping.is_external_column = true;
    } else if (!field_schema.external_field().empty()) {
        mapping.storage_column_name = field_schema.external_field();
        mapping.is_external_column = true;
    }
    return mapping;
}

void
Schema::set_external_spec(const std::string& spec) {
    external_spec_ = spec;
    is_milvus_table_external_ = IsMilvusTableExternalSpec(spec);
}

std::shared_ptr<Schema>
Schema::ParseFrom(const milvus::proto::schema::CollectionSchema& schema_proto) {
    auto schema = std::make_shared<Schema>();
    // schema->set_auto_id(schema_proto.autoid());

    // NOTE: only two system

    auto process_field = [&schema, &schema_proto](const auto& child) {
        auto field_id = FieldId(child.fieldid());

        auto f = FieldMeta::ParseFrom(child);
        schema->AddField(std::move(f));

        if (child.is_function_output()) {
            schema->add_function_output_field_id(field_id);
        }

        if (child.is_primary_key()) {
            AssertInfo(!schema->get_primary_field_id().has_value(),
                       "repetitive primary key");
            schema->set_primary_field_id(field_id);
        }

        if (child.is_dynamic()) {
            Assert(schema_proto.enable_dynamic_field());
            AssertInfo(!schema->get_dynamic_field_id().has_value(),
                       "repetitive dynamic field");
            schema->set_dynamic_field_id(field_id);
        }
        if (child.name() == namespace_field_name) {
            schema->set_namespace_field_id(field_id);
        }

        auto [has_setting, enabled] =
            GetBoolFromRepeatedKVs(child.type_params(), MMAP_ENABLED_KEY);
        if (has_setting) {
            schema->mmap_fields_[field_id] = enabled;
        }

        // Parse warmup policy for the field (key: "warmup")
        auto warmup_policy =
            GetStringFromRepeatedKVs(child.type_params(), WARMUP_KEY);
        if (warmup_policy.has_value()) {
            schema->warmup_fields_[field_id] = std::move(warmup_policy).value();
        }
    };

    for (const milvus::proto::schema::FieldSchema& child :
         schema_proto.fields()) {
        process_field(child);
    }

    for (const milvus::proto::schema::StructArrayFieldSchema& child :
         schema_proto.struct_array_fields()) {
        for (const auto& sub_field : child.fields()) {
            process_field(sub_field);
        }
    }

    for (const auto& function : schema_proto.functions()) {
        if (function.type() != milvus::proto::schema::BM25) {
            continue;
        }
        for (const auto output_field_id : function.output_field_ids()) {
            auto field_id = FieldId(output_field_id);
            if (schema->is_function_output(field_id)) {
                schema->bm25_function_output_fields_.emplace(field_id);
            }
        }
        for (const auto& output_field_name : function.output_field_names()) {
            auto it = schema->name_ids_.find(FieldName(output_field_name));
            if (it != schema->name_ids_.end() &&
                schema->is_function_output(it->second)) {
                schema->bm25_function_output_fields_.emplace(it->second);
            }
        }
    }

    std::tie(schema->has_mmap_setting_, schema->mmap_enabled_) =
        GetBoolFromRepeatedKVs(schema_proto.properties(), MMAP_ENABLED_KEY);

    std::optional<std::string> ttl_field_name;
    for (const auto& property : schema_proto.properties()) {
        if (property.key() == COLLECTION_TTL_FIELD_KEY) {
            ttl_field_name = property.value();
            break;
        }
    }
    if (ttl_field_name.has_value()) {
        bool found = false;
        for (const milvus::proto::schema::FieldSchema& child :
             schema_proto.fields()) {
            if (child.name() == ttl_field_name.value()) {
                schema->set_ttl_field_id(FieldId(child.fieldid()));
                found = true;
                break;
            }
        }
        AssertInfo(found, "ttl field name not found in schema fields");
    }
    // Parse collection-level warmup policies
    schema->warmup_vector_index_ = GetStringFromRepeatedKVs(
        schema_proto.properties(), WARMUP_VECTOR_INDEX_KEY);
    schema->warmup_scalar_index_ = GetStringFromRepeatedKVs(
        schema_proto.properties(), WARMUP_SCALAR_INDEX_KEY);
    schema->warmup_scalar_field_ = GetStringFromRepeatedKVs(
        schema_proto.properties(), WARMUP_SCALAR_FIELD_KEY);
    schema->warmup_vector_field_ = GetStringFromRepeatedKVs(
        schema_proto.properties(), WARMUP_VECTOR_FIELD_KEY);

    AssertInfo(schema->get_primary_field_id().has_value(),
               "primary key should be specified");

    // Parse external collection properties
    if (!schema_proto.external_source().empty()) {
        schema->set_external_source(schema_proto.external_source());
        schema->set_external_spec(schema_proto.external_spec());
    }

    return schema;
}

const FieldMeta FieldMeta::RowIdMeta(
    FieldName("RowID"), RowFieldID, DataType::INT64, false, std::nullopt);

const ArrowSchemaPtr
Schema::ConvertToArrowSchema() const {
    arrow::FieldVector arrow_fields;
    arrow_fields.reserve(field_ids_.size());
    for (const auto& field_id : field_ids_) {
        const auto& meta = fields_.at(field_id);
        int dim = IsVectorDataType(meta.get_data_type()) &&
                          !IsSparseFloatVectorDataType(meta.get_data_type())
                      ? meta.get_dim()
                      : 1;

        std::shared_ptr<arrow::DataType> arrow_data_type = nullptr;
        auto data_type = meta.get_data_type();
        if (data_type == DataType::VECTOR_ARRAY) {
            arrow_data_type = GetArrowDataTypeForVectorArray(
                meta.get_element_type(), meta.get_dim());
        } else {
            arrow_data_type = GetArrowDataType(data_type, dim);
        }

        auto arrow_field = std::make_shared<arrow::Field>(
            meta.get_name().get(),
            arrow_data_type,
            meta.is_nullable(),
            arrow::key_value_metadata({milvus_storage::ARROW_FIELD_ID_KEY},
                                      {std::to_string(meta.get_id().get())}));
        arrow_fields.push_back(arrow_field);
    }
    return arrow::schema(arrow_fields);
}

const ArrowSchemaPtr
Schema::ConvertToLoonArrowSchema(bool text_lob_as_binary) const {
    arrow::FieldVector arrow_fields;
    arrow_fields.reserve(field_ids_.size());
    for (const auto& field_id : field_ids_) {
        const auto& meta = fields_.at(field_id);
        int dim = IsVectorDataType(meta.get_data_type()) &&
                          !IsSparseFloatVectorDataType(meta.get_data_type())
                      ? meta.get_dim()
                      : 1;

        std::shared_ptr<arrow::DataType> arrow_data_type = nullptr;
        auto data_type = meta.get_data_type();
        auto is_nullable_dense_vector =
            meta.is_nullable() && IsVectorDataType(data_type) &&
            !IsSparseFloatVectorDataType(data_type) &&
            data_type != DataType::VECTOR_ARRAY;
        if (is_nullable_dense_vector) {
            arrow_data_type = arrow::binary();
        } else if (text_lob_as_binary && data_type == DataType::TEXT) {
            arrow_data_type = arrow::binary();
        } else if (data_type == DataType::VECTOR_ARRAY) {
            arrow_data_type = GetArrowDataTypeForVectorArray(
                meta.get_element_type(), meta.get_dim());
        } else {
            arrow_data_type = GetArrowDataType(data_type, dim);
        }

        auto metadata = is_nullable_dense_vector
                            ? arrow::key_value_metadata(
                                  {"dim"}, {std::to_string(meta.get_dim())})
                            : nullptr;
        auto arrow_field =
            std::make_shared<arrow::Field>(std::to_string(field_id.get()),
                                           arrow_data_type,
                                           meta.is_nullable(),
                                           metadata);
        arrow_fields.push_back(arrow_field);
    }
    return arrow::schema(arrow_fields);
}

proto::schema::CollectionSchema
Schema::ToProto() const {
    proto::schema::CollectionSchema schema_proto;
    schema_proto.set_enable_dynamic_field(dynamic_field_id_opt_.has_value());

    for (const auto& field_id : field_ids_) {
        const auto& meta = fields_.at(field_id);
        auto* field_proto = schema_proto.add_fields();
        *field_proto = meta.ToProto();

        if (primary_field_id_opt_.has_value() &&
            field_id == primary_field_id_opt_.value()) {
            field_proto->set_is_primary_key(true);
        }
        if (dynamic_field_id_opt_.has_value() &&
            field_id == dynamic_field_id_opt_.value()) {
            field_proto->set_is_dynamic(true);
        }
    }

    return schema_proto;
}

std::unique_ptr<std::vector<FieldMeta>>
Schema::AbsentFields(Schema& old_schema) const {
    std::vector<FieldMeta> result;
    result.reserve(fields_.size());
    for (const auto& [field_id, field_meta] : fields_) {
        auto it = old_schema.fields_.find(field_id);
        if (it == old_schema.fields_.end()) {
            result.emplace_back(field_meta);
        }
    }

    return std::make_unique<std::vector<FieldMeta>>(std::move(result));
}

std::shared_ptr<std::vector<std::string>>
Schema::GetExternalColumnNames() const {
    auto columns = std::make_shared<std::vector<std::string>>();
    for (const auto& field_id : field_ids_) {
        auto it = fields_.find(field_id);
        if (it == fields_.end())
            continue;
        if (IsExternalManifestStoredField(field_id)) {
            columns->push_back(GetPhysicalColumnName(field_id));
        }
    }
    return columns;
}

// Mirror pkg/util/typeutil.StorageColumnResolver. Keep Go and C++ behavior
// aligned when changing external physical-column rules.
bool
Schema::IsExternalDataField(FieldId field_id) const {
    auto it = fields_.find(field_id);
    if (it == fields_.end() || !is_external_collection()) {
        return false;
    }
    const auto& meta = it->second;
    if (!is_milvus_table_external_collection()) {
        return meta.is_external_field();
    }
    return IsMilvusTableExternalDataField(
        field_id, meta.get_name().get(), is_function_output(field_id));
}

bool
Schema::IsExternalManifestStoredField(FieldId field_id) const {
    if (!is_external_collection()) {
        return false;
    }
    if (field_id == TimestampFieldID && RequiresSourceInsertTimestamps()) {
        return true;
    }
    return IsExternalDataField(field_id) || is_function_output(field_id);
}

bool
Schema::RequiresSourceInsertTimestamps() const {
    return is_milvus_table_external_collection() &&
           primary_field_id_opt_.has_value() &&
           IsExternalDataField(primary_field_id_opt_.value());
}

std::string
Schema::GetPhysicalColumnName(FieldId field_id) const {
    const auto& meta = (*this)[field_id];
    if (is_milvus_table_external_collection() &&
        IsExternalDataField(field_id)) {
        return std::to_string(field_id.get());
    }
    if (meta.is_external_field()) {
        return meta.get_external_field();
    }
    return std::to_string(field_id.get());
}

FieldId
Schema::ResolveColumnFieldId(const std::string& column_name) const {
    if (is_external_collection()) {
        if (is_milvus_table_external_collection()) {
            if (auto field_id = ParseFieldIdColumnName(column_name);
                field_id.has_value() && fields_.count(field_id.value())) {
                return field_id.value();
            }
        }
        for (const auto& [fid, meta] : fields_) {
            if (meta.is_external_field() &&
                meta.get_external_field() == column_name) {
                return fid;
            }
        }
        if (auto field_id = ParseFieldIdColumnName(column_name);
            field_id.has_value()) {
            return field_id.value();
        }
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "external column '{}' not found in schema",
                  column_name);
    }
    if (auto field_id = ParseFieldIdColumnName(column_name);
        field_id.has_value()) {
        return field_id.value();
    }
    ThrowInfo(ErrorCode::DataFormatBroken,
              "column '{}' is not a valid field id",
              column_name);
}

std::pair<bool, bool>
Schema::MmapEnabled(const FieldId& field_id) const {
    auto it = mmap_fields_.find(field_id);
    // fallback to  collection-level config
    if (it == mmap_fields_.end()) {
        return {has_mmap_setting_, mmap_enabled_};
    }
    return {true, it->second};
}

const FieldMeta&
Schema::GetFirstArrayFieldInStruct(const std::string& struct_name) const {
    auto cache_it = struct_array_field_cache_.find(struct_name);
    if (cache_it != struct_array_field_cache_.end()) {
        return fields_.at(cache_it->second);
    }

    ThrowInfo(ErrorCode::UnexpectedError,
              "No array field found in struct: {}",
              struct_name);
}

std::pair<bool, std::string>
Schema::WarmupPolicy(const FieldId& field_id,
                     bool is_vector,
                     bool is_index) const {
    // First check field-level warmup policy
    auto it = warmup_fields_.find(field_id);
    if (it != warmup_fields_.end()) {
        return {true, it->second};
    }

    // Fallback to appropriate collection-level config based on field type
    if (is_vector) {
        if (is_index) {
            return {warmup_vector_index_.has_value(),
                    warmup_vector_index_.value_or("")};
        }
        return {warmup_vector_field_.has_value(),
                warmup_vector_field_.value_or("")};
    }
    if (is_index) {
        return {warmup_scalar_index_.has_value(),
                warmup_scalar_index_.value_or("")};
    }
    return {warmup_scalar_field_.has_value(),
            warmup_scalar_field_.value_or("")};
}

}  // namespace milvus
