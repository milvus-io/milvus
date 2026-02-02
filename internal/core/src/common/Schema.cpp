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
#include <cstddef>
#include <optional>
#include <string>
#include "arrow/type.h"
#include <boost/lexical_cast.hpp>
#include <google/protobuf/text_format.h>
#include <memory>

#include "Schema.h"
#include "SystemProperty.h"
#include "arrow/util/key_value_metadata.h"
#include "common/Consts.h"
#include "milvus-storage/common/constants.h"
#include "pb/common.pb.h"
#include "protobuf_utils.h"

namespace milvus {

using std::string;

std::shared_ptr<Schema>
Schema::ParseFrom(const milvus::proto::schema::CollectionSchema& schema_proto) {
    auto schema = std::make_shared<Schema>();
    // schema->set_auto_id(schema_proto.autoid());

    // NOTE: only two system

    auto process_field = [&schema, &schema_proto](const auto& child) {
        auto field_id = FieldId(child.fieldid());

        auto f = FieldMeta::ParseFrom(child);
        schema->AddField(std::move(f));

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

        auto [has_setting, enabled] =
            GetBoolFromRepeatedKVs(child.type_params(), MMAP_ENABLED_KEY);
        if (has_setting) {
            schema->mmap_fields_[field_id] = enabled;
        }

        // Parse warmup policy for the field (key: "warmup")
        auto warmup_policy =
            GetStringFromRepeatedKVs(child.type_params(), WARMUP_KEY);
        if (warmup_policy.has_value()) {
            schema->warmup_fields_[field_id] = warmup_policy.value();
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

    std::tie(schema->has_mmap_setting_, schema->mmap_enabled_) =
        GetBoolFromRepeatedKVs(schema_proto.properties(), MMAP_ENABLED_KEY);

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

    return schema;
}

const FieldMeta FieldMeta::RowIdMeta(
    FieldName("RowID"), RowFieldID, DataType::INT64, false, std::nullopt);

const ArrowSchemaPtr
Schema::ConvertToArrowSchema() const {
    arrow::FieldVector arrow_fields;
    for (auto& field : fields_) {
        auto meta = field.second;
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
    for (const auto& [field_id, field_meta] : fields_) {
        auto it = old_schema.fields_.find(field_id);
        if (it == old_schema.fields_.end()) {
            result.emplace_back(field_meta);
        }
    }

    return std::make_unique<std::vector<FieldMeta>>(result);
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
