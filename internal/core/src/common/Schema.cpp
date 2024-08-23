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

#include <optional>
#include <string>
#include <boost/lexical_cast.hpp>
#include <google/protobuf/text_format.h>

#include "Schema.h"
#include "SystemProperty.h"
#include "protobuf_utils.h"

namespace milvus {

using std::string;

std::shared_ptr<Schema>
Schema::ParseFrom(const milvus::proto::schema::CollectionSchema& schema_proto) {
    auto schema = std::make_shared<Schema>();
    // schema->set_auto_id(schema_proto.autoid());

    // NOTE: only two system

    for (const milvus::proto::schema::FieldSchema& child :
         schema_proto.fields()) {
        auto field_id = FieldId(child.fieldid());
        auto name = FieldName(child.name());
        auto nullable = child.nullable();
        if (field_id.get() < 100) {
            // system field id
            auto is_system =
                SystemProperty::Instance().SystemFieldVerify(name, field_id);
            AssertInfo(is_system,
                       "invalid system type: name(" + name.get() + "), id(" +
                           std::to_string(field_id.get()) + ")");
        }

        auto data_type = DataType(child.data_type());

        if (IsVectorDataType(data_type)) {
            auto type_map = RepeatedKeyValToMap(child.type_params());
            auto index_map = RepeatedKeyValToMap(child.index_params());

            int64_t dim = 0;
            if (!IsSparseFloatVectorDataType(data_type)) {
                AssertInfo(type_map.count("dim"), "dim not found");
                dim = boost::lexical_cast<int64_t>(type_map.at("dim"));
            }
            if (!index_map.count("metric_type")) {
                schema->AddField(
                    name, field_id, data_type, dim, std::nullopt, false);
            } else {
                auto metric_type = index_map.at("metric_type");
                schema->AddField(
                    name, field_id, data_type, dim, metric_type, false);
            }
        } else if (IsStringDataType(data_type)) {
            auto type_map = RepeatedKeyValToMap(child.type_params());
            AssertInfo(type_map.count(MAX_LENGTH), "max_length not found");
            auto max_len =
                boost::lexical_cast<int64_t>(type_map.at(MAX_LENGTH));
            schema->AddField(name, field_id, data_type, max_len, nullable);
        } else if (IsArrayDataType(data_type)) {
            schema->AddField(name,
                             field_id,
                             data_type,
                             DataType(child.element_type()),
                             nullable);
        } else {
            schema->AddField(name, field_id, data_type, nullable);
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
    }

    AssertInfo(schema->get_primary_field_id().has_value(),
               "primary key should be specified");

    return schema;
}

const FieldMeta FieldMeta::RowIdMeta(FieldName("RowID"),
                                     RowFieldID,
                                     DataType::INT64,
                                     false);

}  // namespace milvus
