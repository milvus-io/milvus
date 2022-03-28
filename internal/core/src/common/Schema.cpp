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

namespace milvus {

using std::string;
static std::map<string, string>
RepeatedKeyValToMap(const google::protobuf::RepeatedPtrField<proto::common::KeyValuePair>& kvs) {
    std::map<string, string> mapping;
    for (auto& kv : kvs) {
        AssertInfo(!mapping.count(kv.key()), "repeat key(" + kv.key() + ") in protobuf");
        mapping.emplace(kv.key(), kv.value());
    }
    return mapping;
}

std::shared_ptr<Schema>
Schema::ParseFrom(const milvus::proto::schema::CollectionSchema& schema_proto) {
    auto schema = std::make_shared<Schema>();
    // schema->set_auto_id(schema_proto.autoid());

    // NOTE: only two system

    for (const milvus::proto::schema::FieldSchema& child : schema_proto.fields()) {
        auto field_offset = FieldOffset(schema->size());
        auto field_id = FieldId(child.fieldid());
        auto name = FieldName(child.name());

        if (field_id.get() < 100) {
            // system field id
            auto is_system = SystemProperty::Instance().SystemFieldVerify(name, field_id);
            AssertInfo(is_system,
                       "invalid system type: name(" + name.get() + "), id(" + std::to_string(field_id.get()) + ")");
            continue;
        }

        auto data_type = DataType(child.data_type());

        if (datatype_is_vector(data_type)) {
            auto type_map = RepeatedKeyValToMap(child.type_params());
            auto index_map = RepeatedKeyValToMap(child.index_params());

            AssertInfo(type_map.count("dim"), "dim not found");
            auto dim = boost::lexical_cast<int64_t>(type_map.at("dim"));
            if (!index_map.count("metric_type")) {
                schema->AddField(name, field_id, data_type, dim, std::nullopt);
            } else {
                auto metric_type = GetMetricType(index_map.at("metric_type"));
                schema->AddField(name, field_id, data_type, dim, metric_type);
            }
        } else {
            schema->AddField(name, field_id, data_type);
        }

        if (child.is_primary_key()) {
            AssertInfo(!schema->get_primary_key_offset().has_value(), "repetitive primary key");
            Assert(!schema_proto.autoid());
            schema->set_primary_key(field_offset);
        }
    }
    if (schema->get_is_auto_id()) {
        AssertInfo(!schema->get_primary_key_offset().has_value(), "auto id mode: shouldn't have primary key");
    } else {
        AssertInfo(schema->get_primary_key_offset().has_value(), "primary key should be specified when autoId is off");
    }

    return schema;
}

const FieldMeta FieldMeta::RowIdMeta(FieldName("RowID"), FieldId(0), DataType::INT64);

}  // namespace milvus
