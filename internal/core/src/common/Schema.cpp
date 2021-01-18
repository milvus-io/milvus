// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "common/Schema.h"
#include <google/protobuf/text_format.h>
#include <boost/lexical_cast.hpp>
#include "common/SystemProperty.h"

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
    schema->set_auto_id(schema_proto.autoid());

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

        if (child.is_primary_key()) {
            AssertInfo(!schema->primary_key_offset_opt_.has_value(), "repetitive primary key");
            schema->primary_key_offset_opt_ = field_offset;
        }

        if (datatype_is_vector(data_type)) {
            auto type_map = RepeatedKeyValToMap(child.type_params());
            auto index_map = RepeatedKeyValToMap(child.index_params());
            if (!index_map.count("metric_type")) {
                auto default_metric_type =
                    data_type == DataType::VECTOR_FLOAT ? MetricType::METRIC_L2 : MetricType::METRIC_Jaccard;
                index_map["metric_type"] = default_metric_type;
            }

            AssertInfo(type_map.count("dim"), "dim not found");
            auto dim = boost::lexical_cast<int64_t>(type_map.at("dim"));
            AssertInfo(index_map.count("metric_type"), "index not found");
            auto metric_type = GetMetricType(index_map.at("metric_type"));
            schema->AddField(name, field_id, data_type, dim, metric_type);
        } else {
            schema->AddField(name, field_id, data_type);
        }
    }
    return schema;
}
}  // namespace milvus
