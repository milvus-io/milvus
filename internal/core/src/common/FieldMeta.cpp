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

#include "common/FieldMeta.h"
#include "common/SystemProperty.h"
#include "common/protobuf_utils.h"
#include "common/Common.h"
#include <boost/lexical_cast.hpp>

#include "Consts.h"

namespace milvus {
TokenizerParams
ParseTokenizerParams(const TypeParams& params) {
    auto iter = params.find("analyzer_params");
    if (iter == params.end()) {
        return "{}";
    }
    return iter->second;
}

bool
FieldMeta::enable_match() const {
    if (!IsStringDataType(type_)) {
        return false;
    }
    if (!string_info_.has_value()) {
        return false;
    }
    return string_info_->enable_match;
}

bool
FieldMeta::enable_growing_jsonStats() const {
    return IsJsonDataType(type_) && GROWING_JSON_KEY_STATS_ENABLED;
}

bool
FieldMeta::enable_analyzer() const {
    if (!IsStringDataType(type_)) {
        return false;
    }
    if (!string_info_.has_value()) {
        return false;
    }
    return string_info_->enable_analyzer;
}

TokenizerParams
FieldMeta::get_analyzer_params() const {
    if (!enable_analyzer()) {
        PanicInfo(
            Unsupported,
            fmt::format("unsupported text index when not enable analyzer"));
    }
    auto params = string_info_->params;
    return ParseTokenizerParams(params);
}

FieldMeta
FieldMeta::ParseFrom(const milvus::proto::schema::FieldSchema& schema_proto) {
    auto field_id = FieldId(schema_proto.fieldid());
    auto name = FieldName(schema_proto.name());
    auto nullable = schema_proto.nullable();
    if (field_id.get() < 100) {
        // system field id
        auto is_system =
            SystemProperty::Instance().SystemFieldVerify(name, field_id);
        AssertInfo(is_system,
                   "invalid system type: name(" + name.get() + "), id(" +
                       std::to_string(field_id.get()) + ")");
    }

    auto data_type = DataType(schema_proto.data_type());

    if (IsVectorDataType(data_type)) {
        auto type_map = RepeatedKeyValToMap(schema_proto.type_params());
        auto index_map = RepeatedKeyValToMap(schema_proto.index_params());

        int64_t dim = 0;
        if (!IsSparseFloatVectorDataType(data_type)) {
            AssertInfo(type_map.count("dim"), "dim not found");
            dim = boost::lexical_cast<int64_t>(type_map.at("dim"));
        }
        if (!index_map.count("metric_type")) {
            return FieldMeta{
                name, field_id, data_type, dim, std::nullopt, false};
        }
        auto metric_type = index_map.at("metric_type");
        return FieldMeta{name, field_id, data_type, dim, metric_type, false};
    }

    if (IsStringDataType(data_type)) {
        auto type_map = RepeatedKeyValToMap(schema_proto.type_params());
        AssertInfo(type_map.count(MAX_LENGTH), "max_length not found");
        auto max_len = boost::lexical_cast<int64_t>(type_map.at(MAX_LENGTH));

        auto get_bool_value = [&](const std::string& key) -> bool {
            if (!type_map.count(key)) {
                return false;
            }
            auto param_str = type_map.at(key);
            std::transform(param_str.begin(),
                           param_str.end(),
                           param_str.begin(),
                           ::tolower);
            std::istringstream ss(param_str);
            bool b;
            ss >> std::boolalpha >> b;
            return b;
        };

        bool enable_analyzer = get_bool_value("enable_analyzer");
        bool enable_match = get_bool_value("enable_match");

        return FieldMeta{name,
                         field_id,
                         data_type,
                         max_len,
                         nullable,
                         enable_match,
                         enable_analyzer,
                         type_map};
    }

    if (IsArrayDataType(data_type)) {
        return FieldMeta{name,
                         field_id,
                         data_type,
                         DataType(schema_proto.element_type()),
                         nullable};
    }

    return FieldMeta{name, field_id, data_type, nullable};
}

}  // namespace milvus
