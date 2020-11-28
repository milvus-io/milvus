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

namespace milvus {
std::shared_ptr<Schema>
Schema::ParseFrom(const milvus::proto::schema::CollectionSchema& schema_proto) {
    auto schema = std::make_shared<Schema>();
    schema->set_auto_id(schema_proto.autoid());
    for (const milvus::proto::schema::FieldSchema& child : schema_proto.fields()) {
        const auto& type_params = child.type_params();
        int64_t dim = -1;
        auto data_type = DataType(child.data_type());
        for (const auto& type_param : type_params) {
            if (type_param.key() == "dim") {
                dim = strtoll(type_param.value().c_str(), nullptr, 10);
            }
        }

        if (field_is_vector(data_type)) {
            AssertInfo(dim != -1, "dim not found");
        } else {
            AssertInfo(dim == 1 || dim == -1, "Invalid dim field. Should be 1 or not exists");
            dim = 1;
        }

        if (child.is_primary_key()) {
            AssertInfo(!schema->primary_key_offset_opt_.has_value(), "repetitive primary key");
            schema->primary_key_offset_opt_ = schema->size();
        }

        schema->AddField(child.name(), data_type, dim);
    }
    return schema;
}
}  // namespace milvus
