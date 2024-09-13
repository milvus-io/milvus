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
