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

#include <google/protobuf/text_format.h>

#include <memory>

#include "pb/schema.pb.h"
#include "segcore/Collection.h"
#include "log/Log.h"

namespace milvus::segcore {

Collection::Collection(const milvus::proto::schema::CollectionSchema* schema) {
    Assert(schema != nullptr);
    collection_name_ = schema->name();
    schema_ = Schema::ParseFrom(*schema);
}

Collection::Collection(const std::string_view schema_proto) {
    milvus::proto::schema::CollectionSchema collection_schema;
    auto suc = google::protobuf::TextFormat::ParseFromString(
        std::string(schema_proto), &collection_schema);
    if (!suc) {
        LOG_SEGCORE_WARNING_ << "unmarshal schema string failed";
    }
    collection_name_ = collection_schema.name();
    schema_ = Schema::ParseFrom(collection_schema);
}

Collection::Collection(const void* schema_proto, const int64_t length) {
    Assert(schema_proto != nullptr);
    milvus::proto::schema::CollectionSchema collection_schema;
    auto suc = collection_schema.ParseFromArray(schema_proto, length);
    if (!suc) {
        LOG_SEGCORE_WARNING_ << "unmarshal schema string failed";
    }

    collection_name_ = collection_schema.name();
    schema_ = Schema::ParseFrom(collection_schema);
    LOG_SEGCORE_ERROR_ << "[remove me] collection schema fields num: "
                       << schema_->get_field_ids().size();
}

void
Collection::parseIndexMeta(const void* index_proto, const int64_t length) {
    Assert(index_proto != nullptr);

    milvus::proto::segcore::CollectionIndexMeta indexMeta;
    auto suc = indexMeta.ParseFromArray(index_proto, length);

    if (!suc) {
        LOG_SEGCORE_ERROR_ << "unmarshal index meta string failed";
        return;
    }

    index_meta_ = std::make_shared<CollectionIndexMeta>(indexMeta);
    LOG_SEGCORE_INFO_ << "index meta info : " << index_meta_->ToString();
}

}  // namespace milvus::segcore
