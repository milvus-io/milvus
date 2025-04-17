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

#pragma once

#include <memory>
#include <string>

#include "common/Schema.h"
#include "common/IndexMeta.h"

namespace milvus::segcore {

class Collection {
 public:
    explicit Collection(const milvus::proto::schema::CollectionSchema* schema);
    explicit Collection(const std::string_view schema_proto);
    explicit Collection(const void* collection_proto, const int64_t length);

    void
    parseIndexMeta(const void* index_meta_proto_blob, const int64_t length);

    void
    parse_schema(const void* schema_proto_blob, const int64_t length);

 public:
    SchemaPtr&
    get_schema() {
        return schema_;
    }

    IndexMetaPtr&
    get_index_meta() {
        return index_meta_;
    }

    void
    set_index_meta(const IndexMetaPtr index_meta) {
        index_meta_ = index_meta;
    }

    const std::string_view
    get_collection_name() {
        return collection_name_;
    }

 private:
    std::string collection_name_;
    SchemaPtr schema_;
    IndexMetaPtr index_meta_;
};

using CollectionPtr = std::unique_ptr<Collection>;

}  // namespace milvus::segcore
