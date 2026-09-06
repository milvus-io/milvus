// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "common/Schema.h"

namespace milvus::segcore {

// Process-wide cache for immutable collection schemas. A schema is identified
// only by (collection_id, CollectionSchema.version). The returned SchemaPtr is
// the ownership token: copies keep the entry alive, and dropping the final
// external copy removes the entry from the cache.
class SchemaCache {
 public:
    SchemaCache();
    ~SchemaCache();

    SchemaCache(const SchemaCache&) = delete;
    SchemaCache&
    operator=(const SchemaCache&) = delete;

    [[nodiscard]] SchemaPtr
    GetOrCreate(int64_t collection_id,
                const milvus::proto::schema::CollectionSchema& schema_proto);

    [[nodiscard]] size_t
    EntryCountForTest() const;

 private:
    struct State;
    std::shared_ptr<State> state_;
};

SchemaCache&
GetGlobalSchemaCache();

}  // namespace milvus::segcore
