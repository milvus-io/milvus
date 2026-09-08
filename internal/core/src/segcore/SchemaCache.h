// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "common/Schema.h"

namespace milvus::segcore {

// Process-wide cache for immutable logical schemas. The returned SchemaPtr is
// the cache reference: copying it extends the entry lifetime, and destroying
// its final copy removes the entry. No weak pointer or external lease type is
// needed.
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
