// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

#include <gtest/gtest.h>

#include "common/Schema.h"
#include "pb/schema.pb.h"
#include "segcore/SchemaCache.h"

namespace milvus::segcore {
namespace {

proto::schema::CollectionSchema
SchemaProto(int32_t version) {
    proto::schema::CollectionSchema schema;
    schema.set_name("schema_cache_test");
    schema.set_version(version);
    auto* primary = schema.add_fields();
    primary->set_name("id");
    primary->set_fieldid(100);
    primary->set_data_type(proto::schema::DataType::Int64);
    primary->set_is_primary_key(true);
    return schema;
}

TEST(SchemaCacheTest, ReusesKeyAndErasesAfterLastReference) {
    SchemaCache cache;

    auto first = cache.GetOrCreate(1, SchemaProto(7));
    auto second_proto = SchemaProto(7);
    second_proto.set_name("same_key_is_same_schema");
    auto second = cache.GetOrCreate(1, second_proto);

    EXPECT_EQ(first.get(), second.get());
    EXPECT_EQ(first->get_schema_version(), 7);
    EXPECT_EQ(cache.EntryCountForTest(), 1);

    auto segment_reference = first;
    first.reset();
    EXPECT_EQ(cache.EntryCountForTest(), 1);
    second.reset();
    EXPECT_EQ(cache.EntryCountForTest(), 1);
    segment_reference.reset();
    EXPECT_EQ(cache.EntryCountForTest(), 0);
}

TEST(SchemaCacheTest, CollectionAndVersionFormTheKey) {
    SchemaCache cache;
    auto collection_one_v1 = cache.GetOrCreate(1, SchemaProto(1));
    auto collection_one_v2 = cache.GetOrCreate(1, SchemaProto(2));
    auto collection_two_v1 = cache.GetOrCreate(2, SchemaProto(1));

    EXPECT_NE(collection_one_v1.get(), collection_one_v2.get());
    EXPECT_NE(collection_one_v1.get(), collection_two_v1.get());
    EXPECT_EQ(cache.EntryCountForTest(), 3);
}

TEST(SchemaCacheTest, RuntimeRevisionSeparatesSameLogicalVersion) {
    SchemaCache cache;
    auto schema = SchemaProto(7);
    auto first = cache.GetOrCreate(1, 100, schema);
    auto second = cache.GetOrCreate(1, 101, schema);

    EXPECT_NE(first.get(), second.get());
    EXPECT_EQ(first->get_schema_version(), 100);
    EXPECT_EQ(second->get_schema_version(), 101);
    EXPECT_EQ(cache.EntryCountForTest(), 2);
}

}  // namespace
}  // namespace milvus::segcore
