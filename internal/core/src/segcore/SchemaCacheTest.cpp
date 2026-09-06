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

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <string>
#include <thread>
#include <vector>

#include "common/Consts.h"
#include "common/Schema.h"
#include "pb/schema.pb.h"
#include "segcore/Collection.h"
#include "segcore/SchemaCache.h"
#include "segcore/SegmentInterface.h"
#include "segcore/schema_handle.h"
#include "segcore/segment_c.h"

namespace milvus::segcore {
namespace {

proto::schema::CollectionSchema
SchemaProto(int64_t version) {
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

TEST(SchemaCacheTest, CollectionAndSchemaVersionFormTheOnlyKey) {
    SchemaCache cache;

    auto first = cache.GetOrCreate(1, SchemaProto(7));
    auto same_key_proto = SchemaProto(7);
    same_key_proto.set_name("same_key_reuses_first_schema");
    auto same_key = cache.GetOrCreate(1, same_key_proto);
    auto next_version = cache.GetOrCreate(1, SchemaProto(8));
    auto other_collection = cache.GetOrCreate(2, SchemaProto(7));

    EXPECT_EQ(first.get(), same_key.get());
    EXPECT_NE(first.get(), next_version.get());
    EXPECT_NE(first.get(), other_collection.get());
    EXPECT_EQ(first->get_schema_version(), 7);
    EXPECT_EQ(cache.EntryCountForTest(), 3);
}

TEST(SchemaCacheTest, ErasesEntryAfterFinalExternalReference) {
    SchemaCache cache;

    auto first = cache.GetOrCreate(1, SchemaProto(7));
    auto second_acquisition = cache.GetOrCreate(1, SchemaProto(7));
    auto copied_reference = first;
    ASSERT_EQ(cache.EntryCountForTest(), 1);

    first.reset();
    second_acquisition.reset();
    EXPECT_EQ(cache.EntryCountForTest(), 1);
    copied_reference.reset();
    EXPECT_EQ(cache.EntryCountForTest(), 0);
}

TEST(SchemaCacheTest, ConcurrentAcquisitionsReuseOneSchema) {
    constexpr size_t kThreadCount = 16;
    SchemaCache cache;
    std::vector<SchemaPtr> schemas(kThreadCount);
    std::vector<std::thread> threads;
    std::atomic<size_t> ready{0};
    std::atomic<bool> start{false};

    threads.reserve(kThreadCount);
    for (size_t i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&, i] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            schemas[i] = cache.GetOrCreate(10, SchemaProto(3));
        });
    }
    while (ready.load(std::memory_order_acquire) != kThreadCount) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_NE(schemas.front(), nullptr);
    for (const auto& schema : schemas) {
        EXPECT_EQ(schema.get(), schemas.front().get());
    }
    EXPECT_EQ(cache.EntryCountForTest(), 1);

    schemas.clear();
    EXPECT_EQ(cache.EntryCountForTest(), 0);
}

TEST(SchemaCacheCTest, HandleIsAThinSchemaPtrOwner) {
    auto& cache = GetGlobalSchemaCache();
    const auto initial_entries = cache.EntryCountForTest();
    auto schema_proto = SchemaProto(1);
    const auto blob = schema_proto.SerializeAsString();

    CSchemaHandle acquired = nullptr;
    auto status =
        AcquireSchemaHandle(2026090601, blob.data(), blob.size(), &acquired);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_NE(acquired, nullptr);
    EXPECT_EQ(BorrowSchemaPtrFromC(acquired)->get_schema_version(), 1);
    EXPECT_EQ(cache.EntryCountForTest(), initial_entries + 1);

    CSchemaHandle cloned = nullptr;
    status = CloneSchemaHandle(acquired, &cloned);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_NE(cloned, nullptr);
    EXPECT_EQ(BorrowSchemaPtrFromC(acquired).get(),
              BorrowSchemaPtrFromC(cloned).get());

    ReleaseSchemaHandle(acquired);
    EXPECT_EQ(cache.EntryCountForTest(), initial_entries + 1);
    ReleaseSchemaHandle(cloned);
    EXPECT_EQ(cache.EntryCountForTest(), initial_entries);
}

TEST(SchemaCacheCTest, LoadSchemaHandleIsNotCachedAndCarriesLoadPolicy) {
    auto& cache = GetGlobalSchemaCache();
    const auto initial_entries = cache.EntryCountForTest();
    auto schema_proto = SchemaProto(3);
    auto* vector = schema_proto.add_fields();
    vector->set_name("vector");
    vector->set_fieldid(101);
    vector->set_data_type(proto::schema::DataType::FloatVector);
    auto* dim = vector->add_type_params();
    dim->set_key("dim");
    dim->set_value("4");
    auto* mmap = schema_proto.add_properties();
    mmap->set_key(MMAP_ENABLED_KEY);
    mmap->set_value("true");
    const auto blob = schema_proto.SerializeAsString();
    const int64_t load_fields[] = {101};

    CSchemaHandle load_schema_handle = nullptr;
    auto status = AcquireLoadSchemaHandle(
        blob.data(), blob.size(), load_fields, 1, &load_schema_handle);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_NE(load_schema_handle, nullptr);
    EXPECT_EQ(cache.EntryCountForTest(), initial_entries);

    const auto& load_schema = BorrowSchemaPtrFromC(load_schema_handle);
    EXPECT_EQ(load_schema->get_schema_version(), 3);
    EXPECT_FALSE(load_schema->ShouldLoadField(FieldId(100)));
    EXPECT_TRUE(load_schema->ShouldLoadField(FieldId(101)));
    EXPECT_EQ(load_schema->MmapEnabled(FieldId(101)),
              std::make_pair(true, true));

    CSchemaHandle separately_parsed = nullptr;
    status = AcquireLoadSchemaHandle(
        blob.data(), blob.size(), load_fields, 1, &separately_parsed);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    EXPECT_NE(BorrowSchemaPtrFromC(load_schema_handle).get(),
              BorrowSchemaPtrFromC(separately_parsed).get());
    ReleaseSchemaHandle(separately_parsed);

    CSchemaHandle cloned = nullptr;
    status = CloneSchemaHandle(load_schema_handle, &cloned);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ReleaseSchemaHandle(load_schema_handle);
    EXPECT_TRUE(BorrowSchemaPtrFromC(cloned)->ShouldLoadField(FieldId(101)));
    ReleaseSchemaHandle(cloned);
    EXPECT_EQ(cache.EntryCountForTest(), initial_entries);
}

TEST(SchemaCacheCTest, CollectionSharesCachedSchemaAcrossUpdates) {
    auto& cache = GetGlobalSchemaCache();
    const auto initial_entries = cache.EntryCountForTest();

    auto first_proto = SchemaProto(4);
    const auto first_blob = first_proto.SerializeAsString();
    CSchemaHandle first_handle = nullptr;
    auto status = AcquireSchemaHandle(
        2026090604, first_blob.data(), first_blob.size(), &first_handle);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;

    CCollection collection = nullptr;
    status = NewCollectionWithSchema(first_handle, &collection);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_NE(collection, nullptr);
    EXPECT_EQ(static_cast<Collection*>(collection)->get_schema().get(),
              BorrowSchemaPtrFromC(first_handle).get());
    ReleaseSchemaHandle(first_handle);
    EXPECT_EQ(cache.EntryCountForTest(), initial_entries + 1);

    auto next_proto = SchemaProto(5);
    const auto next_blob = next_proto.SerializeAsString();
    CSchemaHandle next_handle = nullptr;
    status = AcquireSchemaHandle(
        2026090604, next_blob.data(), next_blob.size(), &next_handle);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    status = UpdateSchemaWithHandle(collection, next_handle);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    EXPECT_EQ(static_cast<Collection*>(collection)->get_schema().get(),
              BorrowSchemaPtrFromC(next_handle).get());
    ReleaseSchemaHandle(next_handle);
    EXPECT_EQ(cache.EntryCountForTest(), initial_entries + 1);

    DeleteCollection(collection);
    EXPECT_EQ(cache.EntryCountForTest(), initial_entries);
}

TEST(SchemaCacheCTest, SegmentPinsSchemaAfterCollectionAndHandleAreReleased) {
    auto& cache = GetGlobalSchemaCache();
    const auto initial_entries = cache.EntryCountForTest();
    auto schema_proto = SchemaProto(2);
    const auto blob = schema_proto.SerializeAsString();

    CSchemaHandle schema_handle = nullptr;
    auto status = AcquireSchemaHandle(
        2026090602, blob.data(), blob.size(), &schema_handle);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(cache.EntryCountForTest(), initial_entries + 1);

    CCollection collection = nullptr;
    status = NewCollectionWithSchema(schema_handle, &collection);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;

    CSchemaHandle load_schema_handle = nullptr;
    status = AcquireLoadSchemaHandle(
        blob.data(), blob.size(), nullptr, 0, &load_schema_handle);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    proto::segcore::SegmentLoadInfo load_info;
    load_info.set_collectionid(2026090602);
    load_info.set_partitionid(1);
    load_info.set_segmentid(1001);
    const auto load_info_blob = load_info.SerializeAsString();

    CSegmentInterface c_segment = nullptr;
    status = NewSegmentWithLoadInfoAndSchema(
        collection,
        schema_handle,
        load_schema_handle,
        Growing,
        load_info.segmentid(),
        &c_segment,
        false,
        reinterpret_cast<const uint8_t*>(load_info_blob.data()),
        load_info_blob.size());
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_NE(c_segment, nullptr);

    ReleaseSchemaHandle(load_schema_handle);
    ReleaseSchemaHandle(schema_handle);
    DeleteCollection(collection);
    auto* segment = static_cast<SegmentInterface*>(c_segment);
    EXPECT_EQ(segment->get_row_count(), 0);
    EXPECT_EQ(cache.EntryCountForTest(), initial_entries + 1);

    auto schema = segment->get_schema_snapshot();
    ASSERT_NE(schema, nullptr);
    EXPECT_EQ(schema->get_schema_version(), 2);
    DeleteSegment(c_segment);
    EXPECT_EQ(cache.EntryCountForTest(), initial_entries + 1);

    schema.reset();
    EXPECT_EQ(cache.EntryCountForTest(), initial_entries);
}

TEST(SchemaCacheCTest, SchemaAwareGrowingSegmentKeepsLoadInfo) {
    auto schema_proto = SchemaProto(6);
    const auto schema_blob = schema_proto.SerializeAsString();

    CSchemaHandle schema_handle = nullptr;
    auto status = AcquireSchemaHandle(
        2026090606, schema_blob.data(), schema_blob.size(), &schema_handle);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    CSchemaHandle load_schema_handle = nullptr;
    status = AcquireLoadSchemaHandle(schema_blob.data(),
                                     schema_blob.size(),
                                     nullptr,
                                     0,
                                     &load_schema_handle);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;

    CCollection collection = nullptr;
    status = NewCollectionWithSchema(schema_handle, &collection);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;

    proto::segcore::SegmentLoadInfo load_info;
    load_info.set_collectionid(2026090606);
    load_info.set_partitionid(1);
    load_info.set_segmentid(1002);
    load_info.set_storageversion(STORAGE_V3);
    load_info.set_manifest_path("/schema_cache_test/nonexistent_manifest.json");
    const auto load_info_blob = load_info.SerializeAsString();

    CSegmentInterface c_segment = nullptr;
    status = NewSegmentWithLoadInfoAndSchema(
        collection,
        schema_handle,
        load_schema_handle,
        Growing,
        load_info.segmentid(),
        &c_segment,
        false,
        reinterpret_cast<const uint8_t*>(load_info_blob.data()),
        load_info_blob.size());
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_NE(c_segment, nullptr);

    milvus::tracer::TraceContext trace_context;
    auto* segment = static_cast<SegmentInterface*>(c_segment);
    // An empty load info returns immediately. Reaching the missing manifest is
    // observable proof that the schema-aware wrapper retained the proto.
    EXPECT_ANY_THROW(segment->Load(trace_context, nullptr));

    DeleteSegment(c_segment);
    DeleteCollection(collection);
    ReleaseSchemaHandle(load_schema_handle);
    ReleaseSchemaHandle(schema_handle);
}

}  // namespace
}  // namespace milvus::segcore
