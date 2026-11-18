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

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "segcore/Collection.h"
#include "segcore/collection_c.h"

namespace milvus::segcore {
namespace {

proto::schema::CollectionSchema
MakeSchema(int32_t version) {
    proto::schema::CollectionSchema schema;
    schema.set_name("schema_load_fields");
    schema.set_version(version);
    for (int64_t id : {100, 101, 102}) {
        auto field = schema.add_fields();
        field->set_fieldid(id);
        field->set_name("field_" + std::to_string(id));
        field->set_data_type(proto::schema::Int64);
        field->set_is_primary_key(id == 100);
    }
    return schema;
}

CStatus
ApplySchema(Collection& collection,
            const proto::schema::CollectionSchema& schema,
            const std::vector<int64_t>& load_fields) {
    const auto blob = schema.SerializeAsString();
    return UpdateSchema(&collection,
                        blob.data(),
                        blob.size(),
                        load_fields.data(),
                        load_fields.size());
}

}  // namespace

TEST(CollectionSchemaUpdate, PublishesSchemaAndHintsTogether) {
    auto schema = MakeSchema(7);
    Collection collection(&schema);
    auto original = collection.get_schema();
    original->UpdateLoadFields({100, 101});
    ASSERT_EQ(collection.get_schema_version(), 7);

    schema.set_version(8);
    auto added = schema.add_fields();
    added->set_fieldid(200);
    added->set_name("added");
    added->set_data_type(proto::schema::Int64);
    added->set_nullable(true);
    auto status = ApplySchema(collection, schema, {100, 101, 200});
    ASSERT_EQ(status.error_code, Success);
    auto current = collection.get_schema();
    ASSERT_NE(current, original);
    EXPECT_EQ(current->get_schema_version(), 8);
    EXPECT_EQ((*current)[FieldId(200)].get_name().get(), "added");
    EXPECT_TRUE(current->ShouldLoadField(FieldId(200)));
    EXPECT_FALSE(current->ShouldLoadField(FieldId(102)));
    EXPECT_FALSE(original->ShouldLoadField(FieldId(200)));
    EXPECT_FALSE(original->ShouldLoadField(FieldId(102)));

    // Equal and older versions must not rewrite an already published snapshot.
    for (auto version : {8, 7}) {
        schema.set_version(version);
        status = ApplySchema(collection, schema, {100, 102});
        ASSERT_EQ(status.error_code, Success);
        EXPECT_EQ(collection.get_schema(), current);
        EXPECT_TRUE(current->ShouldLoadField(FieldId(200)));
        EXPECT_FALSE(current->ShouldLoadField(FieldId(102)));
    }

    // An empty selection retains the native all-fields meaning.
    schema.set_version(9);
    status = ApplySchema(collection, schema, {});
    ASSERT_EQ(status.error_code, Success);
    EXPECT_TRUE(collection.get_schema()->ShouldLoadField(FieldId(102)));
    EXPECT_FALSE(current->ShouldLoadField(FieldId(102)));
}

TEST(CollectionSchemaUpdate, FailedUpdateCanRetrySameVersion) {
    auto schema = MakeSchema(7);
    Collection collection(&schema);
    auto original = collection.get_schema();
    original->UpdateLoadFields({100, 101});

    schema.set_version(8);
    auto invalid = schema;
    *invalid.add_fields() = schema.fields(0);  // Duplicate field fails parsing.
    auto status = ApplySchema(collection, invalid, {100, 102});
    ASSERT_NE(status.error_code, Success);
    free(const_cast<char*>(status.error_msg));
    EXPECT_EQ(collection.get_schema(), original);
    EXPECT_EQ(collection.get_schema_version(), 7);
    EXPECT_TRUE(original->ShouldLoadField(FieldId(101)));
    EXPECT_FALSE(original->ShouldLoadField(FieldId(102)));

    status = ApplySchema(collection, schema, {100, 102});
    ASSERT_EQ(status.error_code, Success);
    EXPECT_EQ(collection.get_schema_version(), 8);
    EXPECT_FALSE(collection.get_schema()->ShouldLoadField(FieldId(101)));
    EXPECT_TRUE(collection.get_schema()->ShouldLoadField(FieldId(102)));
    EXPECT_TRUE(original->ShouldLoadField(FieldId(101)));
}

TEST(CollectionSchemaUpdate, ConcurrentReadersKeepCompleteSnapshots) {
    auto schema = MakeSchema(0);
    Collection collection(&schema);
    auto original = collection.get_schema();
    original->UpdateLoadFields({100, 102});
    std::atomic<bool> done{false};
    std::atomic<int> ready{0};
    std::atomic<int> inconsistent{0};
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&] {
            ++ready;
            do {
                auto snapshot = collection.get_schema();
                const bool odd = snapshot->get_schema_version() % 2 != 0;
                // Read outside Collection's lock, just as segment loading does.
                if (snapshot->ShouldLoadField(FieldId(101)) != odd ||
                    snapshot->ShouldLoadField(FieldId(102)) == odd ||
                    !original->ShouldLoadField(FieldId(102)) ||
                    original->ShouldLoadField(FieldId(101))) {
                    ++inconsistent;
                }
            } while (!done.load());
        });
    }
    while (ready.load() != 4) {
        std::this_thread::yield();
    }
    for (int32_t version = 1; version <= 1000; ++version) {
        schema.set_version(version);
        const int64_t selected = version % 2 != 0 ? 101 : 102;
        auto status = ApplySchema(collection, schema, {100, selected});
        EXPECT_EQ(status.error_code, Success);
        if (status.error_code != Success) {
            free(const_cast<char*>(status.error_msg));
            break;
        }
    }
    done.store(true);
    for (auto& reader : readers) {
        reader.join();
    }
    EXPECT_EQ(inconsistent.load(), 0);
}

}  // namespace milvus::segcore
