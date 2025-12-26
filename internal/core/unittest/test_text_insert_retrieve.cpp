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

#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "segcore/SegmentGrowingImpl.h"
#include "pb/schema.pb.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::segcore;

class TextInsertRetrieveTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        schema = std::make_shared<Schema>();

        // add primary key field (int64)
        auto pk_field_id = schema->AddDebugField("pk", DataType::INT64);
        schema->set_primary_field_id(pk_field_id);

        // add TEXT field
        text_field_id = schema->AddDebugField("text_field", DataType::TEXT);

        segment = CreateGrowingSegment(schema, empty_index_meta);
    }

    SchemaPtr schema;
    FieldId text_field_id;
    SegmentGrowingPtr segment;
};

// test TEXT field insert and retrieve with small text (< 64KB)
TEST_F(TextInsertRetrieveTest, SmallText) {
    const int N = 5;
    std::vector<std::string> expected_texts = {"hello world",
                                               "this is a test",
                                               "small text data",
                                               "milvus vector database",
                                               "TEXT field testing"};

    // prepare insert data
    auto insert_data = std::make_unique<proto::segcore::InsertRecord>();

    // add pk field
    auto pk_data = insert_data->add_fields_data();
    pk_data->set_field_id(100);  // pk field
    pk_data->set_type(proto::schema::DataType::Int64);
    auto pk_array = pk_data->mutable_scalars()->mutable_long_data();
    for (int i = 0; i < N; i++) {
        pk_array->add_data(i + 1);
    }

    // add TEXT field
    auto text_data = insert_data->add_fields_data();
    text_data->set_field_id(text_field_id.get());
    text_data->set_type(proto::schema::DataType::Text);
    auto string_array = text_data->mutable_scalars()->mutable_string_data();
    for (const auto& text : expected_texts) {
        string_array->add_data(text);
    }

    insert_data->set_num_rows(N);

    // prepare row_ids and timestamps
    std::vector<int64_t> row_ids(N);
    std::vector<Timestamp> timestamps(N);
    for (int i = 0; i < N; i++) {
        row_ids[i] = i;
        timestamps[i] = i + 1;
    }

    // insert
    auto offset = segment->PreInsert(N);
    ASSERT_EQ(offset, 0);
    segment->Insert(
        offset, N, row_ids.data(), timestamps.data(), insert_data.get());

    // retrieve
    std::vector<int64_t> offsets(N);
    for (int i = 0; i < N; i++) {
        offsets[i] = i;
    }

    auto result =
        segment->bulk_subscript(nullptr, text_field_id, offsets.data(), N);

    // verify
    ASSERT_TRUE(result != nullptr);
    ASSERT_TRUE(result->has_scalars());
    ASSERT_TRUE(result->scalars().has_string_data());

    const auto& retrieved_data = result->scalars().string_data().data();
    ASSERT_EQ(retrieved_data.size(), N);

    for (int i = 0; i < N; i++) {
        EXPECT_EQ(retrieved_data[i], expected_texts[i])
            << "Mismatch at index " << i;
    }
}

// test TEXT field insert and retrieve with medium text (64KB - 1MB)
TEST_F(TextInsertRetrieveTest, MediumText) {
    const int N = 3;
    std::vector<std::string> expected_texts;

    // generate medium-sized texts
    expected_texts.push_back(std::string(100 * 1024, 'a'));  // 100KB
    expected_texts.push_back(std::string(200 * 1024, 'b'));  // 200KB
    expected_texts.push_back(std::string(500 * 1024, 'c'));  // 500KB

    // prepare insert data
    auto insert_data = std::make_unique<proto::segcore::InsertRecord>();

    // add pk field
    auto pk_data = insert_data->add_fields_data();
    pk_data->set_field_id(100);
    pk_data->set_type(proto::schema::DataType::Int64);
    auto pk_array = pk_data->mutable_scalars()->mutable_long_data();
    for (int i = 0; i < N; i++) {
        pk_array->add_data(i + 1);
    }

    // add TEXT field
    auto text_data = insert_data->add_fields_data();
    text_data->set_field_id(text_field_id.get());
    text_data->set_type(proto::schema::DataType::Text);
    auto string_array = text_data->mutable_scalars()->mutable_string_data();
    for (const auto& text : expected_texts) {
        string_array->add_data(text);
    }

    insert_data->set_num_rows(N);

    // prepare row_ids and timestamps
    std::vector<int64_t> row_ids(N);
    std::vector<Timestamp> timestamps(N);
    for (int i = 0; i < N; i++) {
        row_ids[i] = i;
        timestamps[i] = i + 1;
    }

    // insert
    auto offset = segment->PreInsert(N);
    ASSERT_EQ(offset, 0);
    segment->Insert(
        offset, N, row_ids.data(), timestamps.data(), insert_data.get());

    // retrieve
    std::vector<int64_t> offsets(N);
    for (int i = 0; i < N; i++) {
        offsets[i] = i;
    }

    auto result =
        segment->bulk_subscript(nullptr, text_field_id, offsets.data(), N);

    // verify
    ASSERT_TRUE(result != nullptr);
    ASSERT_TRUE(result->has_scalars());
    ASSERT_TRUE(result->scalars().has_string_data());

    const auto& retrieved_data = result->scalars().string_data().data();
    ASSERT_EQ(retrieved_data.size(), N);

    for (int i = 0; i < N; i++) {
        EXPECT_EQ(retrieved_data[i].size(), expected_texts[i].size())
            << "Size mismatch at index " << i;
        EXPECT_EQ(retrieved_data[i], expected_texts[i])
            << "Content mismatch at index " << i;
    }
}

// test TEXT field insert and retrieve with mixed sizes
TEST_F(TextInsertRetrieveTest, MixedSizes) {
    const int N = 5;
    std::vector<std::string> expected_texts;

    expected_texts.push_back("small");                       // small
    expected_texts.push_back(std::string(100 * 1024, 'x'));  // 100KB
    expected_texts.push_back("tiny");                        // small
    expected_texts.push_back(std::string(300 * 1024, 'y'));  // 300KB
    expected_texts.push_back("short text");                  // small

    // prepare insert data
    auto insert_data = std::make_unique<proto::segcore::InsertRecord>();

    // add pk field
    auto pk_data = insert_data->add_fields_data();
    pk_data->set_field_id(100);
    pk_data->set_type(proto::schema::DataType::Int64);
    auto pk_array = pk_data->mutable_scalars()->mutable_long_data();
    for (int i = 0; i < N; i++) {
        pk_array->add_data(i + 1);
    }

    // add TEXT field
    auto text_data = insert_data->add_fields_data();
    text_data->set_field_id(text_field_id.get());
    text_data->set_type(proto::schema::DataType::Text);
    auto string_array = text_data->mutable_scalars()->mutable_string_data();
    for (const auto& text : expected_texts) {
        string_array->add_data(text);
    }

    insert_data->set_num_rows(N);

    // prepare row_ids and timestamps
    std::vector<int64_t> row_ids(N);
    std::vector<Timestamp> timestamps(N);
    for (int i = 0; i < N; i++) {
        row_ids[i] = i;
        timestamps[i] = i + 1;
    }

    // insert
    auto offset = segment->PreInsert(N);
    ASSERT_EQ(offset, 0);
    segment->Insert(
        offset, N, row_ids.data(), timestamps.data(), insert_data.get());

    // retrieve
    std::vector<int64_t> offsets(N);
    for (int i = 0; i < N; i++) {
        offsets[i] = i;
    }

    auto result =
        segment->bulk_subscript(nullptr, text_field_id, offsets.data(), N);

    // verify
    ASSERT_TRUE(result != nullptr);
    ASSERT_TRUE(result->has_scalars());
    ASSERT_TRUE(result->scalars().has_string_data());

    const auto& retrieved_data = result->scalars().string_data().data();
    ASSERT_EQ(retrieved_data.size(), N);

    for (int i = 0; i < N; i++) {
        EXPECT_EQ(retrieved_data[i].size(), expected_texts[i].size())
            << "Size mismatch at index " << i;
        EXPECT_EQ(retrieved_data[i], expected_texts[i])
            << "Content mismatch at index " << i;
    }
}

// test TEXT field with empty strings
TEST_F(TextInsertRetrieveTest, EmptyStrings) {
    const int N = 3;
    std::vector<std::string> expected_texts = {"", "non-empty", ""};

    // prepare insert data
    auto insert_data = std::make_unique<proto::segcore::InsertRecord>();

    // add pk field
    auto pk_data = insert_data->add_fields_data();
    pk_data->set_field_id(100);
    pk_data->set_type(proto::schema::DataType::Int64);
    auto pk_array = pk_data->mutable_scalars()->mutable_long_data();
    for (int i = 0; i < N; i++) {
        pk_array->add_data(i + 1);
    }

    // add TEXT field
    auto text_data = insert_data->add_fields_data();
    text_data->set_field_id(text_field_id.get());
    text_data->set_type(proto::schema::DataType::Text);
    auto string_array = text_data->mutable_scalars()->mutable_string_data();
    for (const auto& text : expected_texts) {
        string_array->add_data(text);
    }

    insert_data->set_num_rows(N);

    // prepare row_ids and timestamps
    std::vector<int64_t> row_ids(N);
    std::vector<Timestamp> timestamps(N);
    for (int i = 0; i < N; i++) {
        row_ids[i] = i;
        timestamps[i] = i + 1;
    }

    // insert
    auto offset = segment->PreInsert(N);
    ASSERT_EQ(offset, 0);
    segment->Insert(
        offset, N, row_ids.data(), timestamps.data(), insert_data.get());

    // retrieve
    std::vector<int64_t> offsets(N);
    for (int i = 0; i < N; i++) {
        offsets[i] = i;
    }

    auto result =
        segment->bulk_subscript(nullptr, text_field_id, offsets.data(), N);

    // verify
    ASSERT_TRUE(result != nullptr);
    ASSERT_TRUE(result->has_scalars());
    ASSERT_TRUE(result->scalars().has_string_data());

    const auto& retrieved_data = result->scalars().string_data().data();
    ASSERT_EQ(retrieved_data.size(), N);

    for (int i = 0; i < N; i++) {
        EXPECT_EQ(retrieved_data[i], expected_texts[i])
            << "Mismatch at index " << i;
    }
}
