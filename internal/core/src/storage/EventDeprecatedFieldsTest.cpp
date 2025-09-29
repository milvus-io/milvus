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
#include <memory>
#include <vector>
#include <cstring>

#include "storage/Event.h"
#include "storage/BinlogReader.h"
#include "common/Types.h"

using namespace milvus::storage;

// Tests for deprecated field handling in Event.cpp
class EventDeprecatedFieldsTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Create a simple test buffer
        test_buffer_size_ = 1024;
        test_buffer_ =
            std::shared_ptr<uint8_t[]>(new uint8_t[test_buffer_size_]);
        memset(test_buffer_.get(), 0, test_buffer_size_);
    }

    std::shared_ptr<uint8_t[]> test_buffer_;
    size_t test_buffer_size_;
};

TEST_F(EventDeprecatedFieldsTest, DescriptorEventDataFixPartDefaultValues) {
    // Test that DescriptorEventDataFixPart has correct default values for deprecated fields
    DescriptorEventDataFixPart fix_part;

    // All deprecated fields should default to DEPRECATED_ID_VALUE (-1)
    ASSERT_EQ(fix_part.collection_id, DEPRECATED_ID_VALUE);
    ASSERT_EQ(fix_part.partition_id, DEPRECATED_ID_VALUE);
    ASSERT_EQ(fix_part.segment_id, DEPRECATED_ID_VALUE);

    // Other fields should have their expected defaults
    ASSERT_EQ(fix_part.field_id, 0);
    ASSERT_EQ(fix_part.start_timestamp, 0);
    ASSERT_EQ(fix_part.end_timestamp, 0);
    ASSERT_EQ(fix_part.data_type, milvus::proto::schema::DataType::None);
}

TEST_F(EventDeprecatedFieldsTest, DescriptorEventDataFixPartConstantValue) {
    // Test that DEPRECATED_ID_VALUE constant has expected value
    ASSERT_EQ(DEPRECATED_ID_VALUE, -1);
}

TEST_F(EventDeprecatedFieldsTest, DescriptorEventDataFixPartSerialization) {
    // Test that deprecated fields are serialized as DEPRECATED_ID_VALUE regardless of their actual values
    DescriptorEventDataFixPart fix_part;

    // Set up test data with different values
    fix_part.field_id = 12345;
    fix_part.start_timestamp = 100;
    fix_part.end_timestamp = 200;
    fix_part.data_type = milvus::proto::schema::DataType::Int64;

    // Serialize the fix part
    auto serialized = fix_part.Serialize();

    // Verify the serialized size
    size_t expected_size =
        sizeof(int64_t) * 6 + sizeof(milvus::proto::schema::DataType);
    ASSERT_EQ(serialized.size(), expected_size);

    // Parse the serialized data to verify deprecated fields are written as -1
    size_t offset = 0;

    // Check collection_id (should be -1)
    int64_t collection_id;
    memcpy(&collection_id, serialized.data() + offset, sizeof(int64_t));
    ASSERT_EQ(collection_id, DEPRECATED_ID_VALUE);
    offset += sizeof(int64_t);

    // Check partition_id (should be -1)
    int64_t partition_id;
    memcpy(&partition_id, serialized.data() + offset, sizeof(int64_t));
    ASSERT_EQ(partition_id, DEPRECATED_ID_VALUE);
    offset += sizeof(int64_t);

    // Check segment_id (should be -1)
    int64_t segment_id;
    memcpy(&segment_id, serialized.data() + offset, sizeof(int64_t));
    ASSERT_EQ(segment_id, DEPRECATED_ID_VALUE);
    offset += sizeof(int64_t);

    // Check field_id (should preserve original value)
    int64_t field_id;
    memcpy(&field_id, serialized.data() + offset, sizeof(int64_t));
    ASSERT_EQ(field_id, 12345);
    offset += sizeof(int64_t);

    // Check start_timestamp (should preserve original value)
    milvus::Timestamp start_ts;
    memcpy(&start_ts, serialized.data() + offset, sizeof(milvus::Timestamp));
    ASSERT_EQ(start_ts, 100);
    offset += sizeof(milvus::Timestamp);

    // Check end_timestamp (should preserve original value)
    milvus::Timestamp end_ts;
    memcpy(&end_ts, serialized.data() + offset, sizeof(milvus::Timestamp));
    ASSERT_EQ(end_ts, 200);
    offset += sizeof(milvus::Timestamp);

    // Check data_type (should preserve original value)
    milvus::proto::schema::DataType data_type;
    memcpy(&data_type,
           serialized.data() + offset,
           sizeof(milvus::proto::schema::DataType));
    ASSERT_EQ(data_type, milvus::proto::schema::DataType::Int64);
}

TEST_F(EventDeprecatedFieldsTest, DescriptorEventDataFixPartDeserialization) {
    // Test that deserialization forces deprecated fields to DEPRECATED_ID_VALUE

    // Create a binary buffer with non-deprecated values in the deprecated fields
    std::vector<uint8_t> test_data;

    // Write non-deprecated values for collection_id, partition_id, segment_id
    int64_t non_deprecated_collection = 1000;
    int64_t non_deprecated_partition = 2000;
    int64_t non_deprecated_segment = 3000;
    int64_t field_id = 12345;
    milvus::Timestamp start_ts = 100;
    milvus::Timestamp end_ts = 200;
    milvus::proto::schema::DataType data_type =
        milvus::proto::schema::DataType::Int32;

    test_data.resize(sizeof(int64_t) * 6 +
                     sizeof(milvus::proto::schema::DataType));
    size_t offset = 0;

    memcpy(
        test_data.data() + offset, &non_deprecated_collection, sizeof(int64_t));
    offset += sizeof(int64_t);
    memcpy(
        test_data.data() + offset, &non_deprecated_partition, sizeof(int64_t));
    offset += sizeof(int64_t);
    memcpy(test_data.data() + offset, &non_deprecated_segment, sizeof(int64_t));
    offset += sizeof(int64_t);
    memcpy(test_data.data() + offset, &field_id, sizeof(int64_t));
    offset += sizeof(int64_t);
    memcpy(test_data.data() + offset, &start_ts, sizeof(milvus::Timestamp));
    offset += sizeof(milvus::Timestamp);
    memcpy(test_data.data() + offset, &end_ts, sizeof(milvus::Timestamp));
    offset += sizeof(milvus::Timestamp);
    memcpy(test_data.data() + offset,
           &data_type,
           sizeof(milvus::proto::schema::DataType));

    // Create a BinlogReader from the test data
    std::shared_ptr<uint8_t[]> shared_data(new uint8_t[test_data.size()]);
    memcpy(shared_data.get(), test_data.data(), test_data.size());

    auto reader = std::make_shared<BinlogReader>(shared_data, test_data.size());

    // Deserialize using the constructor
    DescriptorEventDataFixPart fix_part(reader);

    // Verify that deprecated fields are forced to DEPRECATED_ID_VALUE
    ASSERT_EQ(fix_part.collection_id, DEPRECATED_ID_VALUE);
    ASSERT_EQ(fix_part.partition_id, DEPRECATED_ID_VALUE);
    ASSERT_EQ(fix_part.segment_id, DEPRECATED_ID_VALUE);

    // Verify that non-deprecated fields preserve their values
    ASSERT_EQ(fix_part.field_id, field_id);
    ASSERT_EQ(fix_part.start_timestamp, start_ts);
    ASSERT_EQ(fix_part.end_timestamp, end_ts);
    ASSERT_EQ(fix_part.data_type, data_type);
}

TEST_F(EventDeprecatedFieldsTest,
       DescriptorEventDataFixPartBinaryCompatibility) {
    // Test that the binary format is maintained for compatibility
    DescriptorEventDataFixPart fix_part1;
    fix_part1.field_id = 500;
    fix_part1.start_timestamp = 1000;
    fix_part1.end_timestamp = 2000;
    fix_part1.data_type = milvus::proto::schema::DataType::Float;

    // Serialize the first instance
    auto serialized1 = fix_part1.Serialize();

    // Create a second instance by deserializing
    std::shared_ptr<uint8_t[]> shared_data(new uint8_t[serialized1.size()]);
    memcpy(shared_data.get(), serialized1.data(), serialized1.size());
    auto reader =
        std::make_shared<BinlogReader>(shared_data, serialized1.size());

    DescriptorEventDataFixPart fix_part2(reader);

    // Serialize the second instance
    auto serialized2 = fix_part2.Serialize();

    // The serialized data should be identical (binary compatible)
    ASSERT_EQ(serialized1.size(), serialized2.size());
    ASSERT_EQ(
        memcmp(serialized1.data(), serialized2.data(), serialized1.size()), 0);

    // Both instances should have the same values
    ASSERT_EQ(fix_part1.collection_id, fix_part2.collection_id);
    ASSERT_EQ(fix_part1.partition_id, fix_part2.partition_id);
    ASSERT_EQ(fix_part1.segment_id, fix_part2.segment_id);
    ASSERT_EQ(fix_part1.field_id, fix_part2.field_id);
    ASSERT_EQ(fix_part1.start_timestamp, fix_part2.start_timestamp);
    ASSERT_EQ(fix_part1.end_timestamp, fix_part2.end_timestamp);
    ASSERT_EQ(fix_part1.data_type, fix_part2.data_type);
}

TEST_F(EventDeprecatedFieldsTest,
       DescriptorEventDataFixPartMultipleOperations) {
    // Test that repeated serialize/deserialize operations maintain consistency
    DescriptorEventDataFixPart original;
    original.field_id = 999;
    original.start_timestamp = 5000;
    original.end_timestamp = 10000;
    original.data_type = milvus::proto::schema::DataType::String;

    for (int i = 0; i < 5; ++i) {
        // Serialize
        auto serialized = original.Serialize();

        // Deserialize
        std::shared_ptr<uint8_t[]> shared_data(new uint8_t[serialized.size()]);
        memcpy(shared_data.get(), serialized.data(), serialized.size());
        auto reader =
            std::make_shared<BinlogReader>(shared_data, serialized.size());

        DescriptorEventDataFixPart deserialized(reader);

        // Verify consistency
        ASSERT_EQ(deserialized.collection_id, DEPRECATED_ID_VALUE);
        ASSERT_EQ(deserialized.partition_id, DEPRECATED_ID_VALUE);
        ASSERT_EQ(deserialized.segment_id, DEPRECATED_ID_VALUE);
        ASSERT_EQ(deserialized.field_id, original.field_id);
        ASSERT_EQ(deserialized.start_timestamp, original.start_timestamp);
        ASSERT_EQ(deserialized.end_timestamp, original.end_timestamp);
        ASSERT_EQ(deserialized.data_type, original.data_type);

        // Use deserialized as the new original for next iteration
        original = deserialized;
    }
}