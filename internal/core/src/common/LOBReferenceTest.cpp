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
#include <cstring>
#include <limits>

#include "common/Types.h"

using namespace milvus;

class LOBReferenceTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
    }

    void
    TearDown() override {
    }
};

// Test 1: Default constructor
TEST_F(LOBReferenceTest, DefaultConstructor) {
    LOBReference ref;

    EXPECT_EQ(ref.magic, LOBReference::MAGIC_LOB_REF);
    EXPECT_EQ(ref.lob_file_id, 0);
    EXPECT_EQ(ref.row_offset, 0);
    EXPECT_TRUE(ref.IsLOBRef());
}

// Test 2: Constructor with values
TEST_F(LOBReferenceTest, ConstructorWithValues) {
    uint64_t file_id = 12345;
    uint32_t offset = 100;

    LOBReference ref(file_id, offset);

    EXPECT_EQ(ref.magic, LOBReference::MAGIC_LOB_REF);
    EXPECT_EQ(ref.lob_file_id, file_id);
    EXPECT_EQ(ref.row_offset, offset);
    EXPECT_TRUE(ref.IsLOBRef());
}

// Test 3: ForGrowing static constructor
TEST_F(LOBReferenceTest, ForGrowingConstructor) {
    uint64_t byte_offset = 1024 * 1024;  // 1MB offset
    uint32_t size = 4096;                // 4KB size

    LOBReference ref = LOBReference::ForGrowing(byte_offset, size);

    EXPECT_EQ(ref.magic, LOBReference::MAGIC_LOB_REF);
    EXPECT_EQ(ref.GetOffset(), byte_offset);
    EXPECT_EQ(ref.GetSize(), size);
    EXPECT_EQ(ref.lob_file_id, byte_offset);
    EXPECT_EQ(ref.row_offset, size);
}

// Test 4: Encode and decode - basic values
TEST_F(LOBReferenceTest, EncodeDecodeBasic) {
    LOBReference original(12345, 100);

    std::string encoded = original.EncodeToString();
    LOBReference decoded = LOBReference::DecodeFromString(encoded);

    EXPECT_EQ(decoded.magic, LOBReference::MAGIC_LOB_REF);
    EXPECT_EQ(decoded.lob_file_id, original.lob_file_id);
    EXPECT_EQ(decoded.row_offset, original.row_offset);
}

// Test 5: Encode and decode - zero values
TEST_F(LOBReferenceTest, EncodeDecodeZeros) {
    LOBReference original;

    std::string encoded = original.EncodeToString();
    LOBReference decoded = LOBReference::DecodeFromString(encoded);

    EXPECT_EQ(decoded.magic, LOBReference::MAGIC_LOB_REF);
    EXPECT_EQ(decoded.lob_file_id, 0);
    EXPECT_EQ(decoded.row_offset, 0);
}

// Test 6: Encode produces exactly 16 bytes
TEST_F(LOBReferenceTest, EncodedSize) {
    LOBReference ref(12345, 100);

    std::string encoded = ref.EncodeToString();

    EXPECT_EQ(encoded.size(), 16);
}

// Test 7: Encode is little-endian (new layout with magic first)
TEST_F(LOBReferenceTest, EncodeLittleEndian) {
    // magic = 0x00FF00FF (MAGIC_LOB_REF)
    // lob_file_id = 0x0102030405060708
    // row_offset = 0x11121314
    LOBReference ref(0x0807060504030201ULL, 0x14131211);

    std::string encoded = ref.EncodeToString();

    EXPECT_EQ(static_cast<uint8_t>(encoded[0]), 0xFF);
    EXPECT_EQ(static_cast<uint8_t>(encoded[1]), 0x00);
    EXPECT_EQ(static_cast<uint8_t>(encoded[2]), 0xFF);
    EXPECT_EQ(static_cast<uint8_t>(encoded[3]), 0x00);

    EXPECT_EQ(static_cast<uint8_t>(encoded[4]), 0x01);
    EXPECT_EQ(static_cast<uint8_t>(encoded[5]), 0x02);
    EXPECT_EQ(static_cast<uint8_t>(encoded[6]), 0x03);
    EXPECT_EQ(static_cast<uint8_t>(encoded[7]), 0x04);
    EXPECT_EQ(static_cast<uint8_t>(encoded[8]), 0x05);
    EXPECT_EQ(static_cast<uint8_t>(encoded[9]), 0x06);
    EXPECT_EQ(static_cast<uint8_t>(encoded[10]), 0x07);
    EXPECT_EQ(static_cast<uint8_t>(encoded[11]), 0x08);

    EXPECT_EQ(static_cast<uint8_t>(encoded[12]), 0x11);
    EXPECT_EQ(static_cast<uint8_t>(encoded[13]), 0x12);
    EXPECT_EQ(static_cast<uint8_t>(encoded[14]), 0x13);
    EXPECT_EQ(static_cast<uint8_t>(encoded[15]), 0x14);
}

// Test 8: Decode from raw bytes (new layout)
TEST_F(LOBReferenceTest, DecodeFromRawBytes) {
    std::array<uint8_t, 16> bytes = {
        0xFF,
        0x00,
        0xFF,
        0x00,  // magic (0x00FF00FF)
        0x01,
        0x02,
        0x03,
        0x04,
        0x05,
        0x06,
        0x07,
        0x08,  // lob_file_id
        0x11,
        0x12,
        0x13,
        0x14  // row_offset
    };

    std::string bytes_str(reinterpret_cast<const char*>(bytes.data()), 16);
    LOBReference decoded = LOBReference::DecodeFromString(bytes_str);

    EXPECT_EQ(decoded.magic, 0x00FF00FF);
    EXPECT_EQ(decoded.lob_file_id, 0x0807060504030201ULL);
    EXPECT_EQ(decoded.row_offset, 0x14131211);
}

// Test 9: IsLOBRef method
TEST_F(LOBReferenceTest, IsLOBRef) {
    LOBReference ref1;
    EXPECT_TRUE(ref1.IsLOBRef());

    LOBReference ref2(12345, 100);
    EXPECT_TRUE(ref2.IsLOBRef());

    LOBReference ref3;
    ref3.magic = LOBReference::MAGIC_LOB_REF;
    EXPECT_TRUE(ref3.IsLOBRef());

    LOBReference ref4;
    ref4.magic = 0x12345678;
    EXPECT_FALSE(ref4.IsLOBRef());
}

// Test 10: IsLOBReference static method with string
TEST_F(LOBReferenceTest, IsLOBReferenceStatic) {
    // Valid LOB reference (16 bytes with correct magic)
    LOBReference ref(12345, 100);
    std::string encoded = ref.EncodeToString();
    EXPECT_TRUE(LOBReference::IsLOBReference(encoded));

    // Not 16 bytes - should return false
    std::string short_string = "Hello";
    EXPECT_FALSE(LOBReference::IsLOBReference(short_string));

    std::string long_string = "This is a longer string than 16 bytes";
    EXPECT_FALSE(LOBReference::IsLOBReference(long_string));

    // 16 bytes but wrong magic
    std::string wrong_magic(16, 'x');
    EXPECT_FALSE(LOBReference::IsLOBReference(wrong_magic));

    // Inline text (not LOB reference)
    std::string inline_text = "Some text data!";  // exactly 15 bytes
    EXPECT_FALSE(LOBReference::IsLOBReference(inline_text));
}

// Test 11: EncodeToString and DecodeFromString
TEST_F(LOBReferenceTest, EncodeDecodeToFromString) {
    LOBReference original(0xDEADBEEFCAFEBABEULL, 0x12345678);

    std::string encoded = original.EncodeToString();
    EXPECT_EQ(encoded.size(), 16);

    LOBReference decoded = LOBReference::DecodeFromString(encoded);

    EXPECT_EQ(decoded.magic, LOBReference::MAGIC_LOB_REF);
    EXPECT_EQ(decoded.lob_file_id, original.lob_file_id);
    EXPECT_EQ(decoded.row_offset, original.row_offset);
}

// Test 12: Decode and extract fields
TEST_F(LOBReferenceTest, DecodeAndExtractFields) {
    LOBReference original(99999, 88888);
    std::string encoded = original.EncodeToString();

    // Check if it's a valid LOB reference
    EXPECT_TRUE(LOBReference::IsLOBReference(encoded));

    // Decode and extract fields
    LOBReference decoded = LOBReference::DecodeFromString(encoded);
    EXPECT_EQ(decoded.lob_file_id, 99999);
    EXPECT_EQ(decoded.row_offset, 88888);

    // Test with non-LOB reference data
    std::string not_lob_ref = "Not a LOB ref!!";
    EXPECT_FALSE(LOBReference::IsLOBReference(not_lob_ref));
}

// Test 13: Struct size is exactly 16 bytes (packed)
TEST_F(LOBReferenceTest, StructSize) {
    EXPECT_EQ(sizeof(LOBReference), 16);
}

// Test 14: Round-trip encode/decode with various values
TEST_F(LOBReferenceTest, RoundTripMultipleValues) {
    std::vector<LOBReference> test_cases = {
        LOBReference(0, 0),
        LOBReference(1, 1),
        LOBReference(100, 200),
        LOBReference(1000000, 2000000),
        LOBReference(0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFF),
        LOBReference(0x123456789ABCDEF0ULL, 0x12345678),
    };

    for (const auto& original : test_cases) {
        std::string encoded = original.EncodeToString();
        LOBReference decoded = LOBReference::DecodeFromString(encoded);

        EXPECT_EQ(decoded.magic, LOBReference::MAGIC_LOB_REF);
        EXPECT_EQ(decoded.lob_file_id, original.lob_file_id);
        EXPECT_EQ(decoded.row_offset, original.row_offset);
    }
}

// Test 15: Encode can be used as map key (via string conversion)
TEST_F(LOBReferenceTest, EncodeAsMapKey) {
    LOBReference ref1(100, 200);
    LOBReference ref2(100, 200);
    LOBReference ref3(100, 201);

    std::string key1 = ref1.EncodeToString();
    std::string key2 = ref2.EncodeToString();
    std::string key3 = ref3.EncodeToString();

    EXPECT_EQ(key1, key2);
    EXPECT_NE(key1, key3);
}

// Test 16: Growing segment use case
TEST_F(LOBReferenceTest, GrowingSegmentUseCase) {
    // Simulate writing multiple TEXT values to growing segment
    uint64_t current_offset = 0;

    std::vector<std::pair<std::string, LOBReference>> entries;

    std::vector<std::string> texts = {
        "Hello", "World", "This is a longer text"};

    for (const auto& text : texts) {
        LOBReference ref = LOBReference::ForGrowing(
            current_offset, static_cast<uint32_t>(text.size()));
        entries.emplace_back(text, ref);
        current_offset += text.size();
    }

    // Verify references
    EXPECT_EQ(entries[0].second.GetOffset(), 0);
    EXPECT_EQ(entries[0].second.GetSize(), 5);

    EXPECT_EQ(entries[1].second.GetOffset(), 5);
    EXPECT_EQ(entries[1].second.GetSize(), 5);

    EXPECT_EQ(entries[2].second.GetOffset(), 10);
    EXPECT_EQ(entries[2].second.GetSize(), 21);
}

// Test 17: Sealed segment use case
TEST_F(LOBReferenceTest, SealedSegmentUseCase) {
    // Simulate LOB references for sealed segment
    uint64_t lob_file_id = 999;

    std::vector<LOBReference> refs;
    for (uint32_t row = 0; row < 100; ++row) {
        refs.emplace_back(lob_file_id, row);
    }

    // Verify references
    for (uint32_t i = 0; i < refs.size(); ++i) {
        EXPECT_EQ(refs[i].magic, LOBReference::MAGIC_LOB_REF);
        EXPECT_EQ(refs[i].lob_file_id, lob_file_id);
        EXPECT_EQ(refs[i].row_offset, i);
        EXPECT_TRUE(refs[i].IsLOBRef());
    }
}
