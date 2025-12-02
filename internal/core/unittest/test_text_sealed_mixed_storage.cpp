// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <memory>

#include "common/Types.h"

using namespace milvus;

class TextSealedMixedStorageTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // test setup if needed
    }

    void
    TearDown() override {
        // test cleanup if needed
    }

    // helper: generate a string of specified size
    std::string
    GenerateString(size_t size, char fill_char = 'x') {
        return std::string(size, fill_char);
    }

    // helper: create a LOB reference and encode it
    std::string
    CreateLOBReferenceString(uint64_t file_id, uint32_t row_offset) {
        LOBReference ref(file_id, row_offset);
        return ref.EncodeToString();
    }
};

// Test 1: IsLOBReference correctly identifies LOB references
TEST_F(TextSealedMixedStorageTest, IsLOBReference_ValidReference) {
    // create a valid LOB reference
    LOBReference ref(12345, 100);
    std::string encoded = ref.EncodeToString();

    // verify it's identified as LOB reference
    EXPECT_TRUE(LOBReference::IsLOBReference(encoded));
    EXPECT_EQ(encoded.size(), 16);
}

// Test 2: IsLOBReference correctly rejects inline text
TEST_F(TextSealedMixedStorageTest, IsLOBReference_InlineText) {
    // various inline text scenarios
    std::vector<std::string> inline_texts = {
        "hello world",             // short text
        GenerateString(15, 'a'),   // 15 bytes (< 16)
        GenerateString(17, 'b'),   // 17 bytes (> 16)
        GenerateString(100, 'c'),  // 100 bytes
        "",                        // empty string
    };

    for (const auto& text : inline_texts) {
        EXPECT_FALSE(LOBReference::IsLOBReference(text))
            << "Should not identify inline text as LOB reference: size="
            << text.size();
    }
}

// Test 3: IsLOBReference with exactly 16 bytes but wrong magic
TEST_F(TextSealedMixedStorageTest, IsLOBReference_SixteenBytesWrongMagic) {
    // create a 16-byte string that doesn't start with the magic number
    std::string fake_ref = "0123456789ABCDEF";  // exactly 16 bytes
    ASSERT_EQ(fake_ref.size(), 16);

    // should NOT be identified as LOB reference (wrong magic)
    EXPECT_FALSE(LOBReference::IsLOBReference(fake_ref));
}

// Test 4: IsLOBReference with correct magic number
TEST_F(TextSealedMixedStorageTest, IsLOBReference_CorrectMagic) {
    // manually construct a buffer with correct magic
    std::array<uint8_t, 16> buffer;

    // set magic (0x00FF00FF in little-endian: 0xFF 0x00 0xFF 0x00)
    buffer[0] = 0xFF;
    buffer[1] = 0x00;
    buffer[2] = 0xFF;
    buffer[3] = 0x00;

    // fill rest with arbitrary data
    for (int i = 4; i < 16; i++) {
        buffer[i] = static_cast<uint8_t>(i);
    }

    std::string data(reinterpret_cast<const char*>(buffer.data()), 16);

    // should be identified as LOB reference (correct magic)
    EXPECT_TRUE(LOBReference::IsLOBReference(data));
}

// Test 5: DecodeFromString correctly decodes LOB reference
TEST_F(TextSealedMixedStorageTest, DecodeFromString_ValidReference) {
    uint64_t expected_file_id = 0xDEADBEEFCAFEBABE;
    uint32_t expected_row_offset = 0x12345678;

    LOBReference original(expected_file_id, expected_row_offset);
    std::string encoded = original.EncodeToString();

    LOBReference decoded = LOBReference::DecodeFromString(encoded);

    EXPECT_EQ(decoded.magic, LOBReference::MAGIC_LOB_REF);
    EXPECT_EQ(decoded.lob_file_id, expected_file_id);
    EXPECT_EQ(decoded.row_offset, expected_row_offset);
}

// Test 6: Mixed storage scenario - distinguishing inline from LOB
TEST_F(TextSealedMixedStorageTest, MixedStorage_Identification) {
    // create a mix of inline text and LOB references
    std::vector<std::string> mixed_data;

    // add inline text
    mixed_data.push_back("small text 1");
    mixed_data.push_back(GenerateString(1000, 'a'));

    // add LOB references
    mixed_data.push_back(CreateLOBReferenceString(100, 0));
    mixed_data.push_back(CreateLOBReferenceString(200, 5));

    // add more inline text
    mixed_data.push_back("small text 2");
    mixed_data.push_back(CreateLOBReferenceString(300, 10));

    // verify identification
    EXPECT_FALSE(LOBReference::IsLOBReference(mixed_data[0]));  // inline
    EXPECT_FALSE(LOBReference::IsLOBReference(mixed_data[1]));  // inline
    EXPECT_TRUE(LOBReference::IsLOBReference(mixed_data[2]));   // LOB ref
    EXPECT_TRUE(LOBReference::IsLOBReference(mixed_data[3]));   // LOB ref
    EXPECT_FALSE(LOBReference::IsLOBReference(mixed_data[4]));  // inline
    EXPECT_TRUE(LOBReference::IsLOBReference(mixed_data[5]));   // LOB ref
}

// Test 7: Edge case - empty string is not LOB reference
TEST_F(TextSealedMixedStorageTest, EmptyString_NotLOBReference) {
    std::string empty = "";
    EXPECT_FALSE(LOBReference::IsLOBReference(empty));
}

// Test 8: LOB reference encoding/decoding preserves data
TEST_F(TextSealedMixedStorageTest, EncodeDecode_RoundTrip) {
    std::vector<std::pair<uint64_t, uint32_t>> test_cases = {
        {0, 0},
        {1, 1},
        {12345, 67890},
        {0xFFFFFFFFFFFFFFFF, 0xFFFFFFFF},
        {0x123456789ABCDEF0, 0x87654321},
    };

    for (const auto& [file_id, row_offset] : test_cases) {
        LOBReference original(file_id, row_offset);
        std::string encoded = original.EncodeToString();

        EXPECT_EQ(encoded.size(), 16);
        EXPECT_TRUE(LOBReference::IsLOBReference(encoded));

        LOBReference decoded = LOBReference::DecodeFromString(encoded);

        EXPECT_EQ(decoded.magic, LOBReference::MAGIC_LOB_REF);
        EXPECT_EQ(decoded.lob_file_id, file_id);
        EXPECT_EQ(decoded.row_offset, row_offset);
    }
}

// Test 9: Multiple consecutive encode/decode operations
TEST_F(TextSealedMixedStorageTest, EncodeDecode_MultipleIterations) {
    LOBReference original(0xDEADBEEFCAFEBABE, 0x12345678);

    LOBReference current = original;
    for (int i = 0; i < 10; ++i) {
        std::string encoded = current.EncodeToString();
        current = LOBReference::DecodeFromString(encoded);
    }

    // after multiple iterations, data should remain intact
    EXPECT_EQ(current.magic, LOBReference::MAGIC_LOB_REF);
    EXPECT_EQ(current.lob_file_id, original.lob_file_id);
    EXPECT_EQ(current.row_offset, original.row_offset);
}

// Test 10: LOB reference as map key (uniqueness test)
TEST_F(TextSealedMixedStorageTest, LOBReference_AsMapKey) {
    LOBReference ref1(100, 200);
    LOBReference ref2(100, 200);  // same values
    LOBReference ref3(100, 201);  // different offset

    std::string key1 = ref1.EncodeToString();
    std::string key2 = ref2.EncodeToString();
    std::string key3 = ref3.EncodeToString();

    // same values should produce same key
    EXPECT_EQ(key1, key2);

    // different values should produce different keys
    EXPECT_NE(key1, key3);
}

// Test 11: Magic number is in correct position (first 4 bytes)
TEST_F(TextSealedMixedStorageTest, MagicNumber_Position) {
    LOBReference ref(0x123456789ABCDEF0, 0x87654321);
    std::string encoded = ref.EncodeToString();

    // check magic bytes (little-endian: 0xFF 0x00 0xFF 0x00)
    EXPECT_EQ(static_cast<uint8_t>(encoded[0]), 0xFF);
    EXPECT_EQ(static_cast<uint8_t>(encoded[1]), 0x00);
    EXPECT_EQ(static_cast<uint8_t>(encoded[2]), 0xFF);
    EXPECT_EQ(static_cast<uint8_t>(encoded[3]), 0x00);
}

// Test 12: IsLOBRef instance method
TEST_F(TextSealedMixedStorageTest, IsLOBRef_InstanceMethod) {
    // valid LOB reference
    LOBReference ref1(100, 200);
    EXPECT_TRUE(ref1.IsLOBRef());

    // LOB reference with correct magic
    LOBReference ref2;
    ref2.magic = LOBReference::MAGIC_LOB_REF;
    ref2.lob_file_id = 12345;
    ref2.row_offset = 67890;
    EXPECT_TRUE(ref2.IsLOBRef());

    // LOB reference with wrong magic
    LOBReference ref3;
    ref3.magic = 0x12345678;  // wrong magic
    ref3.lob_file_id = 12345;
    ref3.row_offset = 67890;
    EXPECT_FALSE(ref3.IsLOBRef());
}

// Test 13: Struct size is exactly 16 bytes
TEST_F(TextSealedMixedStorageTest, StructSize) {
    EXPECT_EQ(sizeof(LOBReference), 16);
}

// Test 14: Mixed storage simulation - realistic scenario
TEST_F(TextSealedMixedStorageTest, RealisticMixedStorage) {
    // simulate a TEXT field with mixed storage (threshold = 64KB)
    std::vector<std::pair<std::string, bool>> test_data;  // <data, is_lob_ref>

    // small texts (inline)
    test_data.push_back({GenerateString(100, 'a'), false});
    test_data.push_back({GenerateString(1000, 'b'), false});
    test_data.push_back({"hello world", false});

    // large texts (LOB references)
    test_data.push_back({CreateLOBReferenceString(1001, 0), true});
    test_data.push_back({CreateLOBReferenceString(1002, 1), true});

    // more small texts (inline)
    test_data.push_back({GenerateString(5000, 'c'), false});
    test_data.push_back({CreateLOBReferenceString(1003, 2), true});

    // verify each item is correctly identified
    for (size_t i = 0; i < test_data.size(); ++i) {
        const auto& [data, is_lob_ref] = test_data[i];

        bool detected_as_lob = LOBReference::IsLOBReference(data);
        EXPECT_EQ(detected_as_lob, is_lob_ref)
            << "Item " << i << ": expected is_lob_ref=" << is_lob_ref
            << ", but detected as " << detected_as_lob;

        if (is_lob_ref) {
            // should be able to decode
            LOBReference ref = LOBReference::DecodeFromString(data);
            EXPECT_EQ(ref.magic, LOBReference::MAGIC_LOB_REF);
            EXPECT_TRUE(ref.IsLOBRef());
        }
    }
}
