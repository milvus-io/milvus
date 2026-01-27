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

#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "segcore/TextLobSpillover.h"

using namespace milvus::segcore;

class TextLobSpilloverTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Create a temp directory for tests
        test_dir_ =
            "/tmp/milvus_text_lob_test_" + std::to_string(std::time(nullptr));
        std::filesystem::create_directories(test_dir_);
    }

    void
    TearDown() override {
        // Clean up temp directory
        if (std::filesystem::exists(test_dir_)) {
            std::filesystem::remove_all(test_dir_);
        }
    }

    std::string test_dir_;
};

TEST_F(TextLobSpilloverTest, TextLobRefEncoding) {
    TextLobRef ref;
    ref.offset = 12345;
    ref.size = 100;
    ref.flags = 0;

    // Test encoding
    std::string encoded = ref.Encode();
    ASSERT_EQ(encoded.size(), TextLobRef::kEncodedSize);

    // Test decoding
    TextLobRef decoded = TextLobRef::Decode(encoded);
    ASSERT_EQ(decoded.offset, ref.offset);
    ASSERT_EQ(decoded.size, ref.size);
    ASSERT_EQ(decoded.flags, ref.flags);
}

TEST_F(TextLobSpilloverTest, TextLobWriterReader) {
    std::string path = test_dir_ + "/test.lob";

    // Write some data
    {
        TextLobWriter writer(path);

        TextLobRef ref1 = writer.Write("Hello");
        ASSERT_EQ(ref1.offset, 0);
        ASSERT_EQ(ref1.size, 5);

        TextLobRef ref2 = writer.Write("World!");
        ASSERT_EQ(ref2.offset, 5);
        ASSERT_EQ(ref2.size, 6);

        ASSERT_EQ(writer.GetCurrentOffset(), 11);
    }

    // Read data back
    {
        TextLobReader reader(path);

        std::string text1 = reader.Read(0, 5);
        ASSERT_EQ(text1, "Hello");

        std::string text2 = reader.Read(5, 6);
        ASSERT_EQ(text2, "World!");
    }
}

TEST_F(TextLobSpilloverTest, TextLobSpilloverBasic) {
    int64_t segment_id = 12345;
    FieldId field_id(100);

    // Create spillover with RAII cleanup
    {
        TextLobSpillover spillover(segment_id, field_id, test_dir_);

        // Verify path is created
        ASSERT_FALSE(spillover.GetPath().empty());
        ASSERT_TRUE(std::filesystem::exists(spillover.GetPath()));

        // Write and encode
        std::string ref1 = spillover.WriteAndEncode("Hello, World!");
        ASSERT_EQ(ref1.size(), TextLobRef::kEncodedSize);

        std::string ref2 = spillover.WriteAndEncode("Testing TEXT spillover");
        ASSERT_EQ(ref2.size(), TextLobRef::kEncodedSize);

        // Decode and read
        std::string text1 = spillover.DecodeAndRead(ref1);
        ASSERT_EQ(text1, "Hello, World!");

        std::string text2 = spillover.DecodeAndRead(ref2);
        ASSERT_EQ(text2, "Testing TEXT spillover");
    }
    // After spillover goes out of scope, temp file should be deleted
    // (Note: the specific path format includes growing_lob/{segment_id}/{field_id}.lob)
}

TEST_F(TextLobSpilloverTest, ConcurrentWrites) {
    int64_t segment_id = 12345;
    FieldId field_id(100);
    TextLobSpillover spillover(segment_id, field_id, test_dir_);

    const int num_threads = 4;
    const int writes_per_thread = 100;

    std::vector<std::thread> threads;
    std::vector<std::vector<std::string>> refs(num_threads);

    // Multiple threads writing concurrently
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < writes_per_thread; i++) {
                std::string text =
                    "Thread" + std::to_string(t) + "_Item" + std::to_string(i);
                refs[t].push_back(spillover.WriteAndEncode(text));
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all writes can be read back correctly
    for (int t = 0; t < num_threads; t++) {
        for (int i = 0; i < writes_per_thread; i++) {
            std::string expected =
                "Thread" + std::to_string(t) + "_Item" + std::to_string(i);
            std::string actual = spillover.DecodeAndRead(refs[t][i]);
            ASSERT_EQ(actual, expected)
                << "Mismatch at thread " << t << ", item " << i;
        }
    }
}

TEST_F(TextLobSpilloverTest, LargeText) {
    int64_t segment_id = 12345;
    FieldId field_id(100);
    TextLobSpillover spillover(segment_id, field_id, test_dir_);

    // Create a large text (1MB)
    std::string large_text(1024 * 1024, 'A');
    for (size_t i = 0; i < large_text.size(); i++) {
        large_text[i] = 'A' + (i % 26);
    }

    // Write and read back
    std::string ref = spillover.WriteAndEncode(large_text);
    std::string read_back = spillover.DecodeAndRead(ref);

    ASSERT_EQ(read_back, large_text);
}

TEST_F(TextLobSpilloverTest, EmptyText) {
    int64_t segment_id = 12345;
    FieldId field_id(100);
    TextLobSpillover spillover(segment_id, field_id, test_dir_);

    // Empty string
    std::string ref = spillover.WriteAndEncode("");
    std::string read_back = spillover.DecodeAndRead(ref);

    ASSERT_EQ(read_back, "");
}
