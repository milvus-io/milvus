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
#include <string>
#include <vector>
#include <random>
#include <thread>
#include <chrono>
#include <filesystem>

#include "segcore/GrowingLOBManager.h"
#include "common/Types.h"

using namespace milvus;
using namespace milvus::segcore;

class GrowingLOBTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        segment_id_ = 12345;
        field_id_ = FieldId(100);
        field_id2_ = FieldId(101);

        // create temp directory for test
        test_root_ = std::filesystem::temp_directory_path() / "milvus_lob_test";
        std::filesystem::create_directories(test_root_);

        lob_manager_ = std::make_unique<GrowingLOBManager>(segment_id_,
                                                           test_root_.string());
    }

    void
    TearDown() override {
        lob_manager_.reset();

        // clean up test directory
        std::filesystem::remove_all(test_root_);
    }

    std::string
    GenerateRandomString(size_t length) {
        static const char charset[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, sizeof(charset) - 2);

        std::string result;
        result.reserve(length);
        for (size_t i = 0; i < length; ++i) {
            result += charset[dis(gen)];
        }
        return result;
    }

    int64_t segment_id_;
    FieldId field_id_;
    FieldId field_id2_;
    std::filesystem::path test_root_;
    std::unique_ptr<GrowingLOBManager> lob_manager_;
};

// Basic WriteLOB functionality
TEST_F(GrowingLOBTest, WriteLOB_Basic) {
    std::string text_data = "Hello, this is a test TEXT value";

    LOBReference ref = lob_manager_->WriteLOB(field_id_, text_data);

    // offset should be 0 for first write
    EXPECT_EQ(ref.GetOffset(), 0);
    // size should match text length
    EXPECT_EQ(ref.GetSize(), text_data.size());
}

// Basic ReadLOB functionality
TEST_F(GrowingLOBTest, ReadLOB_Basic) {
    std::string text_data = "Hello, this is a test TEXT value";

    LOBReference ref = lob_manager_->WriteLOB(field_id_, text_data);
    std::string retrieved = lob_manager_->ReadLOB(field_id_, ref);

    EXPECT_EQ(retrieved, text_data);
}

// Multiple writes - verify offset tracking
TEST_F(GrowingLOBTest, WriteLOB_Multiple) {
    std::vector<std::string> texts = {"First TEXT value",
                                      "Second TEXT value",
                                      "Third TEXT value with more data"};

    std::vector<LOBReference> refs;
    uint64_t expected_offset = 0;

    for (const auto& text : texts) {
        LOBReference ref = lob_manager_->WriteLOB(field_id_, text);
        refs.push_back(ref);

        // verify offset and size
        EXPECT_EQ(ref.GetOffset(), expected_offset);
        EXPECT_EQ(ref.GetSize(), text.size());

        expected_offset += text.size();
    }

    // verify all can be read back correctly
    for (size_t i = 0; i < texts.size(); ++i) {
        std::string retrieved = lob_manager_->ReadLOB(field_id_, refs[i]);
        EXPECT_EQ(retrieved, texts[i]);
    }
}

// Small TEXT
TEST_F(GrowingLOBTest, SmallText) {
    std::string small_text = GenerateRandomString(100);  // 100 bytes

    LOBReference ref = lob_manager_->WriteLOB(field_id_, small_text);
    std::string retrieved = lob_manager_->ReadLOB(field_id_, ref);

    EXPECT_EQ(retrieved, small_text);
    EXPECT_EQ(retrieved.size(), 100);
}

// Large TEXT (greater than inline threshold)
TEST_F(GrowingLOBTest, LargeText) {
    std::string large_text = GenerateRandomString(1024 * 1024);  // 1MB

    LOBReference ref = lob_manager_->WriteLOB(field_id_, large_text);
    std::string retrieved = lob_manager_->ReadLOB(field_id_, ref);

    EXPECT_EQ(retrieved, large_text);
    EXPECT_EQ(retrieved.size(), 1024 * 1024);
}

// Mixed sizes
TEST_F(GrowingLOBTest, MixedSizes) {
    std::vector<size_t> sizes = {10, 100, 1000, 10000, 100000};
    std::vector<std::string> texts;
    std::vector<LOBReference> refs;

    for (auto size : sizes) {
        texts.push_back(GenerateRandomString(size));
        refs.push_back(lob_manager_->WriteLOB(field_id_, texts.back()));
    }

    // Verify all can be read correctly
    for (size_t i = 0; i < texts.size(); ++i) {
        std::string retrieved = lob_manager_->ReadLOB(field_id_, refs[i]);
        EXPECT_EQ(retrieved, texts[i]);
        EXPECT_EQ(retrieved.size(), sizes[i]);
    }
}

// Statistics for single field
TEST_F(GrowingLOBTest, GetStats) {
    std::string text1 = GenerateRandomString(100);
    std::string text2 = GenerateRandomString(200);
    std::string text3 = GenerateRandomString(300);

    lob_manager_->WriteLOB(field_id_, text1);
    lob_manager_->WriteLOB(field_id_, text2);
    lob_manager_->WriteLOB(field_id_, text3);

    auto stats = lob_manager_->GetTotalStats();
    EXPECT_EQ(stats.total_count, 3);
    EXPECT_EQ(stats.total_bytes, 600);  // 100 + 200 + 300

    auto field_stats = lob_manager_->GetFieldStats(field_id_);
    EXPECT_EQ(field_stats.total_count, 3);
    EXPECT_EQ(field_stats.total_bytes, 600);
}

// Concurrent writes (thread safety)
TEST_F(GrowingLOBTest, ConcurrentWrites) {
    const int num_threads = 10;
    const int writes_per_thread = 100;
    std::vector<std::thread> threads;
    std::vector<std::pair<LOBReference, std::string>> all_writes;
    std::mutex result_mutex;

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back(
            [this, t, writes_per_thread, &all_writes, &result_mutex]() {
                for (int i = 0; i < writes_per_thread; ++i) {
                    std::string text = "Thread " + std::to_string(t) +
                                       " write " + std::to_string(i);
                    LOBReference ref = lob_manager_->WriteLOB(field_id_, text);

                    std::lock_guard<std::mutex> lock(result_mutex);
                    all_writes.emplace_back(ref, text);
                }
            });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto stats = lob_manager_->GetTotalStats();
    EXPECT_EQ(stats.total_count, num_threads * writes_per_thread);

    // verify all writes can be read back
    for (const auto& [ref, text] : all_writes) {
        std::string retrieved = lob_manager_->ReadLOB(field_id_, ref);
        EXPECT_EQ(retrieved, text);
    }
}

// Concurrent reads (thread safety)
TEST_F(GrowingLOBTest, ConcurrentReads) {
    // First write some data
    std::vector<LOBReference> refs;
    std::vector<std::string> texts;
    for (int i = 0; i < 100; ++i) {
        texts.push_back("Text value " + std::to_string(i));
        refs.push_back(lob_manager_->WriteLOB(field_id_, texts.back()));
    }

    // Now read concurrently
    const int num_threads = 10;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, &refs, &texts, &success_count]() {
            for (size_t i = 0; i < refs.size(); ++i) {
                std::string retrieved =
                    lob_manager_->ReadLOB(field_id_, refs[i]);
                if (retrieved == texts[i]) {
                    success_count++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(success_count.load(),
              num_threads * static_cast<int>(refs.size()));
}

// Empty string
TEST_F(GrowingLOBTest, EmptyString) {
    std::string empty_text = "";

    LOBReference ref = lob_manager_->WriteLOB(field_id_, empty_text);
    std::string retrieved = lob_manager_->ReadLOB(field_id_, ref);

    EXPECT_EQ(retrieved, empty_text);
    EXPECT_EQ(retrieved.size(), 0);
    EXPECT_EQ(ref.GetSize(), 0);
}

// Very large TEXT (10MB)
TEST_F(GrowingLOBTest, VeryLargeText) {
    std::string very_large_text =
        GenerateRandomString(10 * 1024 * 1024);  // 10MB

    LOBReference ref = lob_manager_->WriteLOB(field_id_, very_large_text);
    std::string retrieved = lob_manager_->ReadLOB(field_id_, ref);

    EXPECT_EQ(retrieved.size(), very_large_text.size());
    EXPECT_EQ(retrieved, very_large_text);
}

// File storage tracking
TEST_F(GrowingLOBTest, FileStorageTracking) {
    size_t total_size = 0;
    std::vector<size_t> sizes = {100, 500, 1000, 5000};

    for (auto size : sizes) {
        std::string text = GenerateRandomString(size);
        lob_manager_->WriteLOB(field_id_, text);
        total_size += size;
    }

    auto stats = lob_manager_->GetTotalStats();
    EXPECT_EQ(stats.total_bytes, total_size);
}

// LOB file path with field_id
TEST_F(GrowingLOBTest, LOBFilePath) {
    std::string lob_file = lob_manager_->GetLOBFilePath(field_id_);
    std::string expected = test_root_.string() + "/" +
                           std::to_string(segment_id_) + "/lob/" +
                           std::to_string(field_id_.get());
    EXPECT_EQ(lob_file, expected);
}

// File persistence - data appended correctly
TEST_F(GrowingLOBTest, FilePersistence) {
    std::string text1 = "First text data";
    std::string text2 = "Second text data";

    LOBReference ref1 = lob_manager_->WriteLOB(field_id_, text1);
    LOBReference ref2 = lob_manager_->WriteLOB(field_id_, text2);

    // Verify offsets are sequential
    EXPECT_EQ(ref1.GetOffset(), 0);
    EXPECT_EQ(ref2.GetOffset(), text1.size());

    // Verify file content
    std::string retrieved1 = lob_manager_->ReadLOB(field_id_, ref1);
    std::string retrieved2 = lob_manager_->ReadLOB(field_id_, ref2);
    EXPECT_EQ(retrieved1, text1);
    EXPECT_EQ(retrieved2, text2);
}

// GetOffset and GetSize helpers
TEST_F(GrowingLOBTest, ReferenceHelpers) {
    std::string text = "Test data for helpers";
    LOBReference ref = lob_manager_->WriteLOB(field_id_, text);

    EXPECT_EQ(ref.GetOffset(), 0);
    EXPECT_EQ(ref.GetSize(), text.size());

    // ForGrowing factory method
    LOBReference ref2 = LOBReference::ForGrowing(1000, 500);
    EXPECT_EQ(ref2.GetOffset(), 1000);
    EXPECT_EQ(ref2.GetSize(), 500);
}

// Multiple TEXT fields - data isolation
TEST_F(GrowingLOBTest, MultipleFields) {
    std::string text1 = "Field 1 text data";
    std::string text2 = "Field 2 different text";
    std::string text3 = "Field 1 another value";

    // Write to field 1
    LOBReference ref1 = lob_manager_->WriteLOB(field_id_, text1);
    // Write to field 2
    LOBReference ref2 = lob_manager_->WriteLOB(field_id2_, text2);
    // Write to field 1 again
    LOBReference ref3 = lob_manager_->WriteLOB(field_id_, text3);

    // Verify data is isolated per field
    // Field 1: offset should be 0 for first write
    EXPECT_EQ(ref1.GetOffset(), 0);
    EXPECT_EQ(ref1.GetSize(), text1.size());
    // Field 2: offset should be 0 for first write (different file)
    EXPECT_EQ(ref2.GetOffset(), 0);
    EXPECT_EQ(ref2.GetSize(), text2.size());
    // Field 1: second write should follow first
    EXPECT_EQ(ref3.GetOffset(), text1.size());
    EXPECT_EQ(ref3.GetSize(), text3.size());

    // Read back and verify
    EXPECT_EQ(lob_manager_->ReadLOB(field_id_, ref1), text1);
    EXPECT_EQ(lob_manager_->ReadLOB(field_id2_, ref2), text2);
    EXPECT_EQ(lob_manager_->ReadLOB(field_id_, ref3), text3);

    // Check per-field statistics
    auto stats1 = lob_manager_->GetFieldStats(field_id_);
    EXPECT_EQ(stats1.total_count, 2);
    EXPECT_EQ(stats1.total_bytes, text1.size() + text3.size());

    auto stats2 = lob_manager_->GetFieldStats(field_id2_);
    EXPECT_EQ(stats2.total_count, 1);
    EXPECT_EQ(stats2.total_bytes, text2.size());

    // Check total statistics
    auto total_stats = lob_manager_->GetTotalStats();
    EXPECT_EQ(total_stats.total_count, 3);
    EXPECT_EQ(total_stats.total_bytes,
              text1.size() + text2.size() + text3.size());
}

// Concurrent writes to multiple fields
TEST_F(GrowingLOBTest, ConcurrentWritesMultipleFields) {
    const int num_threads = 10;
    const int writes_per_thread = 50;
    std::vector<std::thread> threads;
    std::vector<std::tuple<FieldId, LOBReference, std::string>> all_writes;
    std::mutex result_mutex;

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back(
            [this, t, writes_per_thread, &all_writes, &result_mutex]() {
                // Alternate between field_id_ and field_id2_
                FieldId field = (t % 2 == 0) ? field_id_ : field_id2_;
                for (int i = 0; i < writes_per_thread; ++i) {
                    std::string text = "Field " + std::to_string(field.get()) +
                                       " Thread " + std::to_string(t) +
                                       " write " + std::to_string(i);
                    LOBReference ref = lob_manager_->WriteLOB(field, text);

                    std::lock_guard<std::mutex> lock(result_mutex);
                    all_writes.emplace_back(field, ref, text);
                }
            });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto stats = lob_manager_->GetTotalStats();
    EXPECT_EQ(stats.total_count, num_threads * writes_per_thread);

    // verify all writes can be read back
    for (const auto& [field, ref, text] : all_writes) {
        std::string retrieved = lob_manager_->ReadLOB(field, ref);
        EXPECT_EQ(retrieved, text);
    }
}

// File path for different fields
TEST_F(GrowingLOBTest, DifferentFieldPaths) {
    std::string path1 = lob_manager_->GetLOBFilePath(field_id_);
    std::string path2 = lob_manager_->GetLOBFilePath(field_id2_);

    // Paths should be different
    EXPECT_NE(path1, path2);

    // Both should contain the field ID
    EXPECT_TRUE(path1.find(std::to_string(field_id_.get())) !=
                std::string::npos);
    EXPECT_TRUE(path2.find(std::to_string(field_id2_.get())) !=
                std::string::npos);
}
