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
#include <filesystem>
#include <fstream>

#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/GrowingLOBManager.h"
#include "pb/schema.pb.h"
#include "test_utils/DataGen.h"
#include "test_utils/Constants.h"
#include "common/Types.h"

using namespace milvus;
using namespace milvus::segcore;

class TextGrowingSegmentE2ETest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // configure LOB threshold (64KB)
        lob_threshold_ = 65536;
        SegcoreConfig::default_config().set_lob_size_threshold(lob_threshold_);

        // create schema
        schema_ = std::make_shared<Schema>();

        // add primary key field (int64)
        auto pk_field_id = schema_->AddDebugField("pk", DataType::INT64);
        schema_->set_primary_field_id(pk_field_id);

        // add TEXT field
        text_field_id_ = schema_->AddDebugField("text_field", DataType::TEXT);

        // create growing segment with LOB support
        auto segment_growing = CreateGrowingSegment(schema_, empty_index_meta);
        segment_ = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());
        segment_growing_ = std::move(segment_growing);

        ASSERT_NE(segment_, nullptr);
    }

    void
    TearDown() override {
        segment_growing_.reset();
        segment_ = nullptr;
    }

    // helper: generate a string of specified size
    std::string
    GenerateString(size_t size, char fill_char) {
        return std::string(size, fill_char);
    }

    // helper: insert data into segment
    void
    InsertData(const std::vector<int64_t>& pks,
               const std::vector<std::string>& texts) {
        int N = pks.size();

        auto insert_data = std::make_unique<proto::segcore::InsertRecord>();

        // add pk field
        auto pk_data = insert_data->add_fields_data();
        pk_data->set_field_id(100);
        pk_data->set_type(proto::schema::DataType::Int64);
        auto pk_array = pk_data->mutable_scalars()->mutable_long_data();
        for (auto pk : pks) {
            pk_array->add_data(pk);
        }

        // add TEXT field
        auto text_data = insert_data->add_fields_data();
        text_data->set_field_id(text_field_id_.get());
        text_data->set_type(proto::schema::DataType::Text);
        auto string_array = text_data->mutable_scalars()->mutable_string_data();
        for (const auto& text : texts) {
            string_array->add_data(text);
        }

        insert_data->set_num_rows(N);

        // create timestamps
        std::vector<Timestamp> timestamps(N);
        for (int i = 0; i < N; i++) {
            timestamps[i] = i + 1;
        }

        // insert
        auto offset = segment_->PreInsert(N);
        segment_->Insert(
            offset, N, nullptr, timestamps.data(), insert_data.get());
    }

    // helper: retrieve data from segment
    std::vector<std::string>
    RetrieveData(const std::vector<int64_t>& offsets) {
        auto result = segment_->bulk_subscript(
            nullptr, text_field_id_, offsets.data(), offsets.size());

        std::vector<std::string> retrieved;
        if (result && result->has_scalars() &&
            result->scalars().has_string_data()) {
            const auto& data = result->scalars().string_data().data();
            retrieved.assign(data.begin(), data.end());
        }

        return retrieved;
    }

    // helper: check if LOB files exist
    // LOB files are stored at: TestLocalPath/growing_blob/{segment_id}/lob/{field_id}
    bool
    LOBFilesExist(FieldId field_id) {
        auto lob_file_path = std::filesystem::path(TestLocalPath) /
                             "growing_blob" /
                             std::to_string(segment_->get_segment_id()) /
                             "lob" / std::to_string(field_id.get());
        return std::filesystem::exists(lob_file_path);
    }

    // helper: get LOB file size
    // LOB files are stored at: TestLocalPath/growing_blob/{segment_id}/lob/{field_id}
    size_t
    GetLOBFileSize(FieldId field_id) {
        auto lob_file_path = std::filesystem::path(TestLocalPath) /
                             "growing_blob" /
                             std::to_string(segment_->get_segment_id()) /
                             "lob" / std::to_string(field_id.get());
        if (std::filesystem::exists(lob_file_path)) {
            return std::filesystem::file_size(lob_file_path);
        }
        return 0;
    }

    int64_t lob_threshold_;
    SchemaPtr schema_;
    FieldId text_field_id_;
    SegmentGrowingPtr segment_growing_;
    SegmentGrowingImpl* segment_;
};

// Test 1: Growing Segment - Small texts only (all inline)
TEST_F(TextGrowingSegmentE2ETest, SmallTextsOnly_NoLOBFiles) {
    const int N = 5;

    std::vector<int64_t> pks = {1, 2, 3, 4, 5};
    std::vector<std::string> expected_texts = {
        "hello",
        GenerateString(1000, 'a'),   // 1KB
        GenerateString(10000, 'b'),  // 10KB
        GenerateString(50000, 'c'),  // 50KB (< 64KB)
        "world"};

    // insert
    InsertData(pks, expected_texts);

    // verify: no LOB files should be created
    EXPECT_FALSE(LOBFilesExist(text_field_id_))
        << "LOB files should not exist for small texts";

    // retrieve
    std::vector<int64_t> offsets = {0, 1, 2, 3, 4};
    auto retrieved = RetrieveData(offsets);

    // verify
    ASSERT_EQ(retrieved.size(), N);
    for (int i = 0; i < N; i++) {
        EXPECT_EQ(retrieved[i], expected_texts[i]) << "Mismatch at index " << i;
    }
}

// Test 2: Growing Segment - Large texts only (all LOB)
TEST_F(TextGrowingSegmentE2ETest, LargeTextsOnly_LOBFilesCreated) {
    const int N = 3;

    std::vector<int64_t> pks = {1, 2, 3};
    std::vector<std::string> expected_texts = {
        GenerateString(100 * 1024, 'x'),  // 100KB
        GenerateString(200 * 1024, 'y'),  // 200KB
        GenerateString(500 * 1024, 'z'),  // 500KB
    };

    // insert
    InsertData(pks, expected_texts);

    // verify: LOB files should be created
    EXPECT_TRUE(LOBFilesExist(text_field_id_))
        << "LOB files should exist for large texts";

    // verify: LOB file size should be approximately sum of text sizes
    size_t lob_file_size = GetLOBFileSize(text_field_id_);
    size_t expected_size = (100 + 200 + 500) * 1024;
    EXPECT_GT(lob_file_size, expected_size * 0.9)  // at least 90% of expected
        << "LOB file size too small: " << lob_file_size;
    EXPECT_LT(lob_file_size, expected_size * 1.5)  // at most 150% of expected
        << "LOB file size too large: " << lob_file_size;

    // retrieve
    std::vector<int64_t> offsets = {0, 1, 2};
    auto retrieved = RetrieveData(offsets);

    // verify
    ASSERT_EQ(retrieved.size(), N);
    for (int i = 0; i < N; i++) {
        EXPECT_EQ(retrieved[i].size(), expected_texts[i].size())
            << "Size mismatch at index " << i;
        EXPECT_EQ(retrieved[i], expected_texts[i])
            << "Content mismatch at index " << i;
    }
}

// Test 3: Growing Segment - Mixed storage (inline + LOB)
TEST_F(TextGrowingSegmentE2ETest, MixedStorage) {
    const int N = 8;

    std::vector<int64_t> pks = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<std::string> expected_texts;

    // create mixed data
    expected_texts.push_back("small1");                         // inline
    expected_texts.push_back(GenerateString(100 * 1024, 'a'));  // LOB
    expected_texts.push_back(GenerateString(5000, 'b'));        // inline
    expected_texts.push_back(GenerateString(200 * 1024, 'c'));  // LOB
    expected_texts.push_back("small2");                         // inline
    expected_texts.push_back(GenerateString(300 * 1024, 'd'));  // LOB
    expected_texts.push_back(GenerateString(50000, 'e'));       // inline
    expected_texts.push_back(GenerateString(150 * 1024, 'f'));  // LOB

    // insert
    InsertData(pks, expected_texts);

    // verify: LOB files should exist (because we have large texts)
    EXPECT_TRUE(LOBFilesExist(text_field_id_));

    // retrieve all
    std::vector<int64_t> offsets = {0, 1, 2, 3, 4, 5, 6, 7};
    auto retrieved = RetrieveData(offsets);

    // verify
    ASSERT_EQ(retrieved.size(), N);
    for (int i = 0; i < N; i++) {
        EXPECT_EQ(retrieved[i].size(), expected_texts[i].size())
            << "Size mismatch at index " << i;
        EXPECT_EQ(retrieved[i], expected_texts[i])
            << "Content mismatch at index " << i;
    }
}

// Test 4: Growing Segment - Partial retrieve (non-sequential)
TEST_F(TextGrowingSegmentE2ETest, PartialRetrieve_RandomAccess) {
    const int N = 10;

    std::vector<int64_t> pks;
    std::vector<std::string> all_texts;

    for (int i = 0; i < N; i++) {
        pks.push_back(i + 1);
        // alternate between small and large
        if (i % 2 == 0) {
            all_texts.push_back(
                GenerateString(1000 + i * 100, 'a' + i));  // inline
        } else {
            all_texts.push_back(
                GenerateString((100 + i * 50) * 1024, 'A' + i));  // LOB
        }
    }

    // insert all
    InsertData(pks, all_texts);

    // retrieve specific offsets: 1, 3, 5, 7, 9 (all LOB references)
    std::vector<int64_t> query_offsets = {1, 3, 5, 7, 9};
    auto retrieved = RetrieveData(query_offsets);

    // verify
    ASSERT_EQ(retrieved.size(), query_offsets.size());
    for (size_t i = 0; i < query_offsets.size(); i++) {
        int offset = query_offsets[i];
        EXPECT_EQ(retrieved[i].size(), all_texts[offset].size())
            << "Size mismatch at offset " << offset;
        EXPECT_EQ(retrieved[i], all_texts[offset])
            << "Content mismatch at offset " << offset;
    }

    // retrieve different offsets: 0, 2, 4, 6, 8 (all inline)
    query_offsets = {0, 2, 4, 6, 8};
    retrieved = RetrieveData(query_offsets);

    // verify
    ASSERT_EQ(retrieved.size(), query_offsets.size());
    for (size_t i = 0; i < query_offsets.size(); i++) {
        int offset = query_offsets[i];
        EXPECT_EQ(retrieved[i].size(), all_texts[offset].size())
            << "Size mismatch at offset " << offset;
        EXPECT_EQ(retrieved[i], all_texts[offset])
            << "Content mismatch at offset " << offset;
    }
}

// Test 5: Growing Segment - Multiple incremental inserts
TEST_F(TextGrowingSegmentE2ETest, MultipleIncrementalInserts) {
    // first batch: small texts
    InsertData({1, 2}, {"text1", "text2"});

    // verify after first batch
    auto retrieved1 = RetrieveData({0, 1});
    EXPECT_EQ(retrieved1[0], "text1");
    EXPECT_EQ(retrieved1[1], "text2");

    // second batch: large texts
    InsertData(
        {3, 4},
        {GenerateString(100 * 1024, 'x'), GenerateString(200 * 1024, 'y')});

    // verify LOB files now exist
    EXPECT_TRUE(LOBFilesExist(text_field_id_));

    // verify after second batch
    auto retrieved2 = RetrieveData({2, 3});
    EXPECT_EQ(retrieved2[0].size(), 100 * 1024);
    EXPECT_EQ(retrieved2[1].size(), 200 * 1024);

    // third batch: mixed
    InsertData({5, 6, 7}, {"text3", GenerateString(150 * 1024, 'z'), "text4"});

    // verify all data
    auto all_retrieved = RetrieveData({0, 1, 2, 3, 4, 5, 6});
    ASSERT_EQ(all_retrieved.size(), 7);
    EXPECT_EQ(all_retrieved[0], "text1");
    EXPECT_EQ(all_retrieved[1], "text2");
    EXPECT_EQ(all_retrieved[2].size(), 100 * 1024);
    EXPECT_EQ(all_retrieved[3].size(), 200 * 1024);
    EXPECT_EQ(all_retrieved[4], "text3");
    EXPECT_EQ(all_retrieved[5].size(), 150 * 1024);
    EXPECT_EQ(all_retrieved[6], "text4");
}

// Test 6: Growing Segment - Boundary threshold test
TEST_F(TextGrowingSegmentE2ETest, BoundaryThreshold) {
    const int N = 3;

    std::vector<int64_t> pks = {1, 2, 3};
    std::vector<std::string> expected_texts = {
        GenerateString(lob_threshold_ - 1, 'a'),  // 64KB - 1 (inline)
        GenerateString(lob_threshold_, 'b'),      // 64KB (LOB)
        GenerateString(lob_threshold_ + 1, 'c'),  // 64KB + 1 (LOB)
    };

    // insert
    InsertData(pks, expected_texts);

    // verify: LOB files exist (items 2 and 3 use LOB)
    EXPECT_TRUE(LOBFilesExist(text_field_id_));

    // retrieve
    std::vector<int64_t> offsets = {0, 1, 2};
    auto retrieved = RetrieveData(offsets);

    // verify
    ASSERT_EQ(retrieved.size(), N);
    for (int i = 0; i < N; i++) {
        EXPECT_EQ(retrieved[i].size(), expected_texts[i].size())
            << "Size mismatch at index " << i;
        EXPECT_EQ(retrieved[i], expected_texts[i])
            << "Content mismatch at index " << i;
    }
}

// Test 7: Growing Segment - Empty strings
TEST_F(TextGrowingSegmentE2ETest, EmptyStrings) {
    const int N = 5;

    std::vector<int64_t> pks = {1, 2, 3, 4, 5};
    std::vector<std::string> expected_texts = {
        "",                               // empty
        GenerateString(100 * 1024, 'a'),  // LOB
        "",                               // empty
        "small",                          // inline
        GenerateString(200 * 1024, 'b'),  // LOB
    };

    // insert
    InsertData(pks, expected_texts);

    // retrieve
    std::vector<int64_t> offsets = {0, 1, 2, 3, 4};
    auto retrieved = RetrieveData(offsets);

    // verify
    ASSERT_EQ(retrieved.size(), N);
    for (int i = 0; i < N; i++) {
        EXPECT_EQ(retrieved[i], expected_texts[i]) << "Mismatch at index " << i;
    }
}

// Test 8: Growing Segment - Large batch stress test
TEST_F(TextGrowingSegmentE2ETest, LargeBatchStressTest) {
    const int N = 100;

    std::vector<int64_t> pks;
    std::vector<std::string> expected_texts;

    for (int i = 0; i < N; i++) {
        pks.push_back(i + 1);

        // create various sizes
        size_t size;
        if (i % 5 == 0) {
            size = 100;  // very small
        } else if (i % 5 == 1) {
            size = 10000;  // 10KB
        } else if (i % 5 == 2) {
            size = 50000;  // 50KB (inline)
        } else if (i % 5 == 3) {
            size = 100 * 1024;  // 100KB (LOB)
        } else {
            size = 300 * 1024;  // 300KB (LOB)
        }

        expected_texts.push_back(GenerateString(size, 'a' + (i % 26)));
    }

    // insert
    InsertData(pks, expected_texts);

    // verify LOB files exist
    EXPECT_TRUE(LOBFilesExist(text_field_id_));

    // retrieve all
    std::vector<int64_t> offsets;
    for (int i = 0; i < N; i++) {
        offsets.push_back(i);
    }
    auto retrieved = RetrieveData(offsets);

    // verify
    ASSERT_EQ(retrieved.size(), N);
    for (int i = 0; i < N; i++) {
        EXPECT_EQ(retrieved[i].size(), expected_texts[i].size())
            << "Size mismatch at index " << i;

        // for large texts, check prefix and suffix only (performance)
        if (expected_texts[i].size() > 10000) {
            EXPECT_EQ(retrieved[i].substr(0, 100),
                      expected_texts[i].substr(0, 100))
                << "Prefix mismatch at index " << i;
            EXPECT_EQ(retrieved[i].substr(retrieved[i].size() - 100),
                      expected_texts[i].substr(expected_texts[i].size() - 100))
                << "Suffix mismatch at index " << i;
        } else {
            EXPECT_EQ(retrieved[i], expected_texts[i])
                << "Content mismatch at index " << i;
        }
    }
}

// Test 9: Growing Segment - Unicode and special characters
TEST_F(TextGrowingSegmentE2ETest, UnicodeAndSpecialCharacters) {
    const int N = 5;

    std::vector<int64_t> pks = {1, 2, 3, 4, 5};
    std::vector<std::string> expected_texts = {
        "Hello 世界 🌍",                      // unicode
        "Special: \n\t\r\\\"'",              // special chars
        GenerateString(100 * 1024, 'x'),     // large text (LOB)
        "Mixed: ABC123!@# 你好",             // mixed
        std::string(1, '\0') + "null byte",  // null byte
    };

    // insert
    InsertData(pks, expected_texts);

    // retrieve
    std::vector<int64_t> offsets = {0, 1, 2, 3, 4};
    auto retrieved = RetrieveData(offsets);

    // verify
    ASSERT_EQ(retrieved.size(), N);
    for (int i = 0; i < N; i++) {
        EXPECT_EQ(retrieved[i].size(), expected_texts[i].size())
            << "Size mismatch at index " << i;
        EXPECT_EQ(retrieved[i], expected_texts[i])
            << "Content mismatch at index " << i;
    }
}

// Test 10: Growing Segment - Verify LOB reference in stored data
TEST_F(TextGrowingSegmentE2ETest, VerifyLOBReferenceInStorage) {
    const int N = 3;

    std::vector<int64_t> pks = {1, 2, 3};
    std::vector<std::string> expected_texts = {
        "inline text",                    // inline
        GenerateString(100 * 1024, 'x'),  // LOB
        GenerateString(200 * 1024, 'y'),  // LOB
    };

    // insert
    InsertData(pks, expected_texts);

    // access raw field data
    auto& insert_record = segment_->get_insert_record();
    auto text_field_data =
        insert_record.template get_data<std::string>(text_field_id_);

    ASSERT_NE(text_field_data, nullptr);
    ASSERT_EQ(insert_record.ack_responder_.GetAck(), N);

    // verify stored data format
    const auto& stored_data_0 = (*text_field_data)[0];
    const auto& stored_data_1 = (*text_field_data)[1];
    const auto& stored_data_2 = (*text_field_data)[2];

    // first entry: inline text (not LOB reference)
    EXPECT_FALSE(LOBReference::IsLOBReference(stored_data_0))
        << "Small text should not be LOB reference";
    EXPECT_EQ(stored_data_0, "inline text");

    // second entry: LOB reference (16 bytes with magic)
    EXPECT_TRUE(LOBReference::IsLOBReference(stored_data_1))
        << "Large text should be LOB reference";
    EXPECT_EQ(stored_data_1.size(), 16);

    // verify magic bytes for second entry
    EXPECT_EQ(static_cast<uint8_t>(stored_data_1[0]), 0xFF);
    EXPECT_EQ(static_cast<uint8_t>(stored_data_1[1]), 0x00);
    EXPECT_EQ(static_cast<uint8_t>(stored_data_1[2]), 0xFF);
    EXPECT_EQ(static_cast<uint8_t>(stored_data_1[3]), 0x00);

    // third entry: also LOB reference
    EXPECT_TRUE(LOBReference::IsLOBReference(stored_data_2));
    EXPECT_EQ(stored_data_2.size(), 16);

    // decode LOB references
    LOBReference ref1 = LOBReference::DecodeFromString(stored_data_1);
    LOBReference ref2 = LOBReference::DecodeFromString(stored_data_2);

    EXPECT_EQ(ref1.magic, LOBReference::MAGIC_LOB_REF);
    EXPECT_EQ(ref2.magic, LOBReference::MAGIC_LOB_REF);

    // for growing segment, lob_file_id is byte offset, row_offset is size
    EXPECT_EQ(ref1.GetOffset(), 0);  // first LOB write starts at offset 0
    EXPECT_EQ(ref1.GetSize(), 100 * 1024);

    EXPECT_EQ(ref2.GetOffset(),
              100 * 1024);  // second LOB write starts after first
    EXPECT_EQ(ref2.GetSize(), 200 * 1024);

    // but when retrieved, should get original texts
    auto retrieved = RetrieveData({0, 1, 2});
    EXPECT_EQ(retrieved[1].size(), 100 * 1024);
    EXPECT_EQ(retrieved[2].size(), 200 * 1024);
}

// Test 11: Growing Segment - LOB offset tracking
TEST_F(TextGrowingSegmentE2ETest, LOBOffsetTracking) {
    // insert multiple large texts (all >= 64KB threshold) and verify offsets increase
    std::vector<int64_t> pks = {1, 2, 3, 4};
    std::vector<std::string> texts = {
        GenerateString(100 * 1024, 'a'),  // 100KB at offset 0
        GenerateString(70 * 1024,
                       'b'),  // 70KB at offset 100KB (>= 64KB threshold)
        GenerateString(200 * 1024, 'c'),  // 200KB at offset 170KB
        GenerateString(80 * 1024, 'd'),   // 80KB at offset 370KB
    };

    InsertData(pks, texts);

    // access stored data
    auto& insert_record = segment_->get_insert_record();
    auto text_field_data =
        insert_record.template get_data<std::string>(text_field_id_);

    ASSERT_NE(text_field_data, nullptr);

    // decode all LOB references and verify offsets
    uint64_t expected_offset = 0;
    for (size_t i = 0; i < texts.size(); i++) {
        const auto& stored_data = (*text_field_data)[i];
        EXPECT_TRUE(LOBReference::IsLOBReference(stored_data))
            << "Text at index " << i << " should be LOB reference";

        LOBReference ref = LOBReference::DecodeFromString(stored_data);
        EXPECT_EQ(ref.GetOffset(), expected_offset)
            << "Offset mismatch at index " << i;
        EXPECT_EQ(ref.GetSize(), texts[i].size())
            << "Size mismatch at index " << i;

        expected_offset += texts[i].size();
    }

    // verify LOB file size matches total
    size_t lob_file_size = GetLOBFileSize(text_field_id_);
    size_t total_size = (100 + 70 + 200 + 80) * 1024;
    EXPECT_EQ(lob_file_size, total_size)
        << "LOB file size should equal sum of all LOB data";
}
