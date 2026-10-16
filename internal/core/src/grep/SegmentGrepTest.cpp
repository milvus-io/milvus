// Copyright (C) 2026 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include <gtest/gtest.h>
#include <thread>
#include <arrow/c/bridge.h>
#include <arrow/c/abi.h>
#include "grep/segment_grep_c.h"
#include "segcore/test_utils/TextMatchTestUtils.h"
#include "milvus-storage/lob_column/lob_reference.h"

namespace milvus::segcore {
namespace {
class GrepScanTest
    : public ::testing::TestWithParam<std::tuple<bool, DataType>> {
 protected:
    SchemaPtr schema;
    SegcoreConfig config;
    std::shared_ptr<storage::LocalDirectory> binlogs;
    std::unique_ptr<SegmentInternalInterface> segment;
    CGrepScanOptions options{};
    void
    SetUp() override {
        schema = std::make_shared<Schema>();
        schema->AddField(FieldMeta(FieldName("pk"),
                                   FieldId(100),
                                   DataType::INT64,
                                   false,
                                   std::nullopt));
        schema->set_primary_field_id(FieldId(100));
        std::map<std::string, std::string> params;
        schema->AddField(FieldMeta(FieldName("text"),
                                   FieldId(101),
                                   std::get<1>(GetParam()),
                                   65536,
                                   true,
                                   false,
                                   false,
                                   params,
                                   std::nullopt));
        config = SegcoreConfig::default_config();
        config.set_chunk_rows(64);
        options.field_id = 101;
        options.limit = 2;
        options.batch_size = 128;

        options.max_regex_bytes = 1048576;
        options.max_dfa_cache_bytes = 1048576;
        options.max_result_bytes = 1048576;
        Load({"no match", "timeout", "timeout", "超时 timeout", "", "\n"},
             {true, false, true, true, true, true});
    }
    void
    Load(const std::vector<std::string>& texts,
         const std::vector<bool>& valid) {
        segment.reset();
        auto data = text_test::DataGen(schema, texts.size());
        binlogs = data.binlogs;
        auto* field = data.raw_->mutable_fields_data(1);
        for (size_t i = 0; i < texts.size(); ++i) {
            std::string value = texts[i];
            if (!std::get<0>(GetParam()) &&
                std::get<1>(GetParam()) == DataType::TEXT) {
                auto encoded =
                    milvus_storage::lob_column::EncodeInlineText(value);
                value.assign(reinterpret_cast<const char*>(encoded.data()),
                             encoded.size());
            }
            field->mutable_scalars()->mutable_string_data()->set_data(i, value);
            MutableFieldDataRowValidData(field)->Set(i, valid[i]);
        }
        if (std::get<0>(GetParam())) {
            auto growing =
                CreateGrowingSegment(schema, empty_index_meta, 0, config);
            auto offset = growing->PreInsert(texts.size());
            growing->Insert(offset,
                            texts.size(),
                            data.row_ids_.data(),
                            data.timestamps_.data(),
                            data.owner);
            segment = std::move(growing);
        } else {
            segment = text_test::CreateSealedWithFieldDataLoaded(schema, data);
            if (std::get<1>(GetParam()) == DataType::TEXT) {
                dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get())
                    ->SetTextLobPathForTesting(FieldId(101),
                                               binlogs->Path() + "/lobs");
            }
        }
    }
    std::shared_ptr<arrow::RecordBatch>
    Scan(const std::vector<int64_t>& offsets,
         const std::string& pattern = "timeout",
         int expected = Success,
         folly::CancellationSource* cancel = nullptr,
         const std::vector<std::string>& expected_messages = {}) {
        ArrowSchema schema{};
        ArrowArray array{};
        uint64_t length = pattern.size();
        auto status =
            ScanGrepByOffsets(segment.get(),
                              offsets.data(),
                              offsets.size(),
                              &options,
                              reinterpret_cast<const uint8_t*>(pattern.data()),
                              pattern.size(),
                              &length,
                              1,
                              &schema,
                              &array,
                              cancel);
        EXPECT_EQ(status.error_code, expected) << status.error_msg;
        if (status.error_code != Success) {
            for (const auto& message : expected_messages) {
                EXPECT_NE(std::string(status.error_msg).find(message),
                          std::string::npos)
                    << status.error_msg;
            }
            free(const_cast<char*>(status.error_msg));
            EXPECT_EQ(schema.release, nullptr);
            EXPECT_EQ(array.release, nullptr);
            return nullptr;
        }
        auto imported = arrow::ImportRecordBatch(&array, &schema);
        EXPECT_TRUE(imported.ok()) << imported.status();
        return imported.ok() ? imported.ValueOrDie() : nullptr;
    }
};
INSTANTIATE_TEST_SUITE_P(SealedAndGrowing,
                         GrepScanTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Values(DataType::VARCHAR,
                                                              DataType::TEXT)));

TEST_P(GrepScanTest, PositionsDetailsAndOwnership) {
    auto result = Scan({3, 0, 1, 2, 4});
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->num_rows(), 2);
    auto positions =
        std::static_pointer_cast<arrow::Int64Array>(result->column(0));
    EXPECT_EQ(positions->Value(0), 0);
    EXPECT_EQ(positions->Value(1), 3);
    segment
        .reset();  // exported Arrow arrays must not borrow segment or Rust buffers
    auto rows = std::static_pointer_cast<arrow::ListArray>(result->column(1));
    auto lines = std::static_pointer_cast<arrow::StructArray>(rows->values());
    auto texts = std::static_pointer_cast<arrow::StringArray>(lines->field(0));
    EXPECT_EQ(texts->GetString(0), "超时 timeout");
    auto spans = std::static_pointer_cast<arrow::ListArray>(lines->field(1));
    auto bounds = std::static_pointer_cast<arrow::StructArray>(spans->values());
    EXPECT_EQ(std::static_pointer_cast<arrow::UInt64Array>(bounds->field(0))
                  ->Value(0),
              7);
    EXPECT_EQ(std::static_pointer_cast<arrow::UInt64Array>(bounds->field(1))
                  ->Value(0),
              14);
}

TEST_P(GrepScanTest, StopsBeforeUnreadInvalidOffset) {
    options.batch_size = 1;
    options.limit = 1;
    auto result = Scan({0, 1, 2, -1});
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->num_rows(), 1);
    EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>(result->column(0))
                  ->Value(0),
              2);
}

TEST_P(GrepScanTest, ConfiguredReadBatchIsIndependentOfLimit) {
    options.limit = 1;
    options.batch_size = 1;
    auto result = Scan({2, -1});
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->num_rows(), 1);
    // With a larger read batch the invalid offset is visited during prefetch,
    // even though the first row would satisfy the matching limit.
    options.batch_size = 2;
    Scan({2, -1}, "timeout", UnexpectedError);
    options.batch_size = 0;
    Scan({2}, "timeout", UnexpectedError);
}

TEST_P(GrepScanTest, StopsMatchingInsidePrefetchedBatch) {
    Load({"timeout", std::string("\xff", 1)}, {true, true});
    options.batch_size = 128;
    options.limit = 1;
    auto result = Scan({0, 1});
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->num_rows(), 1);
    // The later prefetched text must not reach the regex engine after limit.
    options.limit = 2;
    Scan({0, 1}, "timeout", DataFormatBroken);
}

TEST_P(GrepScanTest, EmptyNullAndMultipleBatches) {
    auto empty = Scan({}, "");
    ASSERT_NE(empty, nullptr);
    ASSERT_EQ(empty->num_rows(), 0);
    auto nulls = Scan({1}, "");
    ASSERT_NE(nulls, nullptr);
    ASSERT_EQ(nulls->num_rows(), 0);
    auto blank = Scan({4}, "");
    ASSERT_NE(blank, nullptr);
    ASSERT_EQ(blank->num_rows(), 0);
    auto blank_line = Scan({5}, "");
    ASSERT_NE(blank_line, nullptr);
    ASSERT_EQ(blank_line->num_rows(), 1);
    std::vector<std::string> texts(300, "no match");
    texts[270] = "timeout";
    texts[299] = "timeout";
    Load(texts, std::vector<bool>(texts.size(), true));
    std::vector<int64_t> offsets(300);
    std::iota(offsets.begin(), offsets.end(), 0);
    auto result = Scan(offsets);
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->num_rows(), 2);
    EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>(result->column(0))
                  ->Value(0),
              270);
    EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>(result->column(0))
                  ->Value(1),
              299);
}

TEST_P(GrepScanTest, ReadsTextLargerThanFormerInputBudget) {
    if (std::get<1>(GetParam()) != DataType::TEXT) {
        GTEST_SKIP() << "VARCHAR retains its schema length constraint";
    }
    Load({std::string(9 << 20, 'x') + "\ntimeout"}, {true});
    auto result = Scan({0});
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->num_rows(), 1);
}

TEST_P(GrepScanTest, ErrorsHaveNoPartialOutputs) {
    Scan({2}, "[", InvalidParameter);
    Scan({-1}, "timeout", UnexpectedError);
    options.max_result_bytes = 16;
    Scan({2}, "timeout", OutOfRange);
    options.max_result_bytes = 1048576;
    folly::CancellationSource cancel;
    cancel.requestCancellation();
    Scan({2}, "timeout", FollyCancel, &cancel);
    // A failure must not poison later calls on the same segment.
    auto recovered = Scan({2});
    ASSERT_NE(recovered, nullptr);
    ASSERT_EQ(recovered->num_rows(), 1);
}

TEST_P(GrepScanTest, BudgetErrorsCarryConfigurationAndCause) {
    options.max_regex_bytes = 1;
    Scan({2},
         "[a-z]+",
         UnexpectedError,
         nullptr,
         {"function.grep.maxRegexBytes=1 bytes",
          "compiled regex exceeds size limit of 1"});
    options.max_regex_bytes = 1048576;
    options.max_result_bytes = 1;
    Scan({2},
         "timeout",
         OutOfRange,
         nullptr,
         {"quotaAndLimits.limits.maxOutputSize=1 bytes",
          "result metadata budget exceeded"});
    options.max_result_bytes = 128;
    Scan({2},
         "timeout",
         OutOfRange,
         nullptr,
         {"quotaAndLimits.limits.maxOutputSize=128 bytes",
          "resource limit exceeded: ResultBytes"});
}

TEST_P(GrepScanTest, CumulativeResultBudget) {
    options.limit = 100;
    options.max_result_bytes = 1024;
    Scan(std::vector<int64_t>(100, 2), "timeout", OutOfRange);
}

TEST_P(GrepScanTest, CancellationDuringExecution) {
    Load(std::vector<std::string>(20000, std::string(512, 'x')),
         std::vector<bool>(20000, true));
    std::vector<int64_t> offsets(20000);
    std::iota(offsets.begin(), offsets.end(), 0);
    folly::CancellationSource source;
    std::thread cancel([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        source.requestCancellation();
    });
    Scan(offsets, "timeout", FollyCancel, &source);
    cancel.join();
}

TEST_P(GrepScanTest, PersistedInvalidUtf8IsSystemError) {
    Load({std::string("\xff", 1)}, {true});
    Scan({0}, "timeout", DataFormatBroken);
}
}  // namespace
}  // namespace milvus::segcore
