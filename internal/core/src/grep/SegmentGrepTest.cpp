// Copyright (C) 2026 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include <gtest/gtest.h>
#include <thread>
#include <filesystem>
#include <arrow/filesystem/localfs.h>
#include <arrow/c/bridge.h>
#include <arrow/c/abi.h>
#include "grep/segment_grep_c.h"
#include "segcore/test_utils/TextMatchTestUtils.h"
#include "segcore/arrow_field_utils.h"
#include "milvus-storage/lob_column/lob_column_manager.h"
#include "milvus-storage/lob_column/lob_column_writer.h"
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
         const std::vector<bool>& valid,
         const std::vector<int64_t>& sealed_chunk_rows = {},
         bool encode_text = true) {
        segment.reset();
        auto data = text_test::DataGen(schema, texts.size());
        binlogs = data.binlogs;
        auto* field = data.raw_->mutable_fields_data(1);
        for (size_t i = 0; i < texts.size(); ++i) {
            std::string value = texts[i];
            if (encode_text && !std::get<0>(GetParam()) &&
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
            if (sealed_chunk_rows.empty()) {
                segment =
                    text_test::CreateSealedWithFieldDataLoaded(schema, data);
            } else {
                auto manager = storage::CreateChunkManager(
                    text_test::get_default_local_storage_config());
                auto info =
                    text_test::PrepareInsertBinlog(1, 2, 3, data, manager);
                info.field_infos.erase(101);
                std::vector<FieldDataPtr> batches;
                int64_t begin = 0;
                const auto& stored = field->scalars().string_data().data();
                for (auto count : sealed_chunk_rows) {
                    ASSERT_LE(begin + count, texts.size());
                    auto native = storage::CreateFieldData(
                        std::get<1>(GetParam()), DataType::NONE, true);
                    std::vector<std::string> values(
                        stored.begin() + begin, stored.begin() + begin + count);
                    std::vector<uint8_t> bitmap((count + 7) / 8, 0);
                    for (int64_t i = 0; i < count; ++i) {
                        if (valid[begin + i])
                            bitmap[i / 8] |= uint8_t{1} << (i % 8);
                    }
                    native->FillFieldData(
                        values.data(), bitmap.data(), count, 0);
                    batches.push_back(std::move(native));
                    begin += count;
                }
                ASSERT_EQ(begin, texts.size());
                text_test::AddBinlog(
                    info, binlogs->Path(), 101, batches, manager);
                auto sealed = CreateSealedSegment(schema, empty_index_meta);
                sealed->LoadFieldData(info);
                segment = std::move(sealed);
                ASSERT_EQ(segment->num_chunk_data(FieldId(101)),
                          sealed_chunk_rows.size());
            }
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
    auto details =
        std::static_pointer_cast<arrow::StructArray>(result->column(1));
    auto rows = std::static_pointer_cast<arrow::ListArray>(details->field(0));
    auto texts = std::static_pointer_cast<arrow::StringArray>(rows->values());
    EXPECT_EQ(texts->GetString(0), "超时 timeout");
    auto start_docs =
        std::static_pointer_cast<arrow::ListArray>(details->field(1));
    auto end_docs =
        std::static_pointer_cast<arrow::ListArray>(details->field(2));
    auto starts =
        std::static_pointer_cast<arrow::ListArray>(start_docs->values());
    auto ends = std::static_pointer_cast<arrow::ListArray>(end_docs->values());
    EXPECT_EQ(details->type()->field(1)->name(), "starts");
    EXPECT_EQ(details->type()->field(2)->name(), "ends");
    EXPECT_EQ(rows->value_length(0), start_docs->value_length(0));
    EXPECT_EQ(rows->value_length(0), end_docs->value_length(0));
    EXPECT_EQ(starts->value_length(0), ends->value_length(0));
    ASSERT_EQ(starts->values()->type_id(), arrow::Type::INT64);
    ASSERT_EQ(ends->values()->type_id(), arrow::Type::INT64);
    EXPECT_EQ(
        std::static_pointer_cast<arrow::Int64Array>(starts->values())->Value(0),
        7);
    EXPECT_EQ(
        std::static_pointer_cast<arrow::Int64Array>(ends->values())->Value(0),
        14);
}

TEST_P(GrepScanTest, StructArrayDocumentsAndSpans) {
    Load({"foo bar foo\nhello foo", "no match", "foo"}, {true, true, true});
    options.batch_size = 1;
    auto result = Scan({0, 1, 2}, "foo");
    ASSERT_NE(result, nullptr);
    ASSERT_TRUE(result->ValidateFull().ok());
    ASSERT_EQ(result->num_rows(), 2);
    ASSERT_EQ(result->column(1)->type_id(), arrow::Type::STRUCT);
    auto details =
        std::static_pointer_cast<arrow::StructArray>(result->column(1));
    auto content =
        std::static_pointer_cast<arrow::ListArray>(details->field(0));
    auto start_docs =
        std::static_pointer_cast<arrow::ListArray>(details->field(1));
    auto end_docs =
        std::static_pointer_cast<arrow::ListArray>(details->field(2));
    for (int64_t row = 0; row < 2; ++row) {
        EXPECT_EQ(content->value_length(row), row == 0 ? 2 : 1);
        EXPECT_EQ(content->value_length(row), start_docs->value_length(row));
        EXPECT_EQ(content->value_length(row), end_docs->value_length(row));
    }
    auto texts =
        std::static_pointer_cast<arrow::StringArray>(content->values());
    EXPECT_EQ(texts->GetString(0), "foo bar foo");
    EXPECT_EQ(texts->GetString(1), "hello foo");
    auto starts =
        std::static_pointer_cast<arrow::ListArray>(start_docs->values());
    auto ends = std::static_pointer_cast<arrow::ListArray>(end_docs->values());
    EXPECT_EQ(starts->value_length(0), 2);
    EXPECT_EQ(starts->value_length(1), 1);
    auto a = std::static_pointer_cast<arrow::Int64Array>(starts->values());
    auto b = std::static_pointer_cast<arrow::Int64Array>(ends->values());
    const std::vector<int64_t> expected_starts{0, 8, 6, 0},
        expected_ends{3, 11, 9, 3};
    ASSERT_EQ(a->length(), expected_starts.size());
    ASSERT_EQ(b->length(), expected_ends.size());
    for (int64_t i = 0; i < a->length(); ++i) {
        EXPECT_EQ(a->Value(i), expected_starts[i]);
        EXPECT_EQ(b->Value(i), expected_ends[i]);
    }
    auto empty = Scan({1}, "foo");
    ASSERT_NE(empty, nullptr);
    EXPECT_EQ(empty->num_rows(), 0);
    EXPECT_TRUE(empty->schema()->Equals(result->schema()));
    EXPECT_TRUE(empty->ValidateFull().ok());
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

TEST_P(GrepScanTest, SparseAndRepeatedOffsetsAcrossChunkBoundaries) {
    std::vector<std::string> texts;
    for (int i = 0; i < 200; ++i)
        texts.push_back("timeout row " + std::to_string(i));
    texts[120] = "no match";
    std::vector<bool> valid(texts.size(), true);
    valid[70] = false;
    Load(texts, valid, {64, 96, 40});
    const std::vector<int64_t> offsets = {
        199, 64, 63, 70, 65, 127, 64, 159, 160, 2, 0, 199, 120};
    options.limit = 100;
    for (auto batch_size : {1, 2, 4, 128}) {
        SCOPED_TRACE(batch_size);
        options.batch_size = batch_size;
        auto result = Scan(offsets);
        ASSERT_NE(result, nullptr);
        auto positions =
            std::static_pointer_cast<arrow::Int64Array>(result->column(0));
        auto result_struct =
            std::static_pointer_cast<arrow::StructArray>(result->column(1));
        auto details =
            std::static_pointer_cast<arrow::ListArray>(result_struct->field(0));
        auto contents =
            std::static_pointer_cast<arrow::StringArray>(details->values());
        int64_t row = 0;
        for (size_t i = 0; i < offsets.size(); ++i) {
            if (!valid[offsets[i]] || offsets[i] == 120)
                continue;
            ASSERT_LT(row, result->num_rows());
            EXPECT_EQ(positions->Value(row), i);
            ASSERT_EQ(details->value_length(row), 1);
            EXPECT_EQ(contents->GetString(details->value_offset(row)),
                      texts[offsets[i]]);
            ++row;
        }
        EXPECT_EQ(result->num_rows(), row);
    }
    if (std::get<1>(GetParam()) == DataType::VARCHAR) {
        options.limit = 1;
        options.batch_size = 128;
        // Row 64 starts a new chunk. Reaching the limit in the first chunk
        // must not inspect the invalid offset in the unread next chunk.
        auto result = Scan({63, 64, -1});
        ASSERT_NE(result, nullptr);
        EXPECT_EQ(result->num_rows(), 1);
        Scan({63, -1}, "timeout", UnexpectedError);
    } else {
        // Unlike VARCHAR, TEXT prefetch covers the complete configured batch,
        // even across chunk boundaries and when its first match meets limit.
        options.limit = 1;
        options.batch_size = 128;
        Scan({63, 64, -1}, "timeout", UnexpectedError);
    }
}

TEST_P(GrepScanTest, MixedInlineAndFileTextPreservesOrder) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::TEXT) {
        GTEST_SKIP() << "sealed internal TEXT only";
    }
    using namespace milvus_storage::lob_column;
    auto directory = storage::LocalDirectory::CreateOwned(
        std::filesystem::temp_directory_path().string(),
        "grep-text-lob-XXXXXX",
        "Grep mixed TEXT test");
    LobColumnConfig lob;
    lob.lob_base_path = directory->Path() + "/lobs";
    lob.field_id = 101;
    lob.inline_threshold = 32;
    auto manager = LobColumnManager::Create(
        std::make_shared<arrow::fs::LocalFileSystem>(), lob);
    ASSERT_TRUE(manager.ok()) << manager.status();
    auto writer = manager.ValueOrDie()->CreateWriter();
    ASSERT_TRUE(writer.ok()) << writer.status();
    const std::vector<std::string> texts = {
        "timeout inline",
        std::string(4096, 'x') + "\ntimeout file",
        "",
        "timeout null",
        "超时 timeout"};
    auto encoded = writer.ValueOrDie()->WriteBatch(texts);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    auto files = writer.ValueOrDie()->Close();
    ASSERT_TRUE(files.ok()) << files.status();
    ASSERT_FALSE(files.ValueOrDie().empty());
    std::vector<std::string> stored;
    for (const auto& value : encoded.ValueOrDie()) {
        stored.emplace_back(reinterpret_cast<const char*>(value.data()),
                            value.size());
    }
    ASSERT_TRUE(IsInlineData(encoded.ValueOrDie()[0].data()));
    ASSERT_TRUE(IsLOBReference(encoded.ValueOrDie()[1].data()));
    Load(stored, {true, true, true, false, true}, {2, 3}, false);
    dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get())
        ->SetTextLobPathForTesting(FieldId(101), lob.lob_base_path);
    options.limit = 100;
    for (auto batch_size : {1, 2, 128}) {
        options.batch_size = batch_size;
        auto result = Scan({4, 1, 0, 3, 2, 4});
        ASSERT_NE(result, nullptr);
        auto positions =
            std::static_pointer_cast<arrow::Int64Array>(result->column(0));
        ASSERT_EQ(result->num_rows(), 4)
            << "batch_size=" << batch_size
            << " positions=" << result->column(0)->ToString()
            << " details=" << result->column(1)->ToString();
        const std::vector<int64_t> expected_positions = {0, 1, 2, 5};
        auto details =
            std::static_pointer_cast<arrow::StructArray>(result->column(1));
        auto rows =
            std::static_pointer_cast<arrow::ListArray>(details->field(0));
        auto contents =
            std::static_pointer_cast<arrow::StringArray>(rows->values());
        const std::vector<std::string> expected = {
            "超时 timeout", "timeout file", "timeout inline", "超时 timeout"};
        for (int64_t i = 0; i < result->num_rows(); ++i) {
            EXPECT_EQ(positions->Value(i), expected_positions[i]);
            ASSERT_EQ(rows->value_length(i), 1);
            EXPECT_EQ(contents->GetString(rows->value_offset(i)), expected[i]);
        }
    }
}

TEST_P(GrepScanTest, FileTextFailureStillOccursDuringPrefetch) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::TEXT) {
        GTEST_SKIP() << "sealed internal TEXT only";
    }
    using namespace milvus_storage::lob_column;
    const auto inline_value = EncodeInlineText("timeout");
    const uint8_t uuid[16] = {};
    const auto file_ref = EncodeLOBReference(uuid, 0);
    const std::vector<std::string> stored = {
        {reinterpret_cast<const char*>(inline_value.data()),
         inline_value.size()},
        {reinterpret_cast<const char*>(file_ref.data()), file_ref.size()}};
    Load(stored, {true, true}, {}, false);
    const int64_t offsets[] = {0, 1};
    int expected_error = Success;
    try {
        milvus::OpContext ctx;
        const auto snapshot = segment->CaptureReadSnapshot();
        const auto schema_snapshot = segment->get_schema_snapshot();
        BulkSubscriptWithSnapshot(segment.get(),
                                  snapshot,
                                  &ctx,
                                  FieldId(101),
                                  (*schema_snapshot)[FieldId(101)],
                                  offsets,
                                  2);
    } catch (const SegcoreError& error) {
        expected_error = error.get_error_code();
    }
    ASSERT_NE(expected_error, Success);
    options.limit = 1;
    options.batch_size = 2;
    Scan({0, 1}, "timeout", expected_error);
    options.batch_size = 1;
    ASSERT_NE(Scan({0, 1}), nullptr);
    // Null LOB references must not cause file reads.
    Load(stored, {true, false}, {}, false);
    options.batch_size = 2;
    ASSERT_NE(Scan({0, 1}), nullptr);
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
          "resource limit exceeded: ResultBytes"});
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
