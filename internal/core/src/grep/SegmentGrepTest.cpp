// Copyright (C) 2026 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include <gtest/gtest.h>
#include <thread>
#include <numeric>
#include <limits>
#include <nlohmann/json.hpp>
#include <filesystem>
#include <arrow/filesystem/localfs.h>
#include <arrow/c/bridge.h>
#include <arrow/c/abi.h>
#include "grep/segment_grep_c.h"
#include "index/contracts/query/INgramReader.h"
#include "index/contracts/query/INullReader.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "query/PlanImpl.h"
#include "segcore/retrieve_result_export_c.h"
#include "segcore/test_utils/TextMatchTestUtils.h"
#include "segcore/arrow_field_utils.h"
#include "milvus-storage/lob_column/lob_column_manager.h"
#include "milvus-storage/lob_column/lob_column_writer.h"
#include "milvus-storage/lob_column/lob_reference.h"

namespace milvus::segcore {
namespace {
struct NgramProbe {
    int queries = 0;
    std::vector<int64_t> input_rows;
    bool short_coverage = false;
    bool element_domain = false;
    bool fail = false;
    folly::CancellationSource* cancel = nullptr;
};

// Observe the production reader through the real segment inventory/cache path.
class ObservedNgramReader final : public index::IIndexReaderBase,
                                  public index::INgramReader,
                                  public index::INullReader {
 public:
    ObservedNgramReader(index::IIndexReaderBasePtr reader,
                        std::shared_ptr<NgramProbe> probe)
        : reader_(std::move(reader)), probe_(std::move(probe)) {
    }
    index::ReaderCaps
    Caps() const override {
        return reader_->Caps();
    }
    index::Domain
    CoordDomain() const override {
        return probe_->element_domain ? index::Domain::Element
                                      : reader_->CoordDomain();
    }
    int64_t
    Count() const override {
        return reader_->Count() - probe_->short_coverage;
    }
    DataType
    ValueType() const override {
        return reader_->ValueType();
    }
    bool
    CanHandleConditionTree(
        const RegexLiteralCondition& condition) const override {
        return Ngram().CanHandleConditionTree(condition);
    }
    void
    ConditionTreeCandidates(const RegexLiteralCondition& condition,
                            TargetBitmap& candidates) const override {
        ++probe_->queries;
        probe_->input_rows.clear();
        for (size_t i = 0; i < candidates.size(); ++i) {
            if (candidates[i])
                probe_->input_rows.push_back(i);
        }
        if (probe_->fail) {
            ThrowInfo(DataFormatBroken, "injected Grep posting failure");
        }
        Ngram().ConditionTreeCandidates(condition, candidates);
        if (probe_->cancel)
            probe_->cancel->requestCancellation();
    }
    bool
    CanHandle(std::string_view literal, index::PatternOp op) const override {
        return Ngram().CanHandle(literal, op);
    }
    bool
    CandidatesAreExact(std::string_view literal,
                       index::PatternOp op) const override {
        return Ngram().CandidatesAreExact(literal, op);
    }
    void
    Candidates(std::string_view literal,
               index::PatternOp op,
               TargetBitmap& candidates) const override {
        Ngram().Candidates(literal, op, candidates);
    }
    TargetBitmap
    IsNull() const override {
        return dynamic_cast<const index::INullReader&>(*reader_).IsNull();
    }
    TargetBitmap
    IsNotNull() const override {
        return dynamic_cast<const index::INullReader&>(*reader_).IsNotNull();
    }

 private:
    const index::INgramReader&
    Ngram() const {
        return dynamic_cast<const index::INgramReader&>(*reader_);
    }
    index::IIndexReaderBasePtr reader_;
    std::shared_ptr<NgramProbe> probe_;
};

class GrepScanTest
    : public ::testing::TestWithParam<std::tuple<bool, DataType>> {
 protected:
    SchemaPtr schema;
    SegcoreConfig config;
    std::shared_ptr<storage::LocalDirectory> binlogs;
    std::shared_ptr<storage::LocalDirectory> lob_files;
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
    void
    LoadLobText(const std::vector<std::string>& texts,
                const std::vector<bool>& valid) {
        using namespace milvus_storage::lob_column;
        auto directory = storage::LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "grep-probe-lob-XXXXXX",
            "Grep probe LOB test");
        LobColumnConfig lob;
        lob.lob_base_path = directory->Path() + "/lobs";
        lob.field_id = 101;
        lob.inline_threshold = 32;
        auto manager = LobColumnManager::Create(
            std::make_shared<arrow::fs::LocalFileSystem>(), lob);
        ASSERT_TRUE(manager.ok()) << manager.status();
        auto writer = manager.ValueOrDie()->CreateWriter();
        ASSERT_TRUE(writer.ok()) << writer.status();
        auto encoded = writer.ValueOrDie()->WriteBatch(texts);
        ASSERT_TRUE(encoded.ok()) << encoded.status();
        auto files = writer.ValueOrDie()->Close();
        ASSERT_TRUE(files.ok()) << files.status();
        std::vector<std::string> stored;
        for (const auto& value : encoded.ValueOrDie()) {
            stored.emplace_back(reinterpret_cast<const char*>(value.data()),
                                value.size());
        }
        Load(stored, valid, {}, false);
        dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get())
            ->SetTextLobPathForTesting(FieldId(101), lob.lob_base_path);
        lob_files = std::move(directory);
    }

    std::shared_ptr<NgramProbe>
    AttachNgram(const std::vector<std::string>& texts,
                const std::vector<bool>& valid,
                bool mmap = false) {
        std::vector<uint8_t> bitmap((texts.size() + 7) / 8, 0);
        for (size_t i = 0; i < valid.size(); ++i) {
            if (valid[i])
                bitmap[i / 8] |= uint8_t{1} << (i % 8);
        }
        const auto type = std::get<1>(GetParam());
        auto field = std::make_shared<FieldData<std::string>>(type, true);
        static_cast<FieldDataBase&>(*field).FillFieldData(
            texts.data(), bitmap.data(), texts.size(), 0);
        auto opened = test::expr_index::BuildIndex(
            FieldId(101),
            type,
            "NGRAM",
            {field},
            Config{{"min_gram", 3}, {"max_gram", 4}},
            DataType::NONE,
            false,
            mmap);
        auto probe = std::make_shared<NgramProbe>();
        test::expr_index::DecorateIndex(
            opened, [probe](index::IIndexReaderBasePtr reader) {
                return std::make_unique<ObservedNgramReader>(std::move(reader),
                                                             probe);
            });
        test::expr_index::InstallIndex(
            *dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get()),
            FieldId(101),
            type,
            std::move(opened));
        return probe;
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

TEST_P(GrepScanTest, MaxCountIsPerRecordAndIndependentOfQueryLimit) {
    const std::vector<std::string> texts = {
        "skip\nerror error\nerror", "error", "error\nerror", ""};
    const std::vector<bool> valid = {true, false, true, true};
    for (bool indexed : {false, true}) {
        if (indexed && std::get<0>(GetParam()))
            continue;
        Load(texts, valid);
        if (indexed)
            AttachNgram(texts, valid);
        for (const std::string pattern : {"error", "(?i:error)"}) {
            options.has_max_count = 1;
            options.max_count = 1;
            options.limit = 2;
            options.batch_size = 2;
            auto result = Scan({0, 1, 0, 2, 3}, pattern);
            ASSERT_NE(result, nullptr);
            ASSERT_EQ(result->num_rows(), 2);
            auto positions =
                std::static_pointer_cast<arrow::Int64Array>(result->column(0));
            EXPECT_EQ(positions->Value(0), 0);
            EXPECT_EQ(positions->Value(1), 2);
            auto details =
                std::static_pointer_cast<arrow::BinaryArray>(result->column(1));
            const auto expected = nlohmann::json::parse(
                R"([{"content":"error error","line_number":2,"line_start_offset":5,"starts":[0,6],"ends":[5,11]}])");
            EXPECT_EQ(nlohmann::json::parse(details->GetString(0)), expected);
            EXPECT_EQ(nlohmann::json::parse(details->GetString(1)), expected);
            options.max_count = 0;
            ASSERT_EQ(Scan({0, 2}, pattern)->num_rows(), 0);
            options.has_max_count = 0;
            auto unlimited = Scan({0}, pattern);
            ASSERT_NE(unlimited, nullptr);
            auto all = std::static_pointer_cast<arrow::BinaryArray>(
                unlimited->column(1));
            EXPECT_EQ(nlohmann::json::parse(all->GetString(0)).size(), 2);
        }
    }
}

TEST_P(GrepScanTest,
       MaxCountZeroSkipsIndexAndUnreadableLobButValidatesPattern) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::TEXT)
        GTEST_SKIP() << "sealed LOB";
    const uint8_t uuid[16] = {};
    const auto missing =
        milvus_storage::lob_column::EncodeLOBReference(uuid, 0);
    Load({std::string(reinterpret_cast<const char*>(missing.data()),
                      missing.size())},
         {true},
         {},
         false);
    auto probe = AttachNgram({"error"}, {true});
    probe->fail = true;
    options.has_max_count = 1;
    options.max_count = 0;
    auto result = Scan({0}, "error");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->num_rows(), 0);
    EXPECT_EQ(probe->queries, 0);
    Scan({0}, "[", InvalidParameter);
    folly::CancellationSource cancel;
    cancel.requestCancellation();
    Scan({0}, "error", FollyCancel, &cancel);
}

TEST_P(GrepScanTest, NgramMatchesRawPositionsAndDetails) {
    if (std::get<0>(GetParam()))
        GTEST_SKIP() << "sealed index route";
    const std::vector<std::string> texts = {"error timeout",
                                            "timeout error",
                                            "warn timeout",
                                            "ordinary",
                                            "ERROR timeout",
                                            "error\ntimeout",
                                            "",
                                            "error timeout",
                                            "中文 error timeout",
                                            "ab",
                                            "needle tail"};
    std::vector<bool> valid(texts.size(), true);
    valid[7] = false;
    const std::vector<int64_t> offsets = {
        8, 3, 1, 0, 7, 4, 2, 8, 5, 9, 10, 6, 0};
    for (bool mmap : {false, true}) {
        Load(texts, valid, {4, 7});
        std::vector<std::shared_ptr<arrow::RecordBatch>> expected;
        for (auto pattern : {"error.*timeout",
                             "(error|warn).*timeout",
                             "(?i:error).*timeout",
                             "absent_phrase",
                             "error|ab",
                             ".*"}) {
            for (auto limit : {1, 100}) {
                for (auto batch_size : {1, 3, 128}) {
                    options.limit = limit;
                    options.batch_size = batch_size;
                    expected.push_back(Scan(offsets, pattern));
                }
            }
        }
        auto probe = AttachNgram(texts, valid, mmap);
        size_t i = 0;
        for (auto pattern : {"error.*timeout",
                             "(error|warn).*timeout",
                             "(?i:error).*timeout",
                             "absent_phrase",
                             "error|ab",
                             ".*"}) {
            SCOPED_TRACE(pattern);
            for (auto limit : {1, 100}) {
                for (auto batch_size : {1, 3, 128}) {
                    options.limit = limit;
                    options.batch_size = batch_size;
                    auto actual = Scan(offsets, pattern);
                    ASSERT_NE(actual, nullptr);
                    ASSERT_NE(expected[i], nullptr);
                    EXPECT_TRUE(actual->Equals(*expected[i++]))
                        << actual->ToString();
                }
            }
        }
        // VARCHAR can finish these short inputs within its prefix budget.
        // TEXT queries the index for the four prunable conditions.
        const auto expected_queries =
            std::get<1>(GetParam()) == DataType::TEXT ? 24 : 0;
        EXPECT_EQ(probe->queries, expected_queries);
        ASSERT_EQ(Scan({})->num_rows(), 0);
        EXPECT_EQ(probe->queries, expected_queries);
    }
}

TEST_P(GrepScanTest, NgramPreservesOffsetValidationAndRefinementLimits) {
    if (std::get<0>(GetParam()))
        GTEST_SKIP() << "sealed index route";
    const std::vector<std::string> texts = {
        "timeout error", "ordinary", "error timeout"};
    Load(texts, {true, true, true});
    auto probe = AttachNgram(texts, {true, true, true});
    options.limit = 1;
    options.batch_size = 1;
    auto result = Scan({0, 1, 2, -1}, "error.*timeout");
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->num_rows(), 1);
    EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>(result->column(0))
                  ->Value(0),
              2);
    options.batch_size = 2;
    Scan({2, -1}, "error.*timeout", UnexpectedError);
    Scan({99}, "absent_phrase", UnexpectedError);
    Scan({-1}, "absent_phrase", UnexpectedError);
    options.batch_size = 1;
    ASSERT_NE(Scan({2, -1}, "error.*timeout"), nullptr);
    options.max_result_bytes = 1;
    Scan({2}, "error.*timeout", InvalidParameter);
}

TEST_P(GrepScanTest,
       NgramIncompatibleReadersFallbackAndPostingErrorsPropagate) {
    if (std::get<0>(GetParam()))
        GTEST_SKIP() << "sealed index route";
    std::vector<std::string> texts(80, "ordinary");
    texts.back() = "error timeout";
    const std::vector<bool> valid(texts.size(), true);
    std::vector<int64_t> offsets(texts.size());
    std::iota(offsets.begin(), offsets.end(), 0);
    Load(texts, valid);
    auto expected = Scan(offsets, "error.*timeout");
    auto probe = AttachNgram(texts, valid);
    probe->short_coverage = true;
    ASSERT_TRUE(Scan(offsets, "error.*timeout")->Equals(*expected));
    probe->short_coverage = false;
    probe->element_domain = true;
    ASSERT_TRUE(Scan(offsets, "error.*timeout")->Equals(*expected));
    probe->element_domain = false;
    EXPECT_EQ(probe->queries, 0);
    probe->fail = true;
    Scan(offsets,
         "error.*timeout",
         DataFormatBroken,
         nullptr,
         {"injected Grep posting failure"});
    probe->fail = false;
    folly::CancellationSource cancel;
    probe->cancel = &cancel;
    Scan(offsets, "error.*timeout", FollyCancel, &cancel);
    probe->cancel = nullptr;
}

TEST_P(GrepScanTest, TextWideOrAlwaysQueriesIndexBeforeRefinement) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::TEXT)
        GTEST_SKIP() << "sealed TEXT index route";
    for (const bool dense : {false, true}) {
        std::vector<std::string> texts(300, dense ? "ERROR" : "ordinary");
        texts.back() = "error";
        const std::vector<bool> valid(texts.size(), true);
        std::vector<int64_t> offsets(texts.size());
        std::iota(offsets.begin(), offsets.end(), 0);
        Load(texts, valid);
        auto probe = AttachNgram(texts, valid);
        for (const int64_t limit : {1, 10, 100, 128, 129}) {
            options.limit = limit;
            const auto before = probe->queries;
            auto result = Scan(offsets, "(?i:error)");
            ASSERT_NE(result, nullptr);
            EXPECT_EQ(result->num_rows(), dense ? limit : 1);
            EXPECT_EQ(probe->queries, before + 1);
            EXPECT_EQ(probe->input_rows, offsets);
        }
    }
}

TEST_P(GrepScanTest, DenseOrProbeFinishesWithoutPostingQueries) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::VARCHAR)
        GTEST_SKIP() << "sealed VARCHAR probe route";
    const std::vector<std::string> texts = {
        "ERROR", "error", "Error", "ordinary", "error"};
    const std::vector<bool> valid = {true, true, true, true, false};
    const std::vector<int64_t> offsets = {2, 4, 0, 1, 2, 3, 3, 3, 3, 3};
    options.limit = 4;
    options.batch_size = 128;
    Load(texts, valid, {2, 3});
    auto expected = Scan(offsets, "(?i:error)");
    ASSERT_NE(expected, nullptr);
    auto probe = AttachNgram(texts, valid);
    auto actual = Scan(offsets, "(?i:error)");
    ASSERT_NE(actual, nullptr);
    EXPECT_TRUE(actual->Equals(*expected));
    EXPECT_EQ(probe->queries, 0);
    // A larger limit also uses the bounded prefix; these inputs are exhausted.
    options.limit = 129;
    ASSERT_NE(Scan(offsets, "(?i:error)"), nullptr);
    EXPECT_EQ(probe->queries, 0);
}

TEST_P(GrepScanTest, ProbeBudgetSwitchesOnceAndPreservesRepeatedPositions) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::VARCHAR)
        GTEST_SKIP() << "sealed VARCHAR probe route";
    std::vector<std::string> texts(1536, "ordinary");
    for (size_t i = 0; i < 127; ++i) {
        texts[i == 0 ? 0 : 256 + (i - 1) * 6] = "ERROR";
    }
    texts[1200] = "error";  // NULL must not supply the remaining match.
    texts[1400] = "error";
    std::vector<bool> valid(texts.size(), true);
    valid[1200] = false;
    std::vector<int64_t> offsets(texts.size());
    std::iota(offsets.begin(), offsets.end(), 0);
    offsets[1300] = 0;    // Same row again, but a distinct input position.
    offsets.back() = -1;  // Unread suffix after reaching the limit.
    options.limit = 128;
    options.batch_size = 128;
    Load(texts, valid, {600, 936});
    auto expected = Scan(offsets, "(?i:error)");
    ASSERT_NE(expected, nullptr);
    auto probe = AttachNgram(texts, valid);
    auto actual = Scan(offsets, "(?i:error)");
    ASSERT_NE(actual, nullptr);
    EXPECT_TRUE(actual->Equals(*expected));
    ASSERT_EQ(actual->num_rows(), 128);
    EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>(actual->column(0))
                  ->Value(127),
              1300);
    EXPECT_EQ(probe->queries, 1);
    EXPECT_NE(std::find(probe->input_rows.begin(), probe->input_rows.end(), 0),
              probe->input_rows.end());
    EXPECT_EQ(std::find(probe->input_rows.begin(), probe->input_rows.end(), 8),
              probe->input_rows.end());
    EXPECT_NE(
        std::find(probe->input_rows.begin(), probe->input_rows.end(), 1024),
        probe->input_rows.end());
    ASSERT_GE(probe->input_rows.size(), 2);
    EXPECT_EQ(probe->input_rows[1], 256);
}

TEST_P(GrepScanTest, ProbeByteBudgetStopsDespiteHighMatchRatio) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::VARCHAR)
        GTEST_SKIP() << "sealed VARCHAR probe";
    std::vector<std::string> texts(64, "error\n" + std::string(60000, 'x'));
    std::vector<bool> valid(texts.size(), true);
    std::vector<int64_t> offsets(texts.size());
    std::iota(offsets.begin(), offsets.end(), 0);
    options.limit = 64;
    options.batch_size = 128;
    Load(texts, valid);
    auto expected = Scan(offsets, "(?i:error)");
    ASSERT_NE(expected, nullptr);
    auto probe = AttachNgram(texts, valid);
    auto actual = Scan(offsets, "(?i:error)");
    ASSERT_NE(actual, nullptr);
    EXPECT_TRUE(actual->Equals(*expected));
    EXPECT_EQ(probe->queries, 1);
    ASSERT_FALSE(probe->input_rows.empty());
    EXPECT_EQ(probe->input_rows.front(), 4);
}

TEST_P(GrepScanTest, TextIndexFirstPreservesRepeatedLobRows) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::TEXT)
        GTEST_SKIP() << "sealed TEXT index route";
    const std::vector<std::string> texts = {
        "error\n" + std::string(2 * 1024 * 1024, 'x'), "ordinary", "ERROR"};
    const std::vector<bool> valid(texts.size(), true);
    ASSERT_NO_FATAL_FAILURE(LoadLobText(texts, valid));
    options.limit = 2;
    options.batch_size = 128;
    const std::vector<int64_t> offsets = {0, 1, 0, 2};
    auto expected = Scan(offsets, "(?i:error)");
    ASSERT_NE(expected, nullptr);
    ASSERT_EQ(expected->num_rows(), 2);
    EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>(expected->column(0))
                  ->Value(1),
              2);
    auto probe = AttachNgram(texts, valid);
    auto actual = Scan(offsets, "(?i:error)");
    ASSERT_NE(actual, nullptr);
    EXPECT_TRUE(actual->Equals(*expected))
        << "actual: " << actual->ToString()
        << " expected: " << expected->ToString();
    EXPECT_EQ(probe->queries, 1);
}

TEST_P(GrepScanTest, TextIndexFirstIncludesPrefixAfterShortInlineValue) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::TEXT)
        GTEST_SKIP() << "sealed TEXT index route";
    std::vector<std::string> texts(400, "ordinary");
    texts[0] = "error";
    texts[1] = std::string(400000, 'x') + "\nerror";
    texts[2] = std::string(400000, 'y') + "\nerror";
    const std::vector<bool> valid(texts.size(), true);
    ASSERT_NO_FATAL_FAILURE(LoadLobText(texts, valid));
    std::vector<int64_t> offsets(texts.size());
    std::iota(offsets.begin(), offsets.end(), 0);
    options.limit = 100;
    options.batch_size = 128;
    auto expected = Scan(offsets, "(?i:error)");
    ASSERT_NE(expected, nullptr);
    auto probe = AttachNgram(texts, valid);
    auto actual = Scan(offsets, "(?i:error)");
    ASSERT_NE(actual, nullptr);
    EXPECT_TRUE(actual->Equals(*expected));
    EXPECT_EQ(probe->queries, 1);
    ASSERT_FALSE(probe->input_rows.empty());
    EXPECT_EQ(probe->input_rows, offsets);
}

TEST_P(GrepScanTest, ProbeSwitchErrorsNeverReturnPartialResults) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::VARCHAR)
        GTEST_SKIP() << "sealed VARCHAR probe route";
    std::vector<std::string> texts(80, "ordinary");
    texts[0] = texts[63] = "error";
    std::vector<bool> valid(texts.size(), true);
    std::vector<int64_t> offsets(texts.size());
    std::iota(offsets.begin(), offsets.end(), 0);
    options.limit = 2;
    options.batch_size = 128;
    Load(texts, valid);
    auto probe = AttachNgram(texts, valid);
    probe->fail = true;
    Scan(offsets,
         "(?i:error)",
         DataFormatBroken,
         nullptr,
         {"injected Grep posting failure"});
    EXPECT_EQ(probe->queries, 1);
    ASSERT_FALSE(probe->input_rows.empty());
    EXPECT_EQ(probe->input_rows.front(), 32);
    probe->fail = false;
    folly::CancellationSource cancel;
    probe->cancel = &cancel;
    Scan(offsets, "(?i:error)", FollyCancel, &cancel);
    EXPECT_EQ(probe->queries, 2);
    probe->cancel = nullptr;
}

TEST_P(GrepScanTest, ProbeSampleSizeTracksLimitAndCapsAtAvailableInputs) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::VARCHAR)
        GTEST_SKIP() << "sealed VARCHAR probe route";
    std::vector<std::string> texts(600, "ordinary");
    texts.back() = "ERROR";
    std::vector<bool> valid(texts.size(), true);
    std::vector<int64_t> offsets(texts.size());
    std::iota(offsets.begin(), offsets.end(), 0);
    Load(texts, valid);
    auto probe = AttachNgram(texts, valid);
    options.batch_size = 17;
    for (const auto [limit, sample] :
         {std::pair<int64_t, int64_t>{1, 32},
          {10, 32},
          {32, 64},
          {100, 200},
          {128, 256},
          {129, 256},
          {std::numeric_limits<int64_t>::max(), 256}}) {
        options.limit = limit;
        const auto before = probe->queries;
        auto result = Scan(offsets, "(?i:error)");
        ASSERT_NE(result, nullptr);
        ASSERT_EQ(result->num_rows(), 1);
        EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>(result->column(0))
                      ->Value(0),
                  599);
        EXPECT_EQ(probe->queries - before, 1);
        ASSERT_FALSE(probe->input_rows.empty());
        EXPECT_EQ(probe->input_rows.front(), sample);
    }
    options.limit = 100;
    offsets.resize(40);
    const auto before = probe->queries;
    auto result = Scan(offsets, "(?i:error)");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->num_rows(), 0);
    EXPECT_EQ(probe->queries,
              before);  // Exhausted inputs need no index lookup.
}

TEST_P(GrepScanTest, ProbeLargerLimitFindsMatchesAfterEmptyPrefix) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::VARCHAR)
        GTEST_SKIP() << "sealed VARCHAR probe route";
    std::vector<std::string> texts(300, "ERROR");
    std::fill(texts.begin(), texts.begin() + 32, "ordinary");
    std::vector<bool> valid(texts.size(), true);
    std::vector<int64_t> offsets(texts.size());
    std::iota(offsets.begin(), offsets.end(), 0);
    options.limit = 100;
    options.batch_size = 128;
    Load(texts, valid);
    auto expected = Scan(offsets, "(?i:error)");
    auto probe = AttachNgram(texts, valid);
    auto result = Scan(offsets, "(?i:error)");
    ASSERT_NE(result, nullptr);
    EXPECT_TRUE(result->Equals(*expected));
    EXPECT_EQ(result->num_rows(), 100);
    EXPECT_EQ(probe->queries, 0);
}

TEST_P(GrepScanTest, ProbeAlwaysIndexesAfterRowBudget) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::VARCHAR)
        GTEST_SKIP() << "sealed VARCHAR probe route";
    for (const int64_t matches : {9, 10}) {
        std::vector<std::string> texts(300, "ordinary");
        for (int64_t i = 0; i < matches; ++i) {
            texts[1 + i * 17] = "error";
        }
        texts[250] = "ERROR";
        std::vector<bool> valid(texts.size(), true);
        std::vector<int64_t> offsets(texts.size());
        std::iota(offsets.begin(), offsets.end(), 0);
        // Both prefixes must switch after 200 rows, regardless of hit ratio.
        options.limit = 100;
        // Preserve original validation windows across batches.
        options.batch_size = 7;
        Load(texts, valid);
        auto expected = Scan(offsets, "(?i:error)");
        auto probe = AttachNgram(texts, valid);
        auto actual = Scan(offsets, "(?i:error)");
        ASSERT_NE(actual, nullptr);
        EXPECT_TRUE(actual->Equals(*expected));
        EXPECT_EQ(actual->num_rows(), matches + 1);
        EXPECT_EQ(probe->queries, 1);
        ASSERT_FALSE(probe->input_rows.empty());
        EXPECT_EQ(probe->input_rows.front(), 200);
    }
}

TEST_P(GrepScanTest, ProbeSupportsLiteralAndConditionsAndLargeLimits) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::VARCHAR)
        GTEST_SKIP() << "sealed VARCHAR probe route";
    const std::vector<std::string> texts(300, "error timeout");
    const std::vector<bool> valid(texts.size(), true);
    std::vector<int64_t> offsets(texts.size());
    std::iota(offsets.begin(), offsets.end(), 0);
    Load(texts, valid);
    auto probe = AttachNgram(texts, valid);
    for (const auto* pattern : {"error", "error.*timeout", "(?i:error)"}) {
        for (const int64_t limit : {1, 10, 100, 129, 256, 257, 10000}) {
            SCOPED_TRACE(pattern);
            SCOPED_TRACE(limit);
            options.limit = limit;
            const auto before = probe->queries;
            auto result = Scan(offsets, pattern);
            ASSERT_NE(result, nullptr);
            EXPECT_EQ(result->num_rows(), std::min<int64_t>(limit, 300));
            EXPECT_EQ(probe->queries - before, limit > 256 ? 1 : 0);
            if (limit > 256) {
                ASSERT_FALSE(probe->input_rows.empty());
                EXPECT_EQ(probe->input_rows.front(), 256);
            }
        }
    }
}

TEST_P(GrepScanTest, ProbeByteBudgetPreservesUnscannedInputSuffix) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::VARCHAR)
        GTEST_SKIP() << "sealed VARCHAR probe route";
    for (const size_t length : {size_t{65536}, size_t{60000}, size_t{300000}}) {
        for (const int64_t batch_size : {1, 17, 128}) {
            SCOPED_TRACE(length);
            SCOPED_TRACE(batch_size);
            std::vector<std::string> texts(6, std::string(length, 'x'));
            texts.back() = "error";
            const std::vector<bool> valid(texts.size(), true);
            const std::vector<int64_t> offsets = {0, 1, 2, 3, 4, 5};
            options.limit = 100;
            options.batch_size = batch_size;
            Load(texts, valid);
            auto expected = Scan(offsets, "error");
            auto probe = AttachNgram(texts, valid);
            auto result = Scan(offsets, "error");
            ASSERT_NE(result, nullptr);
            ASSERT_TRUE(result->Equals(*expected));
            EXPECT_EQ(probe->queries, 1);
            ASSERT_FALSE(probe->input_rows.empty());
            // Exact exhaustion, a value exceeding the remaining budget, and
            // a first value exceeding the whole budget must all make progress.
            EXPECT_EQ(probe->input_rows.front(), length > 262144 ? 0 : 4);
            EXPECT_EQ(probe->input_rows.back(), 5);
        }
    }
}

TEST_P(GrepScanTest, ProbeRowBudgetCountsNullEmptyAndRepeatedInputs) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::VARCHAR)
        GTEST_SKIP() << "sealed VARCHAR probe route";
    Load({"", "", "error"}, {false, true, true});
    std::vector<int64_t> offsets;
    for (int64_t i = 0; i < 32; ++i) offsets.push_back(i % 2);
    offsets.push_back(2);
    options.limit = 1;
    auto probe = AttachNgram({"", "", "error"}, {false, true, true});
    auto result = Scan(offsets, "error");
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->num_rows(), 1);
    EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>(result->column(0))
                  ->Value(0),
              32);
    EXPECT_EQ(probe->queries, 1);
    EXPECT_EQ(probe->input_rows, (std::vector<int64_t>{2}));
}

TEST_P(GrepScanTest, ProbeKeepsOriginalOffsetValidationWindows) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::VARCHAR)
        GTEST_SKIP() << "sealed VARCHAR probe route";
    Load({"error", "ordinary"}, {true, true});
    auto probe = AttachNgram({"error", "ordinary"}, {true, true});
    options.limit = 1;
    options.batch_size = 2;
    Scan({0, -1}, "(?i:error)", UnexpectedError);
    EXPECT_EQ(probe->queries, 0);
    options.batch_size = 1;
    auto result = Scan({0, -1}, "(?i:error)");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->num_rows(), 1);
    EXPECT_EQ(probe->queries, 0);
    options.max_result_bytes = 1;
    Scan({0}, "(?i:error)", InvalidParameter);
}

TEST_P(GrepScanTest, TextIndexPrunesUnreadableLobForWideOr) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::TEXT)
        GTEST_SKIP() << "sealed TEXT index read avoidance";
    using namespace milvus_storage::lob_column;
    const auto first = EncodeInlineText("error");
    const uint8_t uuid[16] = {};
    const auto missing = EncodeLOBReference(uuid, 0);
    Load(
        {std::string(reinterpret_cast<const char*>(first.data()), first.size()),
         std::string(reinterpret_cast<const char*>(missing.data()),
                     missing.size())},
        {true, true},
        {},
        false);
    auto probe = AttachNgram({"error", "ordinary"}, {true, true});
    options.limit = 1;
    options.batch_size = 128;
    auto result = Scan({0, 1}, "(?i:error)");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->num_rows(), 1);
    EXPECT_EQ(probe->queries, 1);

    options.limit = 2;
    result = Scan({0, 1}, "(?i:error)");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->num_rows(), 1);
    EXPECT_EQ(probe->queries, 2);
    EXPECT_EQ(probe->input_rows, (std::vector<int64_t>{0, 1}));
}

TEST_P(GrepScanTest, TextIndexFirstPreservesFullLobMatchingDetails) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::TEXT)
        GTEST_SKIP() << "sealed TEXT index route";
    const std::vector<std::string> texts = {
        std::string(80 * 1024, 'x') + "\nERROR file", "error inline"};
    const std::vector<bool> valid = {true, true};
    ASSERT_NO_FATAL_FAILURE(LoadLobText(texts, valid));
    options.limit = 1;
    options.batch_size = 128;
    auto expected = Scan({0, 1}, "(?i:error)");
    ASSERT_NE(expected, nullptr);
    auto probe = AttachNgram(texts, valid);
    auto actual = Scan({0, 1}, "(?i:error)");
    ASSERT_NE(actual, nullptr);
    EXPECT_TRUE(actual->Equals(*expected));
    EXPECT_EQ(probe->queries, 1);
}

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
        std::static_pointer_cast<arrow::BinaryArray>(result->column(1));
    auto lines = nlohmann::json::parse(details->GetString(0));
    EXPECT_EQ(lines[0]["content"], "超时 timeout");
    EXPECT_EQ(lines[0]["starts"], nlohmann::json::array({7}));
    EXPECT_EQ(lines[0]["ends"], nlohmann::json::array({14}));
    auto logical =
        result->schema()->field(1)->metadata()->Get("milvus.data_type");
    ASSERT_TRUE(logical.ok());
    EXPECT_EQ(logical.ValueOrDie(),
              std::to_string(static_cast<int32_t>(DataType::JSON)));
}

TEST_P(GrepScanTest, OriginalLineCoordinates) {
    Load({"中文\r\n\nfoo\nskip\nfoo", "foo\n"}, {true, true});
    options.batch_size = 1;
    auto result = Scan({0, 1}, "foo");
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->num_rows(), 2);
    auto details =
        std::static_pointer_cast<arrow::BinaryArray>(result->column(1));
    EXPECT_EQ(
        nlohmann::json::parse(details->GetString(0)),
        nlohmann::json::parse(
            R"([{"content":"foo","line_number":3,"line_start_offset":9,"starts":[0],"ends":[3]},{"content":"foo","line_number":5,"line_start_offset":18,"starts":[0],"ends":[3]}])"));
    EXPECT_EQ(
        nlohmann::json::parse(details->GetString(1)),
        nlohmann::json::parse(
            R"([{"content":"foo","line_number":1,"line_start_offset":0,"starts":[0],"ends":[3]}])"));
}

TEST_P(GrepScanTest, JSONDocumentsAndSpans) {
    Load({"foo bar foo\nhello foo", "no match", "foo"}, {true, true, true});
    options.batch_size = 1;
    auto result = Scan({0, 1, 2}, "foo");
    ASSERT_NE(result, nullptr);
    ASSERT_TRUE(result->ValidateFull().ok());
    ASSERT_EQ(result->num_rows(), 2);
    ASSERT_EQ(result->column(1)->type_id(), arrow::Type::BINARY);
    auto details =
        std::static_pointer_cast<arrow::BinaryArray>(result->column(1));
    EXPECT_EQ(
        nlohmann::json::parse(details->GetString(0)),
        nlohmann::json::parse(
            R"([{"content":"foo bar foo","line_number":1,"line_start_offset":0,"starts":[0,8],"ends":[3,11]},{"content":"hello foo","line_number":2,"line_start_offset":12,"starts":[6],"ends":[9]}])"));
    EXPECT_EQ(
        nlohmann::json::parse(details->GetString(1)),
        nlohmann::json::parse(
            R"([{"content":"foo","line_number":1,"line_start_offset":0,"starts":[0],"ends":[3]}])"));
    auto empty = Scan({1}, "foo");
    ASSERT_NE(empty, nullptr);
    EXPECT_EQ(empty->num_rows(), 0);
    EXPECT_TRUE(empty->schema()->Equals(result->schema()));
    EXPECT_TRUE(empty->ValidateFull().ok());
}

TEST_P(GrepScanTest, JSONEscapingAndEncodedBudget) {
    const std::string text = "\"foo\"\\\t";
    Load({text}, {true});
    auto result = Scan({0}, "foo");
    ASSERT_NE(result, nullptr);
    auto details =
        std::static_pointer_cast<arrow::BinaryArray>(result->column(1));
    auto lines = nlohmann::json::parse(details->GetString(0));
    EXPECT_EQ(lines[0]["content"], text);
    EXPECT_EQ(lines[0]["starts"], nlohmann::json::array({1}));
    EXPECT_EQ(lines[0]["ends"], nlohmann::json::array({4}));
    // Each control character becomes six JSON bytes. Charge the encoded size,
    // not just the original text length or the native matching payload.
    Load({std::string(256, '\1') + "foo"}, {true});
    options.max_result_bytes = 1024;
    Scan({0},
         "foo",
         InvalidParameter,
         nullptr,
         {"maxOutputSize=1024 bytes", "actual="});
    options.max_result_bytes = 1048576;
    auto recovered = Scan({0}, "foo");
    ASSERT_NE(recovered, nullptr);
    EXPECT_EQ(recovered->num_rows(), 1);
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
        auto details =
            std::static_pointer_cast<arrow::BinaryArray>(result->column(1));
        int64_t row = 0;
        for (size_t i = 0; i < offsets.size(); ++i) {
            if (!valid[offsets[i]] || offsets[i] == 120)
                continue;
            ASSERT_LT(row, result->num_rows());
            EXPECT_EQ(positions->Value(row), i);
            auto lines = nlohmann::json::parse(details->GetString(row));
            ASSERT_EQ(lines.size(), 1);
            EXPECT_EQ(lines[0]["content"], texts[offsets[i]]);
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
    auto raw = Scan({4, 1, 0, 3, 2, 4});
    auto probe = AttachNgram(texts, {true, true, true, false, true});
    for (auto batch_size : {1, 2, 128}) {
        options.batch_size = batch_size;
        auto result = Scan({4, 1, 0, 3, 2, 4});
        ASSERT_NE(result, nullptr);
        ASSERT_TRUE(result->Equals(*raw));
        auto positions =
            std::static_pointer_cast<arrow::Int64Array>(result->column(0));
        ASSERT_EQ(result->num_rows(), 4)
            << "batch_size=" << batch_size
            << " positions=" << result->column(0)->ToString()
            << " details=" << result->column(1)->ToString();
        const std::vector<int64_t> expected_positions = {0, 1, 2, 5};
        auto details =
            std::static_pointer_cast<arrow::BinaryArray>(result->column(1));
        const std::vector<std::string> expected = {
            "超时 timeout", "timeout file", "timeout inline", "超时 timeout"};
        for (int64_t i = 0; i < result->num_rows(); ++i) {
            EXPECT_EQ(positions->Value(i), expected_positions[i]);
            auto lines = nlohmann::json::parse(details->GetString(i));
            ASSERT_EQ(lines.size(), 1);
            EXPECT_EQ(lines[0]["content"], expected[i]);
        }
    }
}

TEST_P(GrepScanTest, NgramSkipsRejectedLobReadsAndEmptyCandidates) {
    if (std::get<0>(GetParam()) || std::get<1>(GetParam()) != DataType::TEXT)
        GTEST_SKIP() << "sealed TEXT read avoidance";
    using namespace milvus_storage::lob_column;
    const auto inline_value = EncodeInlineText("timeout");
    const uint8_t uuid[16] = {};
    const auto file_ref = EncodeLOBReference(uuid, 0);
    Load({std::string(reinterpret_cast<const char*>(inline_value.data()),
                      inline_value.size()),
          std::string(reinterpret_cast<const char*>(file_ref.data()),
                      file_ref.size())},
         {true, true},
         {},
         false);
    auto probe = AttachNgram({"timeout", "ordinary"}, {true, true});
    options.limit = 100;
    options.batch_size = 128;
    auto hit = Scan({1, 0, 1, 0}, "timeout");
    ASSERT_NE(hit, nullptr);
    ASSERT_EQ(hit->num_rows(), 2);
    auto positions =
        std::static_pointer_cast<arrow::Int64Array>(hit->column(0));
    EXPECT_EQ(positions->Value(0), 1);
    EXPECT_EQ(positions->Value(1), 3);
    ASSERT_EQ(Scan({1, 0, 1}, "absent_phrase")->num_rows(), 0);
    EXPECT_EQ(probe->queries, 2);
    // A true condition cannot prune the missing file and must propagate its read failure.
    ArrowSchema schema{};
    ArrowArray array{};
    const int64_t offsets[] = {1};
    const std::string pattern = ".*";
    uint64_t length = pattern.size();
    auto status =
        ScanGrepByOffsets(segment.get(),
                          offsets,
                          1,
                          &options,
                          reinterpret_cast<const uint8_t*>(pattern.data()),
                          length,
                          &length,
                          1,
                          &schema,
                          &array,
                          nullptr);
    EXPECT_NE(status.error_code, Success);
    EXPECT_EQ(schema.release, nullptr);
    EXPECT_EQ(array.release, nullptr);
    free(const_cast<char*>(status.error_msg));
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

TEST_P(GrepScanTest, MaterializationBudgetUsesRealTextSizes) {
    if (!std::get<0>(GetParam())) {
        GTEST_SKIP() << "Legacy binlog fixture lacks size statistics; sealed "
                        "metadata is covered by TestLazyManifest";
    }
    Load({std::string(4096, 'x'), std::string(4096, 'y')}, {true, true});
    query::RetrievePlan plan(schema);
    plan.field_ids_ = {FieldId(101), TimestampFieldID};
    const auto average = segment->get_fields_avg_size(plan.field_ids_);
    ASSERT_EQ(average, 4096 + sizeof(int64_t));
    CSegmentInterface c_segment = segment.get();
    int32_t index = 0;
    int64_t offset = 0;
    ArrowSchema out_schema{};
    ArrowArray out_array{};
    auto status = FillRetrieveFieldsOrdered(&c_segment,
                                            1,
                                            &plan,
                                            &index,
                                            &offset,
                                            1,
                                            512,
                                            &out_schema,
                                            &out_array,
                                            nullptr);
    EXPECT_EQ(status.error_code, InvalidParameter);
    EXPECT_EQ(out_schema.release, nullptr);
    EXPECT_EQ(out_array.release, nullptr);
    if (status.error_code != Success) {
        free(const_cast<char*>(status.error_msg));
    }
    // Only the selected row counts, not all rows in this segment.
    status = FillRetrieveFieldsOrdered(&c_segment,
                                       1,
                                       &plan,
                                       &index,
                                       &offset,
                                       1,
                                       average,
                                       &out_schema,
                                       &out_array,
                                       nullptr);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    auto imported = arrow::ImportRecordBatch(&out_array, &out_schema);
    ASSERT_TRUE(imported.ok()) << imported.status().ToString();
    EXPECT_EQ((*imported)->num_rows(), 1);
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
    Scan({2}, "timeout", InvalidParameter);
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
         InvalidParameter,
         nullptr,
         {"function.grep.maxRegexBytes=1 bytes",
          "compiled regex exceeds size limit of 1"});
    options.max_regex_bytes = 1048576;
    options.max_result_bytes = 1;
    Scan({2},
         "timeout",
         InvalidParameter,
         nullptr,
         {"quotaAndLimits.limits.maxOutputSize=1 bytes",
          "resource limit exceeded: ResultBytes"});
    options.max_result_bytes = 128;
    Scan({2},
         "timeout",
         InvalidParameter,
         nullptr,
         {"quotaAndLimits.limits.maxOutputSize=128 bytes",
          "resource limit exceeded: ResultBytes"});
}

TEST_P(GrepScanTest, CumulativeResultBudget) {
    options.limit = 100;
    options.max_result_bytes = 1024;
    Scan(std::vector<int64_t>(100, 2), "timeout", InvalidParameter);
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
