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
#include <arrow/api.h>

#include <optional>
#include <string>
#include <vector>

#include "index/contracts/query/INgramReader.h"
#include "index/contracts/query/INullReader.h"
#include "indexbuilder/test_utils/SourceBuildTestUtils.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/lob_column/lob_column_manager.h"
#include "milvus-storage/lob_column/lob_column_writer.h"
#include "milvus-storage/lob_column/lob_reference.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/transaction/transaction.h"
#include "milvus-storage/writer.h"
#include "storage/loon_ffi/util.h"

namespace milvus::indexbuilder::test {
namespace {

using TextRows = std::vector<std::optional<std::string>>;

class NgramTextBuildSessionTest : public SourceBuildTest {
 protected:
    // Persist encoded inline cells and actual Vortex LOBs, rather than passing
    // already-decoded strings straight to the index builder.
    void
    WriteManifest(PreparedBuild& prepared, const TextRows& rows) const {
        namespace lob = milvus_storage::lob_column;
        const auto partition = directory_->Path() + "/source-" +
                               std::to_string(static_cast<int>(
                                   prepared.request.output.generation)) +
                               "/1/2";
        const auto base = partition + "/3";
        // Prepare() starts with a V1 binlog source, so it has no manifest
        // properties. Supply the local manifest transport explicitly here.
        milvus_storage::api::Properties properties;
        milvus_storage::api::SetValue(
            properties, PROPERTY_FS_STORAGE_TYPE, LOON_FS_TYPE_LOCAL);
        milvus_storage::api::SetValue(
            properties, PROPERTY_FS_ROOT_PATH, kLoonLocalFSRootPath);
        ASSERT_FALSE(
            milvus_storage::api::SetValue(
                properties, PROPERTY_READER_RECORD_BATCH_MAX_ROWS, "1024")
                .has_value());
        prepared.file_manager_context.loon_ffi_properties =
            std::make_shared<milvus_storage::api::Properties>(properties);
        const auto& fs = prepared.file_manager_context.fs;
        lob::LobColumnConfig config;
        config.field_id = 100;
        config.data_type = lob::LobDataType::kText;
        config.lob_base_path = partition + "/lobs/100";
        config.inline_threshold = 64 * 1024;
        config.flush_threshold_bytes = 64 * 1024;
        config.max_lob_file_bytes = 256 * 1024;
        config.properties = prepared.file_manager_context.loon_ffi_properties;
        auto manager_result = lob::LobColumnManager::Create(fs, config);
        ASSERT_TRUE(manager_result.ok()) << manager_result.status();
        auto manager = std::move(manager_result).ValueOrDie();
        auto writer_result = manager->CreateWriter();
        ASSERT_TRUE(writer_result.ok()) << writer_result.status();
        auto lob_writer = std::move(writer_result).ValueOrDie();

        arrow::BinaryBuilder cells;
        size_t external_rows = 0;
        for (const auto& text : rows) {
            if (!text) {
                ASSERT_TRUE(cells.AppendNull().ok());
                continue;
            }
            auto encoded = lob_writer->WriteText(*text);
            ASSERT_TRUE(encoded.ok()) << encoded.status();
            const auto& ref = encoded.ValueOrDie();
            ASSERT_FALSE(ref.empty());
            if (text->size() > config.inline_threshold) {
                ASSERT_TRUE(lob::IsLOBReference(ref.data()));
                ASSERT_EQ(ref.size(), lob::LOB_REFERENCE_SIZE);
                ++external_rows;
            } else {
                ASSERT_TRUE(lob::IsInlineData(ref.data()));
            }
            ASSERT_TRUE(cells.Append(ref.data(), ref.size()).ok());
        }
        auto closed_lobs = lob_writer->Close();
        ASSERT_TRUE(closed_lobs.ok()) << closed_lobs.status();
        if (external_rows != 0) {
            ASSERT_FALSE(closed_lobs.ValueOrDie().empty());
        }
        auto array_result = cells.Finish();
        ASSERT_TRUE(array_result.ok()) << array_result.status();
        auto values = std::move(array_result).ValueOrDie();
        auto schema = arrow::schema(
            {arrow::field("100",
                          arrow::binary(),
                          true,
                          arrow::key_value_metadata(
                              {milvus_storage::ARROW_FIELD_ID_KEY}, {"100"}))});
        milvus_storage::api::SetValue(properties,
                                      PROPERTY_WRITER_POLICY,
                                      LOON_COLUMN_GROUP_POLICY_SCHEMA_BASED);
        milvus_storage::api::SetValue(
            properties, PROPERTY_WRITER_SCHEMA_BASE_PATTERNS, "100");
        auto writer_properties =
            std::make_shared<const milvus_storage::api::Properties>(properties);
        auto policy =
            milvus_storage::api::ColumnGroupPolicy::create_column_group_policy(
                writer_properties, schema);
        ASSERT_TRUE(policy.ok()) << policy.status();
        auto writer = milvus_storage::api::Writer::create(
            base, schema, std::move(policy).ValueOrDie(), writer_properties);
        for (int64_t start = 0; start < values->length(); start += 513) {
            auto slice = values->Slice(start, 513);
            auto status = writer->write(
                arrow::RecordBatch::Make(schema, slice->length(), {slice}));
            ASSERT_TRUE(status.ok()) << status;
        }
        auto groups = writer->close();
        ASSERT_TRUE(groups.ok()) << groups.status();
        auto transaction =
            milvus_storage::api::transaction::Transaction::Open(fs, base);
        ASSERT_TRUE(transaction.ok()) << transaction.status();
        auto txn = std::move(transaction).ValueOrDie();
        txn->AppendFiles(*groups.ValueOrDie());
        auto committed = txn->Commit();
        ASSERT_TRUE(committed.ok()) << committed.status();
        prepared.request.source = ManifestBuildSource{Config{
            {"base_path", base},
            {"ver", committed.ValueOrDie()}}.dump()};
    }

    void
    CheckReader(const index::IIndexReaderBase& reader,
                const TextRows& rows,
                const std::vector<std::string>& literals) const {
        ASSERT_EQ(reader.ValueType(), DataType::TEXT);
        ASSERT_EQ(reader.CoordDomain(), index::Domain::Row);
        ASSERT_EQ(reader.Count(), rows.size());
        EXPECT_TRUE(reader.Caps().ngram_candidates);
        EXPECT_FALSE(reader.Caps().exact);
        EXPECT_FALSE(reader.Caps().text_match);
        EXPECT_FALSE(reader.Caps().value_lookup);
        auto* nulls = dynamic_cast<const index::INullReader*>(&reader);
        ASSERT_NE(nulls, nullptr);
        const auto is_null = nulls->IsNull();
        const auto is_not_null = nulls->IsNotNull();
        ASSERT_EQ(is_null.size(), rows.size());
        ASSERT_EQ(is_not_null.size(), rows.size());
        for (size_t row = 0; row < rows.size(); ++row) {
            EXPECT_EQ(is_null[row], !rows[row].has_value()) << row;
            EXPECT_EQ(is_not_null[row], rows[row].has_value()) << row;
        }
        auto* ngram = dynamic_cast<const index::INgramReader*>(&reader);
        ASSERT_NE(ngram, nullptr);
        for (const auto& literal : literals) {
            SCOPED_TRACE(::testing::PrintToString(literal));
            ASSERT_TRUE(
                ngram->CanHandle(literal, index::PatternOp::InnerMatch));
            for (bool sparse : {false, true}) {
                SCOPED_TRACE(sparse);
                TargetBitmap candidates(rows.size(), false);
                for (size_t row = 0; row < rows.size(); ++row) {
                    candidates[row] = !sparse || row % 128 == 0 || row == 3 ||
                                      row == 5 || row == 6 || row == 1025;
                }
                const auto initial = candidates.clone();
                ngram->Candidates(
                    literal, index::PatternOp::InnerMatch, candidates);
                TargetBitmap expected(rows.size(), false);
                auto refined = candidates.clone();
                for (size_t row = 0; row < rows.size(); ++row) {
                    const bool matches =
                        rows[row] &&
                        rows[row]->find(literal) != std::string::npos;
                    expected[row] = initial[row] && matches;
                    EXPECT_FALSE(candidates[row] && !initial[row]) << row;
                    if (expected[row]) {
                        EXPECT_TRUE(candidates[row]) << "missing row " << row;
                    }
                    if (!matches) {
                        refined[row] = false;
                    }
                }
                EXPECT_TRUE(refined == expected);
                if (ngram->CandidatesAreExact(literal,
                                              index::PatternOp::InnerMatch)) {
                    EXPECT_TRUE(candidates == expected);
                }
            }
        }
    }
    void
    CheckManifestRoundTrip(int version);
};

void
NgramTextBuildSessionTest::CheckManifestRoundTrip(int version) {
    // More than the configured TEXT reader batch size: row 1024 must not become
    // row zero when the next decoded batch is fed to the scalar materializer.
    TextRows rows(1030, "repeated ordinary text");
    rows[0] = std::nullopt;
    rows[1] = "";
    rows[3] = std::string(80 * 1024, 'x') + "uniquelobtail";
    rows[4] = "MixedCase 中文字符\nnextline";
    rows[5] = std::string("before\0after", 12);
    rows[6] = "MixedCase 中文字符\nnextline";
    rows[1023] = std::nullopt;
    rows[1024] = "secondbatchmarker";
    rows[1025] = std::string(300 * 1024, 'y') + "lastlobtail";
    rows[1029] = std::nullopt;
    const std::vector<std::string> literals{"uniquelobtail",
                                            "lastlobtail",
                                            "secondbatchmarker",
                                            "repeated",
                                            "MixedCase",
                                            "mixedcase",
                                            "Case",
                                            "case",
                                            "中文字符",
                                            "符\nne",
                                            std::string("re\0af", 5),
                                            "missingliteral",
                                            "xxx"};

    SCOPED_TRACE(version);
    auto prepared = Prepare(DataType::TEXT,
                            DataType::NONE,
                            true,
                            index::NGRAM_INDEX_TYPE,
                            rows.size(),
                            1,
                            {{index::MIN_GRAM, 2}, {index::MAX_GRAM, 4}},
                            version);
    ASSERT_EQ(prepared.request.family, index::families::kNgram);
    ASSERT_EQ(prepared.request.value_type, DataType::TEXT);
    ASSERT_EQ(prepared.request.output.storage_namespace,
              storage::ArtifactStorageNamespace::Index);
    ASSERT_FALSE(prepared.request.params.contains("is_text_match"));
    ASSERT_NO_FATAL_FAILURE(WriteManifest(prepared, rows));
    const auto& manifest =
        std::get<ManifestBuildSource>(prepared.request.source).manifest_path;
    size_t decoded_rows = 0;
    size_t batches = 0;
    const auto outcome = storage::VisitTextFieldDataFromManifest(
        manifest,
        prepared.file_manager_context.loon_ffi_properties,
        prepared.file_manager_context.fieldDataMeta,
        [&](FieldDataPtr batch) {
            ++batches;
            for (int64_t row = 0; row < batch->get_num_rows(); ++row) {
                EXPECT_LT(decoded_rows, rows.size());
                if (decoded_rows < rows.size()) {
                    EXPECT_EQ(batch->is_valid(row),
                              rows[decoded_rows].has_value());
                    if (batch->is_valid(row) && rows[decoded_rows]) {
                        const auto* value = static_cast<const std::string*>(
                            batch->RawValue(row));
                        EXPECT_EQ(*value, *rows[decoded_rows]);
                    }
                }
                ++decoded_rows;
            }
            return storage::VisitControl::Continue;
        });
    ASSERT_EQ(outcome, storage::VisitOutcome::Exhausted);
    ASSERT_EQ(decoded_rows, rows.size());
    ASSERT_GT(batches, 1);
    const auto stats = Publish(prepared);
    ASSERT_FALSE(stats.Files().empty());
    for (bool mmap : {false, true}) {
        SCOPED_TRACE(mmap);
        auto reader = Open(prepared, stats, mmap);
        ASSERT_NE(reader, nullptr);
        ASSERT_NO_FATAL_FAILURE(CheckReader(*reader, rows, literals));
    }
}

TEST_F(NgramTextBuildSessionTest, LegacyManifestLobRoundTrip) {
    ASSERT_NO_FATAL_FAILURE(CheckManifestRoundTrip(1));
}

TEST_F(NgramTextBuildSessionTest, PackedManifestLobRoundTrip) {
    ASSERT_NO_FATAL_FAILURE(CheckManifestRoundTrip(3));
}

TEST_F(NgramTextBuildSessionTest, AllNullManifestRetainsRowDomain) {
    const TextRows rows(1030, std::nullopt);
    auto prepared = Prepare(DataType::TEXT,
                            DataType::NONE,
                            true,
                            index::NGRAM_INDEX_TYPE,
                            rows.size(),
                            1,
                            {{index::MIN_GRAM, 3}, {index::MAX_GRAM, 3}});
    ASSERT_NO_FATAL_FAILURE(WriteManifest(prepared, rows));
    const auto stats = Publish(prepared);
    ASSERT_FALSE(stats.Files().empty());
    for (bool mmap : {false, true}) {
        auto reader = Open(prepared, stats, mmap);
        ASSERT_NE(reader, nullptr);
        ASSERT_NO_FATAL_FAILURE(CheckReader(*reader, rows, {"null", "xxx"}));
    }
}

TEST_F(NgramTextBuildSessionTest, EmptyTextSourceRoundTrip) {
    auto prepared = Prepare(DataType::TEXT,
                            DataType::NONE,
                            true,
                            index::NGRAM_INDEX_TYPE,
                            0,
                            1,
                            {{index::MIN_GRAM, 3}, {index::MAX_GRAM, 3}});
    prepared.request.source = V1BinlogBuildSource{};
    const auto stats = Publish(prepared);
    ASSERT_FALSE(stats.Files().empty());
    for (bool mmap : {false, true}) {
        auto reader = Open(prepared, stats, mmap);
        ASSERT_NE(reader, nullptr);
        ASSERT_NO_FATAL_FAILURE(CheckReader(*reader, {}, {"empty"}));
    }
}

}  // namespace
}  // namespace milvus::indexbuilder::test
