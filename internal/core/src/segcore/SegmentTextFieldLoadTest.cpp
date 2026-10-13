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

#include <arrow/api.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "index/contracts/query/INullReader.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/lob_column/lob_column_manager.h"
#include "milvus-storage/lob_column/lob_column_writer.h"
#include "milvus-storage/lob_column/lob_reference.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/transaction/transaction.h"
#include "milvus-storage/writer.h"
#include "segcore/default_fs.h"
#include "segcore/test_utils/TextMatchTestUtils.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/loon_ffi/util.h"

namespace milvus::segcore {
namespace {
using namespace text_test;

FieldDataPtr
TextBatch(const std::vector<std::string>& texts,
          const std::vector<bool>& valid,
          DataType type = DataType::VARCHAR) {
    auto field =
        storage::CreateFieldData(type, DataType::NONE, true, 1, texts.size());
    std::vector<uint8_t> bytes((texts.size() + 7) / 8, 0);
    for (size_t row = 0; row < texts.size(); ++row) {
        if (valid[row])
            bytes[row / 8] |= uint8_t{1} << (row % 8);
    }
    field->FillFieldData(texts.data(), bytes.data(), texts.size(), 0);
    return field;
}

void
ExpectNullRows(const index::IIndexReaderBase& reader,
               const std::vector<bool>& valid) {
    const auto* nulls = dynamic_cast<const index::INullReader*>(&reader);
    ASSERT_NE(nulls, nullptr);
    const auto is_null = nulls->IsNull();
    const auto is_not_null = nulls->IsNotNull();
    ASSERT_EQ(is_null.size(), valid.size());
    ASSERT_EQ(is_not_null.size(), valid.size());
    for (size_t row = 0; row < valid.size(); ++row) {
        EXPECT_EQ(is_null[row], !valid[row]) << row;
        EXPECT_EQ(is_not_null[row], valid[row]) << row;
    }
}

TEST(SegmentTextFieldLoadTest, MultipleFieldDataBatchesKeepGlobalNullOffsets) {
    auto schema = GenTestSchema({}, true);
    auto data = DataGen(schema, 8);
    auto manager =
        storage::CreateChunkManager(get_default_local_storage_config());
    auto info = PrepareInsertBinlog(1, 2, 3, data, manager);
    info.field_infos.erase(101);
    AddBinlog(info,
              data.binlogs->Path(),
              101,
              {TextBatch({"hello", "", "world"}, {true, false, true}),
               TextBatch({"", "foo", ""}, {false, true, false}),
               TextBatch({"bar", ""}, {true, false})},
              manager);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->LoadFieldData(info);
    auto pin = segment->PinGrowingIndex(FieldId(101));
    ASSERT_TRUE(static_cast<bool>(pin));
    EXPECT_EQ(pin.CoveredRowEnd(), 8);
    ExpectNullRows(pin.Reader(),
                   {true, false, true, false, true, false, true, false});
    const auto& text = TextReader(pin.Reader());
    ExpectOnlyTextMatchHit(text, "hello", 0, 8);
    ExpectOnlyTextMatchHit(text, "foo", 4, 8);
    ExpectOnlyTextMatchHit(text, "bar", 6, 8);
}

TEST(SegmentTextFieldLoadTest, SingleNullableFieldDataBatchPreservesRows) {
    auto schema = GenTestSchema({}, true);
    auto data = DataGen(schema, 4);
    auto manager =
        storage::CreateChunkManager(get_default_local_storage_config());
    auto info = PrepareInsertBinlog(1, 2, 3, data, manager);
    info.field_infos.erase(101);
    AddBinlog(
        info,
        data.binlogs->Path(),
        101,
        {TextBatch({"alpha", "", "beta", ""}, {true, false, true, false})},
        manager);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->LoadFieldData(info);
    auto pin = segment->PinGrowingIndex(FieldId(101));
    ASSERT_TRUE(static_cast<bool>(pin));
    ExpectNullRows(pin.Reader(), {true, false, true, false});
    ExpectOnlyTextMatchHit(TextReader(pin.Reader()), "alpha", 0, 4);
    ExpectOnlyTextMatchHit(TextReader(pin.Reader()), "beta", 2, 4);
}

void
CheckLoadedSuffix(const std::vector<std::string>& inserted,
                  const std::vector<std::string>& loaded,
                  const std::vector<bool>& valid) {
    auto schema = GenTestSchema({}, true);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto first = DataGen(schema, inserted.size());
    auto* strings = first.raw_->mutable_fields_data(1)
                        ->mutable_scalars()
                        ->mutable_string_data();
    for (size_t row = 0; row < inserted.size(); ++row)
        strings->set_data(row, inserted[row]);
    const auto offset = segment->PreInsert(inserted.size());
    ASSERT_EQ(offset, 0);
    segment->Insert(offset,
                    inserted.size(),
                    first.row_ids_.data(),
                    first.timestamps_.data(),
                    first.owner);

    auto second = DataGen(schema, loaded.size(), 0, inserted.size());
    auto manager =
        storage::CreateChunkManager(get_default_local_storage_config());
    auto info = PrepareInsertBinlog(1, 2, 3, second, manager);
    info.field_infos.erase(101);
    AddBinlog(
        info, second.binlogs->Path(), 101, {TextBatch(loaded, valid)}, manager);
    // LoadFieldData reserves and publishes the complete range. It exercises
    // the raw-column adapter and growing-index feed at the same offset.
    segment->LoadFieldData(info);
    auto pin = segment->PinGrowingIndex(FieldId(101));
    ASSERT_TRUE(static_cast<bool>(pin));
    ASSERT_EQ(pin.CoveredRowEnd(), inserted.size() + loaded.size());
    auto all_valid = std::vector<bool>(inserted.size(), true);
    all_valid.insert(all_valid.end(), valid.begin(), valid.end());
    ExpectNullRows(pin.Reader(), all_valid);
    for (size_t row = 0; row < inserted.size(); ++row) {
        ExpectOnlyTextMatchHit(
            TextReader(pin.Reader()), inserted[row], row, all_valid.size());
    }
    for (size_t row = 0; row < loaded.size(); ++row) {
        if (valid[row])
            ExpectOnlyTextMatchHit(TextReader(pin.Reader()),
                                   loaded[row],
                                   inserted.size() + row,
                                   all_valid.size());
    }
}

TEST(SegmentTextFieldLoadTest, NullableFieldDataAppendsAtReservedOffset) {
    CheckLoadedSuffix({"alpha", "beta", "gamma"},
                      {"delta", "", "epsilon"},
                      {true, false, true});
}

TEST(SegmentTextFieldLoadTest, LoadBuildsTextIndexAfterPreviouslyInsertedRows) {
    CheckLoadedSuffix(
        {"football", "tennis"}, {"cricket", "curling"}, {true, true});
}

SchemaPtr
LobSchema(bool nullable) {
    auto schema = std::make_shared<Schema>();
    schema->AddField(FieldMeta(
        FieldName("pk"), FieldId(100), DataType::INT64, false, std::nullopt));
    schema->set_primary_field_id(FieldId(100));
    std::map<std::string, std::string> text_params{
        {"enable_match", "true"},
        {"enable_analyzer", "true"},
        {"analyzer_params", R"({"tokenizer":"standard"})"}};
    schema->AddField(FieldMeta(FieldName("str"),
                               FieldId(101),
                               DataType::TEXT,
                               65536,
                               nullable,
                               true,
                               true,
                               text_params,
                               std::nullopt));
    return schema;
}

std::vector<std::string>
WriteLobReferences(const std::string& path,
                   const std::vector<std::string>& texts) {
    milvus_storage::lob_column::LobColumnConfig config;
    config.lob_base_path = path;
    config.field_id = 101;
    config.inline_threshold = 1;
    config.max_lob_file_bytes = 256 * 1024;
    config.flush_threshold_bytes = 64 * 1024;
    auto properties =
        storage::LoonFFIPropertiesSingleton::GetInstance().GetProperties();
    AssertInfo(properties != nullptr, "LOB fixture properties missing");
    config.properties = *properties;
    auto manager_result = milvus_storage::lob_column::LobColumnManager::Create(
        GetDefaultArrowFileSystem(), config);
    AssertInfo(manager_result.ok(),
               "LOB manager failed: {}",
               manager_result.status().ToString());
    auto manager = std::move(manager_result).ValueOrDie();
    auto writer_result = manager->CreateWriter();
    AssertInfo(writer_result.ok(),
               "LOB writer failed: {}",
               writer_result.status().ToString());
    auto writer = std::move(writer_result).ValueOrDie();
    std::vector<std::string> refs;
    refs.reserve(texts.size());
    for (const auto& text : texts) {
        auto result = writer->WriteText(text);
        AssertInfo(
            result.ok(), "LOB write failed: {}", result.status().ToString());
        auto ref = std::move(result).ValueOrDie();
        AssertInfo(
            ref.size() == milvus_storage::lob_column::LOB_REFERENCE_SIZE &&
                milvus_storage::lob_column::IsLOBReference(ref.data()),
            "fixture did not produce a LOB reference");
        refs.emplace_back(reinterpret_cast<const char*>(ref.data()),
                          ref.size());
    }
    auto result = writer->Close();
    AssertInfo(result.ok(), "LOB close failed: {}", result.status().ToString());
    AssertInfo(!result.ValueOrDie().empty(), "LOB fixture wrote no files");
    return refs;
}

TEST(SegmentTextFieldLoadTest, SealedTextIndexDecodesRemoteLobReferences) {
    auto root = storage::LocalDirectory::CreateOwned(
        TestRemotePath, "sealed-text-lob-XXXXXX", "sealed text LOB test");
    auto schema = LobSchema(false);
    const std::string term = "zzlobuniqueterm";
    const std::vector<std::string> texts{
        "plain text", std::string(72 * 1024, 'x') + " " + term, "another row"};
    const auto lob_path = root->Path() + "/partition/lobs/101";
    auto refs = WriteLobReferences(lob_path, texts);
    auto data = DataGen(schema, refs.size());
    auto* strings = data.raw_->mutable_fields_data(1)
                        ->mutable_scalars()
                        ->mutable_string_data();
    for (size_t row = 0; row < refs.size(); ++row)
        strings->set_data(row, refs[row]);
    auto manager =
        storage::CreateChunkManager(get_default_local_storage_config());
    auto info = PrepareInsertBinlog(1, 2, 3, data, manager);
    auto segment = CreateSealedSegment(schema, empty_index_meta, 3);
    auto* sealed = dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get());
    ASSERT_NE(sealed, nullptr);
    sealed->SetTextLobPathForTesting(FieldId(101), lob_path);
    segment->LoadFieldData(info);
    sealed->CreateTextIndex(FieldId(101));
    auto pin = PinSealedText(*sealed, FieldId(101));
    ASSERT_TRUE(static_cast<bool>(pin));
    ExpectOnlyTextMatchHit(TextReader(*pin.get()), term, 1, texts.size());
}

std::string
WriteLobManifest(const SchemaPtr& schema,
                 const std::string& base_path,
                 const std::vector<std::string>& refs,
                 int64_t null_row) {
    auto loon_schema =
        schema->ConvertToLoonArrowSchema(/*text_lob_as_binary=*/true);
    arrow::Int64Builder pks;
    arrow::BinaryBuilder texts;
    for (size_t row = 0; row < refs.size(); ++row) {
        AssertInfo(pks.Append(row).ok(), "append fixture primary key");
        const auto status = static_cast<int64_t>(row) == null_row
                                ? texts.AppendNull()
                                : texts.Append(refs[row]);
        AssertInfo(status.ok(), "append fixture LOB reference");
    }
    auto pk_array = pks.Finish().ValueOrDie();
    auto text_array = texts.Finish().ValueOrDie();
    auto batch = arrow::RecordBatch::Make(
        loon_schema, refs.size(), {pk_array, text_array});
    milvus_storage::api::Properties properties;
    milvus_storage::api::SetValue(
        properties, PROPERTY_FS_STORAGE_TYPE, LOON_FS_TYPE_LOCAL);
    milvus_storage::api::SetValue(
        properties, PROPERTY_FS_ROOT_PATH, kLoonLocalFSRootPath);
    milvus_storage::api::SetValue(properties,
                                  PROPERTY_WRITER_POLICY,
                                  LOON_COLUMN_GROUP_POLICY_SCHEMA_BASED);
    milvus_storage::api::SetValue(
        properties, PROPERTY_WRITER_SCHEMA_BASE_PATTERNS, "100|101");
    auto policy_result =
        milvus_storage::api::ColumnGroupPolicy::create_column_group_policy(
            properties, loon_schema);
    AssertInfo(policy_result.ok(),
               "LOB manifest policy failed: {}",
               policy_result.status().ToString());
    auto writer = milvus_storage::api::Writer::create(
        base_path,
        loon_schema,
        std::move(policy_result).ValueOrDie(),
        properties);
    auto status = writer->write(batch);
    AssertInfo(status.ok(), "LOB manifest write failed: {}", status.ToString());
    auto closed = writer->close();
    AssertInfo(closed.ok(),
               "LOB manifest close failed: {}",
               closed.status().ToString());
    auto groups = std::move(closed).ValueOrDie();
    auto transaction = milvus_storage::api::transaction::Transaction::Open(
        GetDefaultArrowFileSystem(), base_path);
    AssertInfo(transaction.ok(),
               "LOB transaction open failed: {}",
               transaction.status().ToString());
    auto txn = std::move(transaction).ValueOrDie();
    txn->AppendFiles(*groups);
    auto committed = txn->Commit();
    AssertInfo(committed.ok(),
               "LOB transaction commit failed: {}",
               committed.status().ToString());
    return Config{{"base_path", base_path}, {"ver", committed.ValueOrDie()}}
        .dump();
}

TEST(SegmentTextFieldLoadTest,
     GrowingManifestDecodesLobBatchesAndSkipsNullText) {
    constexpr int64_t rows = 1030;
    auto root = storage::LocalDirectory::CreateOwned(
        TestRemotePath, "growing-text-lob-XXXXXX", "growing text LOB test");
    auto schema = LobSchema(true);
    const std::string large_term = "zzgrowingloblarge";
    const std::string batch_term = "zzgrowinglobbatch";
    const std::string null_term = "zzgrowinglobnull";
    std::vector<std::string> texts(rows, "plain growing text");
    texts[3] = std::string(72 * 1024, 'x') + " " + large_term;
    texts[1025] = batch_term;
    texts[7] = null_term;
    const auto partition = root->Path() + "/partition";
    const auto refs = WriteLobReferences(partition + "/lobs/101", texts);
    const auto manifest =
        WriteLobManifest(schema, partition + "/segment", refs, 7);
    proto::segcore::SegmentLoadInfo info;
    info.set_collectionid(1);
    info.set_partitionid(2);
    info.set_segmentid(3);
    info.set_storageversion(STORAGE_V3);
    info.set_num_of_rows(rows);
    info.set_manifest_path(manifest);
    info.set_insert_channel("text_lob_load_test");
    auto segment = CreateGrowingSegment(schema, empty_index_meta, 3);
    segment->SetLoadInfo(info);
    tracer::TraceContext trace;
    segment->Load(trace, nullptr);
    auto pin = segment->PinGrowingIndex(FieldId(101));
    ASSERT_TRUE(static_cast<bool>(pin));
    ASSERT_EQ(pin.CoveredRowEnd(), rows);
    const auto& text = TextReader(pin.Reader());
    ExpectOnlyTextMatchHit(text, large_term, 3, rows);
    ExpectOnlyTextMatchHit(text, batch_term, 1025, rows);
    EXPECT_EQ(text.MatchQuery(null_term, 1).count(), 0);
    auto valid = std::vector<bool>(rows, true);
    valid[7] = false;
    ExpectNullRows(pin.Reader(), valid);
}

}  // namespace
}  // namespace milvus::segcore
