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

#include <boost/filesystem.hpp>
#include <gtest/gtest.h>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/Common.h"
#include "common/Schema.h"
#include "common/Slice.h"
#include "index/Meta.h"
#include "index/TextMatchIndex.h"
#include "storage/FileManager.h"
#include "storage/Util.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/storagev1translator/TextMatchIndexTranslator.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "query/PlanProto.h"
#include "query/ExecPlanNodeVisitor.h"
#include "expr/ITypeExpr.h"
#include "segcore/segment_c.h"
#include "test_utils/storage_test_utils.h"
#include "exec/expression/ExprCache.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

namespace {

TEST(TextMatch, WriterMemoryBudgets) {
    EXPECT_EQ(milvus::tantivy::DEFAULT_OVERALL_MEMORY_BUDGET_IN_BYTES,
              500UL * 1024 * 1024);
    EXPECT_EQ(milvus::tantivy::GROWING_TEXT_MEMORY_BUDGET_IN_BYTES,
              15UL * 1024 * 1024);
}

SchemaPtr
GenTestSchema(std::map<std::string, std::string> params = {},
              bool nullable = false) {
    auto schema = std::make_shared<Schema>();
    {
        FieldMeta f(FieldName("pk"),
                    FieldId(100),
                    DataType::INT64,
                    false,
                    std::nullopt);
        schema->AddField(std::move(f));
        schema->set_primary_field_id(FieldId(100));
    }
    {
        FieldMeta f(FieldName("str"),
                    FieldId(101),
                    DataType::VARCHAR,
                    65536,
                    nullable,
                    true,
                    true,
                    params,
                    std::nullopt);
        schema->AddField(std::move(f));
    }
    {
        FieldMeta f(FieldName("fvec"),
                    FieldId(102),
                    DataType::VECTOR_FLOAT,
                    16,
                    knowhere::metric::L2,
                    false,
                    std::nullopt);
        schema->AddField(std::move(f));
    }
    return schema;
}

storage::FileManagerContext
CreateTextMatchTestFileManagerContext(int64_t build_id) {
    auto storage_config = get_default_local_storage_config();
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);

    storage::FieldDataMeta field_meta{1, 2, 3, 101};
    field_meta.field_schema.set_data_type(proto::schema::DataType::VarChar);
    storage::IndexMeta index_meta{3, 101, build_id, 10000};
    return storage::FileManagerContext(
        field_meta, index_meta, chunk_manager, fs);
}

class FileSliceSizeGuard {
 public:
    explicit FileSliceSizeGuard(int64_t slice_size)
        : old_slice_size_(FILE_SLICE_SIZE.load()) {
        FILE_SLICE_SIZE.store(slice_size);
    }

    ~FileSliceSizeGuard() {
        FILE_SLICE_SIZE.store(old_slice_size_);
    }

 private:
    int64_t old_slice_size_;
};

std::shared_ptr<milvus::plan::FilterBitsNode>
GetMatchExpr(SchemaPtr schema,
             const std::string& query,
             proto::plan::OpType op,
             int64_t slop = 0) {
    const auto& str_meta = schema->operator[](FieldName("str"));
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);

    auto unary_range_expr = test::GenUnaryRangeExpr(op, query);
    unary_range_expr->set_allocated_column_info(column_info);
    auto generic_for_slop = milvus::test::GenGenericValue(slop);
    unary_range_expr->add_extra_values()->CopyFrom(*generic_for_slop);
    delete generic_for_slop;
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);

    auto parser = ProtoParser(schema);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);
    return parsed;
};

std::shared_ptr<milvus::plan::FilterBitsNode>
GetNotMatchExpr(SchemaPtr schema,
                const std::string& query,
                proto::plan::OpType op,
                int64_t slop = 0) {
    const auto& str_meta = schema->operator[](FieldName("str"));
    proto::plan::GenericValue val;
    val.set_string_val(query);
    std::vector<proto::plan::GenericValue> extra_values;
    auto generic_for_slop = milvus::test::GenGenericValue(slop);
    extra_values.push_back(*generic_for_slop);
    delete generic_for_slop;
    auto child_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        milvus::expr::ColumnInfo(str_meta.get_id(), DataType::VARCHAR),
        op,
        val,
        extra_values);
    auto expr = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, child_expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    return parsed;
};
}  // namespace

TEST(ParseJson, Naive) {
    {
        std::string s(R"({"tokenizer": "jieba"})");
        nlohmann::json j = nlohmann::json::parse(s);
        auto m = j.get<std::map<std::string, std::string>>();
        for (const auto& [k, v] : m) {
            std::cout << k << ": " << v << std::endl;
        }
    }

    {
        std::string s(
            R"({"analyzer":"stop","stop_words":["an","the"],"case_insensitive":false})");
        nlohmann::json j = nlohmann::json::parse(s);
        for (const auto& [key, value] : j.items()) {
            std::cout << key << ": " << value.dump() << std::endl;
        }
    }
}

TEST(ParseTokenizerParams, NoTokenizerParams) {
    TypeParams params{{"k", "v"}};
    auto p = ParseTokenizerParams(params);
    ASSERT_EQ("{}", std::string(p));
}

TEST(ParseTokenizerParams, Default) {
    TypeParams params{{"analyzer_params", R"({"tokenizer": "standard"})"}};
    auto p = ParseTokenizerParams(params);
    ASSERT_EQ(params.at("analyzer_params"), p);
}

TEST(TextMatch, Index) {
    using Index = index::TextMatchIndex;
    auto index = std::make_unique<Index>(std::numeric_limits<int64_t>::max(),
                                         "unique_id",
                                         "milvus_tokenizer",
                                         "{}",
                                         /*enable_background_merge=*/false);
    index->CreateReader(milvus::index::SetBitsetSealed);
    index->AddTextSealed("football, basketball, pingpang", true, 0);
    index->AddTextSealed("", false, 1);
    index->AddTextSealed("swimming, football", true, 2);
    index->Commit();
    index->Reload();

    {
        auto res = index->MatchQuery("football", 1);
        ASSERT_EQ(res.size(), 3);
        ASSERT_TRUE(res[0]);
        ASSERT_FALSE(res[1]);
        ASSERT_TRUE(res[2]);
        auto res1 = index->IsNull();
        ASSERT_FALSE(res1[0]);
        ASSERT_TRUE(res1[1]);
        ASSERT_FALSE(res1[2]);
        auto res2 = index->IsNotNull();
        ASSERT_TRUE(res2[0]);
        ASSERT_FALSE(res2[1]);
        ASSERT_TRUE(res2[2]);
        res = index->MatchQuery("nothing", 1);
        ASSERT_EQ(res.size(), 3);
        ASSERT_FALSE(res[0]);
        ASSERT_FALSE(res[1]);
        ASSERT_FALSE(res[2]);
        auto res3 = index->MatchQuery("football pingpang cricket", 2);
        ASSERT_EQ(res3.size(), 3);
        ASSERT_TRUE(res3[0]);
        ASSERT_FALSE(res3[1]);
        ASSERT_FALSE(res3[2]);
    }

    {
        auto res = index->PhraseMatchQuery("football", 0);
        ASSERT_EQ(res.size(), 3);
        ASSERT_TRUE(res[0]);
        ASSERT_FALSE(res[1]);
        ASSERT_TRUE(res[2]);

        auto res1 = index->PhraseMatchQuery("swimming football", 0);
        ASSERT_EQ(res1.size(), 3);
        ASSERT_FALSE(res1[0]);
        ASSERT_FALSE(res1[1]);
        ASSERT_TRUE(res1[2]);

        auto res2 = index->PhraseMatchQuery("football swimming", 0);
        ASSERT_EQ(res2.size(), 3);
        ASSERT_FALSE(res2[0]);
        ASSERT_FALSE(res2[1]);
        ASSERT_FALSE(res2[2]);

        auto res3 = index->PhraseMatchQuery("football swimming", 1);
        ASSERT_EQ(res3.size(), 3);
        ASSERT_FALSE(res3[0]);
        ASSERT_FALSE(res3[1]);
        ASSERT_FALSE(res3[2]);

        auto res4 = index->PhraseMatchQuery("football swimming", 2);
        ASSERT_EQ(res4.size(), 3);
        ASSERT_FALSE(res4[0]);
        ASSERT_FALSE(res4[1]);
        ASSERT_TRUE(res4[2]);
    }
}

TEST(TextMatch, RawSealedFinalizationMaterializesOnlyWhenNeeded) {
    using Index = index::TextMatchIndex;

    auto with_null =
        std::make_unique<Index>(std::numeric_limits<int64_t>::max(),
                                "raw_sealed_with_null",
                                "milvus_tokenizer",
                                "{}",
                                /*enable_background_merge=*/false);
    with_null->AddTextSealed("alpha", true, 0);
    with_null->AddNullSealed(1);
    with_null->AddTextSealed("beta", true, 2);
    with_null->CreateReader(milvus::index::SetBitsetSealed);
    with_null->Finish();
    with_null->Reload();
    with_null->FinalizeSealed();
    with_null->FinalizeSealed();  // Idempotent for converging sealed paths.

    auto nulls = with_null->IsNull();
    ASSERT_EQ(nulls.size(), 3);
    EXPECT_FALSE(nulls[0]);
    EXPECT_TRUE(nulls[1]);
    EXPECT_FALSE(nulls[2]);
    EXPECT_EQ(with_null->ValidityBitmapByteSize(), sizeof(uint64_t));

    auto all_valid =
        std::make_unique<Index>(std::numeric_limits<int64_t>::max(),
                                "raw_sealed_all_valid",
                                "milvus_tokenizer",
                                "{}",
                                /*enable_background_merge=*/false);
    all_valid->AddTextSealed("alpha", true, 0);
    all_valid->AddTextSealed("beta", true, 1);
    all_valid->CreateReader(milvus::index::SetBitsetSealed);
    all_valid->Finish();
    all_valid->Reload();
    all_valid->FinalizeSealed();

    EXPECT_EQ(all_valid->ValidityBitmapByteSize(), 0);
    auto valid = all_valid->IsNotNull();
    ASSERT_EQ(valid.size(), 2);
    EXPECT_TRUE(valid[0]);
    EXPECT_TRUE(valid[1]);
    auto no_nulls = all_valid->IsNull();
    EXPECT_EQ(no_nulls.count(), 0);
}

TEST(TextMatch, V2LoadFinalizesValidityBitmap) {
    auto ctx = CreateTextMatchTestFileManagerContext(1003);
    auto builder = std::make_unique<index::TextMatchIndex>(
        ctx, index::TANTIVY_INDEX_LATEST_VERSION, "milvus_tokenizer", "{}");

    std::vector<std::string> texts = {"alpha", "", "beta"};
    std::vector<uint8_t> valid_bytes = {0b00000101};
    auto field_data = storage::CreateFieldData(
        DataType::VARCHAR, DataType::NONE, true, 1, texts.size());
    field_data->FillFieldData(
        texts.data(), valid_bytes.data(), texts.size(), 0);
    builder->BuildIndexFromFieldData({field_data}, true);
    auto stats = builder->Upload({});
    auto files = stats->GetIndexFiles();

    for (bool mmap_enabled : {false, true}) {
        Config config;
        config[index::INDEX_FILES] = files;
        config[index::ENABLE_MMAP] = mmap_enabled;

        auto loaded = std::make_unique<index::TextMatchIndex>(ctx);
        loaded->Load(config);

        EXPECT_EQ(loaded->ValidityBitmapByteSize(), sizeof(uint64_t));
        auto nulls = loaded->IsNull();
        ASSERT_EQ(nulls.size(), texts.size());
        EXPECT_FALSE(nulls[0]);
        EXPECT_TRUE(nulls[1]);
        EXPECT_FALSE(nulls[2]);

        std::string excluded = "alpha";
        auto not_in = loaded->NotIn(1, &excluded);
        ASSERT_EQ(not_in.size(), texts.size());
        EXPECT_FALSE(not_in[0]);
        EXPECT_FALSE(not_in[1]);
        EXPECT_TRUE(not_in[2]);
    }
}

TEST(TextMatch, V2LoadSlicedNullOffsets) {
    constexpr int64_t kSliceSize = 4 * 1024;
    constexpr size_t kRows = kSliceSize / sizeof(size_t) + 1;

    auto ctx = CreateTextMatchTestFileManagerContext(1004);
    auto builder = std::make_unique<index::TextMatchIndex>(
        ctx, index::TANTIVY_INDEX_LATEST_VERSION, "milvus_tokenizer", "{}");

    std::vector<std::string> texts(kRows);
    std::vector<uint8_t> valid_bytes((kRows + 7) / 8, 0);
    auto field_data = storage::CreateFieldData(
        DataType::VARCHAR, DataType::NONE, true, 1, texts.size());
    field_data->FillFieldData(
        texts.data(), valid_bytes.data(), texts.size(), 0);
    builder->BuildIndexFromFieldData({field_data}, true);
    auto stats = [&]() {
        FileSliceSizeGuard slice_size_guard(kSliceSize);
        return builder->Upload({});
    }();

    auto files = stats->GetIndexFiles();
    bool found_null_offset_slice = false;
    bool found_slice_meta = false;
    for (const auto& file : files) {
        auto file_name = boost::filesystem::path(file).filename().string();
        found_null_offset_slice |=
            file_name.find(index::INDEX_NULL_OFFSET_FILE_NAME +
                           std::string("_")) == 0;
        found_slice_meta |= file_name == INDEX_FILE_SLICE_META;
    }
    ASSERT_TRUE(found_null_offset_slice);
    ASSERT_TRUE(found_slice_meta);

    for (bool mmap_enabled : {false, true}) {
        Config config;
        config[index::INDEX_FILES] = files;
        config[index::ENABLE_MMAP] = mmap_enabled;

        auto loaded = std::make_unique<index::TextMatchIndex>(ctx);
        loaded->Load(config);

        EXPECT_EQ(loaded->ValidityBitmapByteSize(),
                  TargetBitmap(kRows).size_in_bytes());
        auto nulls = loaded->IsNull();
        ASSERT_EQ(nulls.size(), kRows);
        EXPECT_EQ(nulls.count(), kRows);

        auto valid = loaded->IsNotNull();
        ASSERT_EQ(valid.size(), kRows);
        EXPECT_EQ(valid.count(), 0);

        std::string excluded = "alpha";
        auto not_in = loaded->NotIn(1, &excluded);
        ASSERT_EQ(not_in.size(), kRows);
        EXPECT_EQ(not_in.count(), 0);
    }
}

TEST(TextMatch, TranslatorResourceAccountsForValidityBitmap) {
    auto ctx = CreateTextMatchTestFileManagerContext(1005);
    auto builder = std::make_unique<index::TextMatchIndex>(
        ctx, index::TANTIVY_INDEX_LATEST_VERSION, "milvus_tokenizer", "{}");

    std::vector<std::string> texts = {"alpha", "", "beta"};
    std::vector<uint8_t> valid_bytes = {0b00000101};
    auto field_data = storage::CreateFieldData(
        DataType::VARCHAR, DataType::NONE, true, 1, texts.size());
    field_data->FillFieldData(
        texts.data(), valid_bytes.data(), texts.size(), 0);
    builder->BuildIndexFromFieldData({field_data}, true);
    auto stats = builder->Upload({});

    auto files = stats->GetIndexFiles();
    auto index_size = stats->GetMemSize();
    auto bitmap_bytes =
        static_cast<int64_t>(TargetBitmap(texts.size()).size_in_bytes());

    for (bool mmap_enabled : {false, true}) {
        Config config;
        config[index::INDEX_FILES] = files;
        config[index::ENABLE_MMAP] = mmap_enabled;

        storagev1translator::TextMatchIndexLoadInfo load_info{
            mmap_enabled,
            3,
            101,
            "{}",
            index_size,
            static_cast<int64_t>(texts.size()),
            ""};
        storagev1translator::TextMatchIndexTranslator translator(
            load_info, ctx, config);

        auto [estimated_loaded, estimated_total_loading] =
            translator.estimated_byte_size_of_cell(0);
        if (mmap_enabled) {
            EXPECT_EQ(estimated_loaded.memory_bytes, bitmap_bytes);
            EXPECT_EQ(estimated_loaded.file_bytes, index_size);
            EXPECT_EQ(estimated_total_loading.memory_bytes,
                      index_size + bitmap_bytes);
            EXPECT_EQ(estimated_total_loading.file_bytes, index_size);
        } else {
            EXPECT_EQ(estimated_loaded.memory_bytes, index_size + bitmap_bytes);
            EXPECT_EQ(estimated_loaded.file_bytes, 0);
            EXPECT_EQ(estimated_total_loading.memory_bytes,
                      index_size + bitmap_bytes);
            EXPECT_EQ(estimated_total_loading.file_bytes, index_size);
        }

        auto cells = translator.get_cells(nullptr, {0});
        ASSERT_EQ(cells.size(), 1);
        auto* loaded = cells.front().second.get();
        ASSERT_NE(loaded, nullptr);
        EXPECT_EQ(loaded->ValidityBitmapByteSize(), bitmap_bytes);
        if (mmap_enabled) {
            EXPECT_EQ(loaded->CellByteSize().memory_bytes, bitmap_bytes);
            EXPECT_EQ(loaded->CellByteSize().file_bytes,
                      loaded->ByteSize() - bitmap_bytes);
        } else {
            EXPECT_EQ(loaded->CellByteSize().memory_bytes, loaded->ByteSize());
            EXPECT_EQ(loaded->CellByteSize().file_bytes, 0);
        }
    }
}

// Regression test: BuildIndexFromFieldData with multiple FieldData batches
// and nullable fields must record correct global offsets in null_offset_.
// Previously, null_offset_.push_back(i) used the batch-local index instead
// of the global accumulated offset, causing wrong null tracking for all
// batches after the first.
TEST(TextMatch, BuildIndexFromFieldDataMultiBatchNullable) {
    using Index = index::TextMatchIndex;

    // Helper to create a nullable FieldData<std::string> batch.
    auto make_batch =
        [](const std::vector<std::string>& texts,
           const std::vector<bool>& valids) -> milvus::FieldDataPtr {
        auto fd = storage::CreateFieldData(
            DataType::VARCHAR, DataType::NONE, true, 1, texts.size());
        // Pack valid bits into uint8_t array (LSB first).
        size_t byte_count = (texts.size() + 7) / 8;
        std::vector<uint8_t> valid_bytes(byte_count, 0);
        for (size_t i = 0; i < valids.size(); i++) {
            if (valids[i]) {
                valid_bytes[i >> 3] |= (1u << (i & 7));
            }
        }
        fd->FillFieldData(texts.data(), valid_bytes.data(), texts.size(), 0);
        return fd;
    };

    // Three batches, nulls scattered across all batches.
    // Batch 0 (3 rows): "hello", NULL,  "world"
    // Batch 1 (3 rows): NULL,   "foo",  NULL
    // Batch 2 (2 rows): "bar",  NULL
    //
    // Global layout (8 rows total):
    //   offset 0: "hello" (valid)
    //   offset 1: ""      (null)
    //   offset 2: "world" (valid)
    //   offset 3: ""      (null)
    //   offset 4: "foo"   (valid)
    //   offset 5: ""      (null)
    //   offset 6: "bar"   (valid)
    //   offset 7: ""      (null)
    auto batch0 = make_batch({"hello", "", "world"}, {true, false, true});
    auto batch1 = make_batch({"", "foo", ""}, {false, true, false});
    auto batch2 = make_batch({"bar", ""}, {true, false});

    std::vector<milvus::FieldDataPtr> field_datas = {batch0, batch1, batch2};

    auto index = std::make_unique<Index>(200,
                                         "test_multi_batch",
                                         "milvus_tokenizer",
                                         "{}",
                                         /*enable_background_merge=*/true);
    index->CreateReader(milvus::index::SetBitsetGrowing);
    index->RegisterTokenizer("milvus_tokenizer", "{}");

    index->BuildIndexFromFieldData(field_datas, true /* nullable */);
    index->Commit();
    index->Reload();

    // Verify null tracking: offsets 1, 3, 5, 7 should be null.
    {
        auto null_bits = index->IsNull();
        ASSERT_EQ(null_bits.size(), 8);
        ASSERT_FALSE(null_bits[0]);  // "hello"
        ASSERT_TRUE(null_bits[1]);   // null
        ASSERT_FALSE(null_bits[2]);  // "world"
        ASSERT_TRUE(null_bits[3]);   // null (batch 1, row 0)
        ASSERT_FALSE(null_bits[4]);  // "foo"
        ASSERT_TRUE(null_bits[5]);   // null (batch 1, row 2)
        ASSERT_FALSE(null_bits[6]);  // "bar"
        ASSERT_TRUE(null_bits[7]);   // null (batch 2, row 1)
    }

    // Verify not-null tracking.
    {
        auto not_null_bits = index->IsNotNull();
        ASSERT_EQ(not_null_bits.size(), 8);
        ASSERT_TRUE(not_null_bits[0]);
        ASSERT_FALSE(not_null_bits[1]);
        ASSERT_TRUE(not_null_bits[2]);
        ASSERT_FALSE(not_null_bits[3]);
        ASSERT_TRUE(not_null_bits[4]);
        ASSERT_FALSE(not_null_bits[5]);
        ASSERT_TRUE(not_null_bits[6]);
        ASSERT_FALSE(not_null_bits[7]);
    }

    // Verify text search still works on valid rows.
    {
        auto res = index->MatchQuery("hello", 1);
        ASSERT_EQ(res.size(), 8);
        ASSERT_TRUE(res[0]);
        for (int i = 1; i < 8; i++) {
            ASSERT_FALSE(res[i]) << "unexpected match at offset " << i;
        }
    }
    {
        auto res = index->MatchQuery("foo", 1);
        ASSERT_EQ(res.size(), 8);
        ASSERT_TRUE(res[4]);
        for (int i = 0; i < 8; i++) {
            if (i != 4) {
                ASSERT_FALSE(res[i]) << "unexpected match at offset " << i;
            }
        }
    }
    {
        auto res = index->MatchQuery("bar", 1);
        ASSERT_EQ(res.size(), 8);
        ASSERT_TRUE(res[6]);
        for (int i = 0; i < 8; i++) {
            if (i != 6) {
                ASSERT_FALSE(res[i]) << "unexpected match at offset " << i;
            }
        }
    }
}

// Regression test: BuildIndexFromFieldData with a single batch should still
// work correctly (i == offset in this case, so the old bug was hidden).
TEST(TextMatch, BuildIndexFromFieldDataSingleBatchNullable) {
    using Index = index::TextMatchIndex;

    auto fd =
        storage::CreateFieldData(DataType::VARCHAR, DataType::NONE, true, 1, 4);
    std::vector<std::string> texts = {"alpha", "", "beta", ""};
    std::vector<bool> valids = {true, false, true, false};
    size_t byte_count = (texts.size() + 7) / 8;
    std::vector<uint8_t> valid_bytes(byte_count, 0);
    for (size_t i = 0; i < valids.size(); i++) {
        if (valids[i]) {
            valid_bytes[i >> 3] |= (1u << (i & 7));
        }
    }
    fd->FillFieldData(texts.data(), valid_bytes.data(), texts.size(), 0);

    std::vector<milvus::FieldDataPtr> field_datas = {fd};

    auto index = std::make_unique<Index>(200,
                                         "test_single_batch",
                                         "milvus_tokenizer",
                                         "{}",
                                         /*enable_background_merge=*/true);
    index->CreateReader(milvus::index::SetBitsetGrowing);
    index->RegisterTokenizer("milvus_tokenizer", "{}");

    index->BuildIndexFromFieldData(field_datas, true);
    index->Commit();
    index->Reload();

    auto null_bits = index->IsNull();
    ASSERT_EQ(null_bits.size(), 4);
    ASSERT_FALSE(null_bits[0]);
    ASSERT_TRUE(null_bits[1]);
    ASSERT_FALSE(null_bits[2]);
    ASSERT_TRUE(null_bits[3]);

    auto res = index->MatchQuery("alpha", 1);
    ASSERT_EQ(res.size(), 4);
    ASSERT_TRUE(res[0]);
    ASSERT_FALSE(res[1]);
    ASSERT_FALSE(res[2]);
    ASSERT_FALSE(res[3]);

    auto res2 = index->MatchQuery("beta", 1);
    ASSERT_EQ(res2.size(), 4);
    ASSERT_FALSE(res2[0]);
    ASSERT_FALSE(res2[1]);
    ASSERT_TRUE(res2[2]);
    ASSERT_FALSE(res2[3]);
}

TEST(TextMatch, GrowingNaive) {
    auto schema = GenTestSchema();
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {"football, basketball, pingpang",
                                        "swimming, football"};

    int64_t N = 2;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }

    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);

    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        auto expr =
            GetMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        BitsetType final;
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
        auto expr1 =
            GetNotMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);

        auto expr2 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr2, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        auto expr3 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch, 1);
        final = ExecuteQueryExpr(expr3, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_FALSE(final[1]);
    }
}

TEST(TextMatch, GrowingNaiveNullable) {
    auto schema = GenTestSchema({}, true);
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {
        "football, basketball, pingpang", "swimming, football", ""};
    std::vector<bool> raw_str_valid = {true, true, false};

    int64_t N = 3;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    auto str_col_valid =
        raw_data.raw_->mutable_fields_data()->at(1).mutable_valid_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }
    for (int64_t i = 0; i < N; i++) {
        str_col_valid->at(i) = raw_str_valid[i];
    }

    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);

    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        auto expr =
            GetMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        BitsetType final;
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
        ASSERT_FALSE(final[2]);
        auto expr1 =
            GetNotMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);

        auto expr2 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr2, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);
        auto expr3 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch, 1);
        final = ExecuteQueryExpr(expr3, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);
    }
}

TEST(TextMatch, SealedNaive) {
    auto schema = GenTestSchema();
    std::vector<std::string> raw_str = {"football, basketball, pingpang",
                                        "swimming, football"};

    int64_t N = 2;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }

    auto seg = CreateSealedWithFieldDataLoaded(schema, raw_data);
    seg->CreateTextIndex(FieldId(101));

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        auto expr =
            GetMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        BitsetType final;
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
        auto expr1 =
            GetNotMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);

        auto expr2 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr2, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        auto expr3 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch, 1);
        final = ExecuteQueryExpr(expr3, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_FALSE(final[1]);
    }
}

TEST(TextMatch, SealedNaiveNullable) {
    auto schema = GenTestSchema({}, true);
    std::vector<std::string> raw_str = {
        "football, basketball, pingpang", "swimming, football", ""};
    std::vector<bool> raw_str_valid = {true, true, false};

    int64_t N = 3;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }
    auto str_col_valid =
        raw_data.raw_->mutable_fields_data()->at(1).mutable_valid_data();
    for (int64_t i = 0; i < N; i++) {
        str_col_valid->at(i) = raw_str_valid[i];
    }

    auto seg = CreateSealedWithFieldDataLoaded(schema, raw_data);
    seg->CreateTextIndex(FieldId(101));
    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "football", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "swimming", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        auto expr =
            GetMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        BitsetType final;
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_TRUE(final[1]);
        ASSERT_FALSE(final[2]);
        auto expr1 =
            GetNotMatchExpr(schema, "basketball, swimming", OpType::TextMatch);
        final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);

        auto expr2 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr2, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);
        auto expr3 =
            GetMatchExpr(schema, "football, pingpang", OpType::PhraseMatch, 1);
        final = ExecuteQueryExpr(expr3, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_TRUE(final[0]);
        ASSERT_FALSE(final[1]);
        ASSERT_FALSE(final[2]);
    }
}

TEST(TextMatch, GrowingJieBa) {
    auto schema = GenTestSchema({
        {"enable_match", "true"},
        {"enable_analyzer", "true"},
        {"analyzer_params", R"({"tokenizer": "jieba"})"},
    });
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {"青铜时代", "黄金时代"};

    int64_t N = 2;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }

    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);

    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        auto expr = GetMatchExpr(schema, "黄金时代", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_TRUE(final[1]);
    }
}

TEST(TextMatch, GrowingJieBaNullable) {
    auto schema = GenTestSchema(
        {
            {"enable_match", "true"},
            {"enable_tokenizer", "true"},
            {"analyzer_params", R"({"tokenizer": "jieba"})"},
        },
        true);
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    std::vector<std::string> raw_str = {"青铜时代", "黄金时代", ""};
    std::vector<bool> raw_str_valid = {true, true, false};

    int64_t N = 3;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }
    auto str_col_valid =
        raw_data.raw_->mutable_fields_data()->at(1).mutable_valid_data();
    for (int64_t i = 0; i < N; i++) {
        str_col_valid->at(i) = raw_str_valid[i];
    }

    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);

    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        auto expr = GetMatchExpr(schema, "黄金时代", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_TRUE(final[1]);
        ASSERT_FALSE(final[2]);
    }
}

TEST(TextMatch, SealedJieBa) {
    auto schema = GenTestSchema({
        {"enable_match", "true"},
        {"enable_analyzer", "true"},
        {"analyzer_params", R"({"tokenizer": "jieba"})"},
    });
    std::vector<std::string> raw_str = {"青铜时代", "黄金时代"};

    int64_t N = 2;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }

    auto seg = CreateSealedWithFieldDataLoaded(schema, raw_data);
    seg->CreateTextIndex(FieldId(101));

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            auto expr1 = GetNotMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
        }
    }

    {
        BitsetType final;
        auto expr = GetMatchExpr(schema, "黄金时代", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_TRUE(final[1]);
    }
}

TEST(TextMatch, SealedJieBaNullable) {
    auto schema = GenTestSchema(
        {
            {"enable_match", "true"},
            {"enable_tokenizer", "true"},
            {"analyzer_params", R"({"tokenizer": "jieba"})"},
        },
        true);
    std::vector<std::string> raw_str = {"青铜时代", "黄金时代", ""};
    std::vector<bool> raw_str_valid = {true, true, false};

    int64_t N = 3;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }
    auto str_col_valid =
        raw_data.raw_->mutable_fields_data()->at(1).mutable_valid_data();
    for (int64_t i = 0; i < N; i++) {
        str_col_valid->at(i) = raw_str_valid[i];
    }

    auto seg = CreateSealedWithFieldDataLoaded(schema, raw_data);
    seg->CreateTextIndex(FieldId(101));

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "青铜", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "黄金", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        for (auto op : {OpType::TextMatch, OpType::PhraseMatch}) {
            auto expr = GetMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_TRUE(final[0]);
            ASSERT_TRUE(final[1]);
            ASSERT_FALSE(final[2]);
            auto expr1 = GetNotMatchExpr(schema, "时代", op);
            final = ExecuteQueryExpr(expr1, seg.get(), N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            ASSERT_FALSE(final[0]);
            ASSERT_FALSE(final[1]);
            ASSERT_FALSE(final[2]);
        }
    }

    {
        BitsetType final;
        auto expr = GetMatchExpr(schema, "黄金时代", OpType::PhraseMatch);
        final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        ASSERT_FALSE(final[0]);
        ASSERT_TRUE(final[1]);
        ASSERT_FALSE(final[2]);
    }
}

// Test that growing segment loading flushed binlogs will build text match index.
TEST(TextMatch, GrowingLoadData) {
    int64_t N = 7;
    auto schema = GenTestSchema({}, true);
    schema->AddField(
        FieldName("RowID"), FieldId(0), DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     FieldId(1),
                     DataType::INT64,
                     false,
                     std::nullopt);
    std::vector<std::string> raw_str = {"football, basketball, pingpang",
                                        "swimming, football",
                                        "golf",
                                        "",
                                        "baseball",
                                        "kungfu, football",
                                        ""};
    auto raw_data = DataGen(schema, N);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }
    auto str_col_valid =
        raw_data.raw_->mutable_fields_data()->at(1).mutable_valid_data();
    for (int64_t i = 0; i < N; i++) {
        str_col_valid->at(i) = true;
    }
    // so we cannot match the second row
    str_col_valid->at(1) = false;

    auto storage_config = get_default_local_storage_config();
    auto cm = storage::CreateChunkManager(storage_config);
    auto load_info = PrepareInsertBinlog(1, 2, 3, raw_data, cm);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto status = LoadFieldData(segment.get(), &load_info);
    ASSERT_EQ(status.error_code, Success);
    ASSERT_EQ(segment->get_real_count(), N);
    ASSERT_NE(segment->get_field_avg_size(FieldId(101)), 0);

    // Check whether the text index has been built.
    auto expr = GetMatchExpr(schema, "football", OpType::TextMatch);
    BitsetType final;
    final = ExecuteQueryExpr(expr, segment.get(), N, MAX_TIMESTAMP);
    ASSERT_EQ(final.size(), N);
    ASSERT_TRUE(final[0]);
    ASSERT_FALSE(final[1]);
    ASSERT_FALSE(final[2]);
    ASSERT_FALSE(final[3]);
    ASSERT_FALSE(final[4]);
    ASSERT_TRUE(final[5]);
    ASSERT_FALSE(final[6]);
}

TEST(TextMatch, ConcurrentReadWriteWithNull) {
    auto schema = GenTestSchema({}, true);
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int64_t N = 1000;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col_valid =
        raw_data.raw_->mutable_fields_data()->at(1).mutable_valid_data();
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N - 1; i++) {
        str_col->at(i) = "";
    }
    str_col->at(N - 1) = "football";
    for (int64_t i = 0; i < N - 1; i++) {
        str_col_valid->at(i) = false;
    }
    str_col_valid->at(N - 1) = true;

    std::thread writer([&seg, &raw_data, N]() {
        seg->PreInsert(N);
        seg->Insert(0,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    });

    std::thread reader([&seg, N]() {
        auto start = std::chrono::high_resolution_clock::now();
        ;
        const std::chrono::seconds timeout_duration{2};
        while (true) {
            if (std::chrono::high_resolution_clock::now() - start >
                timeout_duration) {
                ASSERT_TRUE(false)
                    << "Failed to get valid results within timeout";
                break;
            }
            BitsetType final;
            auto expr =
                GetMatchExpr(GenTestSchema(), "football", OpType::TextMatch);
            final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
            if (final.size() != N || !final[N - 1]) {
                continue;
            }
            for (int64_t i = 0; i < N - 1; i++) {
                ASSERT_FALSE(final[i]);
            }
            ASSERT_TRUE(final[N - 1]);
            break;
        }
    });

    writer.join();
    reader.join();
}

TEST(TextMatch, ExprResCacheSealed) {
    using milvus::exec::ExprResCacheManager;
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();
    mgr.SetCapacityBytes(1ULL << 20);

    auto schema = GenTestSchema();
    std::vector<std::string> raw_str = {"football, basketball, pingpang",
                                        "swimming, football"};

    int64_t N = 2;
    uint64_t seed = 19190504;
    auto raw_data = DataGen(schema, N, seed);
    auto str_col = raw_data.raw_->mutable_fields_data()
                       ->at(1)
                       .mutable_scalars()
                       ->mutable_string_data()
                       ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = raw_str[i];
    }

    auto seg = CreateSealedWithFieldDataLoaded(schema, raw_data);
    seg->CreateTextIndex(FieldId(101));

    ASSERT_EQ(mgr.GetEntryCount(), 0);

    BitsetType final;
    auto expr = GetMatchExpr(schema, "football", OpType::TextMatch);
    final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
    ASSERT_EQ(final.size(), N);
    ASSERT_TRUE(final[0]);
    ASSERT_TRUE(final[1]);

    // Expect one cache entry inserted
    ASSERT_EQ(mgr.GetEntryCount(), 1);

    // Run again; should hit cache and not increase entries
    final = ExecuteQueryExpr(expr, seg.get(), N, MAX_TIMESTAMP);
    ASSERT_EQ(final.size(), N);
    ASSERT_TRUE(final[0]);
    ASSERT_TRUE(final[1]);
    ASSERT_EQ(mgr.GetEntryCount(), 1);

    // Cleanup
    mgr.Clear();
    ExprResCacheManager::SetEnabled(false);
}
