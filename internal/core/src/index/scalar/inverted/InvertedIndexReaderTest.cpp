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

#include <cstdint>
#include <string>
#include <string_view>
#include <filesystem>

#include "index/contracts/query/IPatternMatchReader.h"
#include "index/scalar/inverted/InvertedIndexReader.h"
#include "index/SingleCellIndexStorageTranslator.h"
#include "storage/artifact/LocalDirectory.h"
#include "common/Consts.h"
#include "index/test_utils/ScalarReaderFactory.h"

namespace milvus::index::test {
namespace {

TEST(InvertedIndexCacheTest, FreshSlotUsesEachReadersContext) {
    const cachinglayer::ResourceUsage limit{16 * 1024 * 1024, 0};
    auto dlist = std::make_shared<cachinglayer::internal::DList>(
        false, limit, limit, limit, cachinglayer::EvictionConfig{});
    std::vector<int32_t> priorities;
    auto load = [&](OpContext* ctx)
        -> folly::coro::Task<std::unique_ptr<InvertedIndexStorage<int64_t>>> {
        priorities.push_back(ctx->runtime_load_priority.value_or(-1));
        auto directory = storage::LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "inverted-pin-XXXXXX",
            "inverted pin test");
        auto engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
            "values",
            TantivyDataType::I64,
            directory->Path().c_str(),
            TANTIVY_INDEX_LATEST_VERSION);
        const int64_t docs[] = {7, 9};
        engine->add_data<int64_t>(docs, 2, 0);
        engine->finish();
        engine->create_reader(SetBitsetSealed);
        co_return std::make_unique<InvertedIndexStorage<int64_t>>(
            std::move(directory),
            std::move(engine),
            std::make_shared<const std::vector<size_t>>(),
            DataType::INT64,
            false,
            true,
            0,
            0);
    };
    IndexCacheConfig cache{
        "inverted-storage-test",
        cachinglayer::Meta{cachinglayer::StorageType::MEMORY,
                           cachinglayer::CellIdMappingMode::IDENTICAL,
                           cachinglayer::CellDataType::OTHER,
                           CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                           true},
        {1024 * 1024, 0},
        {},
        4096};
    std::weak_ptr<cachinglayer::CacheSlot<InvertedIndexStorage<int64_t>>>
        slot_lifetime;
    auto make_index = [&] {
        auto slot = std::make_shared<
            cachinglayer::CacheSlot<InvertedIndexStorage<int64_t>>>(
            std::make_unique<SingleCellIndexStorageTranslator<
                InvertedIndexStorage<int64_t>>>(cache, false, load),
            dlist.get(),
            true,
            true,
            true,
            std::chrono::seconds(5),
            std::chrono::seconds(5));
        slot_lifetime = slot;
        return std::make_shared<InvertedIndex<int64_t>>(
            slot,
            IndexMetadata{ReaderCaps{.predicate = true},
                          Domain::Row,
                          DataType::INT64,
                          2});
    };
    auto index = make_index();
    OpContext first_ctx;
    first_ctx.runtime_load_priority = 0;
    OpContext second_ctx;
    second_ctx.runtime_load_priority = 1;
    auto first = index->CreateReader(&first_ctx);
    EXPECT_EQ(first->Count(), 2);
    EXPECT_TRUE(priorities.empty());
    const auto* predicates =
        dynamic_cast<const IScalarPredicateReader<int64_t>*>(first.get());
    ASSERT_NE(predicates, nullptr);
    const int64_t key = 7;
    EXPECT_EQ(predicates->In(1, &key).count(), 1);
    const auto scanned = first_ctx.storage_usage.scanned_total_bytes.load();
    ASSERT_GT(scanned, 0);
    EXPECT_EQ(predicates->NotIn(1, &key).count(), 1);
    EXPECT_EQ(predicates->Range(key, CompareOp::NotEqual).count(), 1);
    EXPECT_EQ(first_ctx.storage_usage.scanned_total_bytes.load(), scanned);
    EXPECT_EQ(priorities, (std::vector<int32_t>{0}));
    index.reset();
    EXPECT_FALSE(slot_lifetime.expired());
    first.reset();
    EXPECT_TRUE(slot_lifetime.expired());
    index = make_index();
    auto second = index->CreateReader(&second_ctx);
    EXPECT_EQ(priorities, (std::vector<int32_t>{0}));
    const auto* nulls = dynamic_cast<const INullReader*>(second.get());
    ASSERT_NE(nulls, nullptr);
    EXPECT_EQ(nulls->IsNull().count(), 0);
    EXPECT_EQ(priorities, (std::vector<int32_t>{0, 1}));
    EXPECT_GT(second_ctx.storage_usage.scanned_total_bytes.load(), 0);
    index.reset();
    second.reset();
    EXPECT_TRUE(slot_lifetime.expired());
}

template <typename T>
void
ExpectPredicateProfiles(std::string_view type_name) {
    for (const auto suffix : {std::string_view{},
                              std::string_view{"Mmap"},
                              std::string_view{"NonNull"},
                              std::string_view{"NonNullMmap"}}) {
        const auto name =
            "Inverted" + std::string(type_name) + std::string(suffix);
        const auto& backend = ScalarReaderBackends().Get<T>(name);
        EXPECT_TRUE(backend.DeriveCaps().predicate) << name;
    }
}

TEST(InvertedIndexReaderTest, AdvertisesPrimitivePredicateMatrix) {
    ExpectPredicateProfiles<bool>("Bool");
    ExpectPredicateProfiles<int8_t>("Int8");
    ExpectPredicateProfiles<int16_t>("Int16");
    ExpectPredicateProfiles<int32_t>("Int32");
    ExpectPredicateProfiles<int64_t>("Int64");
    ExpectPredicateProfiles<float>("Float");
    ExpectPredicateProfiles<double>("Double");
    ExpectPredicateProfiles<std::string_view>("Varchar");
}

TEST(InvertedIndexReaderTest, AdvertisesVarcharPatternPolicy) {
    for (const auto name : {"InvertedVarchar",
                            "InvertedVarcharMmap",
                            "InvertedVarcharNonNull",
                            "InvertedVarcharNonNullMmap"}) {
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        EXPECT_TRUE(backend.DeriveCaps().pattern_match) << name;
        EXPECT_EQ(backend.PatternPolicy(PatternOp::Match),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::PrefixMatch),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::PostfixMatch),
                  PatternQueryPolicy::DeclineButRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::InnerMatch),
                  PatternQueryPolicy::DeclineButRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::RegexMatch),
                  PatternQueryPolicy::DeclineButRun);
    }
}

}  // namespace
}  // namespace milvus::index::test
