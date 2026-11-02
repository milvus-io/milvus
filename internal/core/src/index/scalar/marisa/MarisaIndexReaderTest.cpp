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

#include <string_view>

#include "cachinglayer/CacheSlot.h"
#include "storage/EntryStreamUtils.h"

#include "index/contracts/query/IPatternMatchReader.h"
#include "index/scalar/marisa/MarisaIndexReader.h"
#include "index/test_utils/ScalarReaderFactory.h"

namespace milvus::index::test {
namespace {

std::unique_ptr<MarisaIndexStorage>
MakeStorage() {
    auto trie = std::make_shared<marisa::Trie>();
    marisa::Keyset keys;
    keys.push_back("apple");
    trie->build(keys);
    auto storage = std::make_unique<MarisaIndexStorage>();
    storage->trie = std::move(trie);
    storage->str_ids_owner = std::make_shared<const std::vector<int64_t>>(
        std::initializer_list<int64_t>{0, -1});
    storage->csr_index_owner = std::make_shared<const std::vector<uint32_t>>(
        std::initializer_list<uint32_t>{0, 1});
    storage->csr_offsets_owner = std::make_shared<const std::vector<uint32_t>>(
        std::initializer_list<uint32_t>{0});
    storage->str_ids = storage->str_ids_owner->data();
    storage->str_ids_size = 2;
    storage->csr_index = storage->csr_index_owner->data();
    storage->csr_offsets = storage->csr_offsets_owner->data();
    storage->csr_num_keys = 1;
    return storage;
}

TEST(MarisaIndexOwnershipTest, ReadersRetainStorageAndOwnedValues) {
    std::shared_ptr<const MarisaIndexStorage> storage = MakeStorage();
    std::weak_ptr<const MarisaIndexStorage> lifetime = storage;
    auto index = std::make_shared<MarisaIndex>(std::move(storage));
    auto first = index->CreateReader(nullptr);
    auto second = index->CreateReader(nullptr);
    EXPECT_NE(first.get(), second.get());
    EXPECT_EQ(index->Metadata().count, 2);
    index.reset();
    const auto* values =
        dynamic_cast<const IScalarValueReader<std::string_view>*>(first.get());
    ASSERT_NE(values, nullptr);
    auto owned = values->Lookup(0);
    first.reset();
    EXPECT_FALSE(lifetime.expired());
    const auto* nulls = dynamic_cast<const INullReader*>(second.get());
    ASSERT_NE(nulls, nullptr);
    EXPECT_EQ(nulls->IsNull().count(), 1);
    second.reset();
    EXPECT_TRUE(lifetime.expired());
    EXPECT_EQ(owned, "apple");
}

class MarisaStorageTranslator final
    : public cachinglayer::Translator<MarisaIndexStorage> {
 public:
    size_t
    num_cells() const override {
        return 1;
    }
    cachinglayer::cid_t
    cell_id_of(cachinglayer::uid_t uid) const override {
        return uid;
    }
    std::pair<cachinglayer::ResourceUsage, cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(cachinglayer::cid_t) const override {
        return {{1024 * 1024, 0}, {0, 0}};
    }
    const std::string&
    key() const override {
        return key_;
    }
    cachinglayer::Meta*
    meta() override {
        return &meta_;
    }
    int64_t
    cells_storage_bytes(
        const std::vector<cachinglayer::cid_t>&) const override {
        return 128;
    }
    std::vector<
        std::pair<cachinglayer::cid_t, std::unique_ptr<MarisaIndexStorage>>>
    get_cells(OpContext* ctx,
              const std::vector<cachinglayer::cid_t>& cids) override {
        ++loads;
        last_context = ctx;
        if (ctx) {
            storage::ThrowIfCancelled(ctx->cancellation_token,
                                      "test marisa load");
        }
        std::vector<
            std::pair<cachinglayer::cid_t, std::unique_ptr<MarisaIndexStorage>>>
            result;
        result.reserve(cids.size());
        for (const auto cid : cids) {
            result.emplace_back(cid, MakeStorage());
        }
        return result;
    }
    int loads{0};
    OpContext* last_context{nullptr};

 private:
    std::string key_{"marisa-storage-test"};
    cachinglayer::Meta meta_{cachinglayer::StorageType::MEMORY,
                             cachinglayer::CellIdMappingMode::IDENTICAL,
                             cachinglayer::CellDataType::OTHER,
                             CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                             true};
};

class MarisaIndexCacheTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        const cachinglayer::ResourceUsage limit{16 * 1024 * 1024, 0};
        dlist = std::make_shared<cachinglayer::internal::DList>(
            false, limit, limit, limit, cachinglayer::EvictionConfig{});
        CreateSlot(true);
    }
    void
    CreateSlot(bool evictable) {
        auto translator = std::make_unique<MarisaStorageTranslator>();
        observer = translator.get();
        slot = std::make_shared<cachinglayer::CacheSlot<MarisaIndexStorage>>(
            std::move(translator),
            dlist.get(),
            evictable,
            true,
            true,
            std::chrono::seconds(5),
            std::chrono::seconds(5));
    }
    std::shared_ptr<cachinglayer::internal::DList> dlist;
    std::shared_ptr<cachinglayer::CacheSlot<MarisaIndexStorage>> slot;
    MarisaStorageTranslator* observer{};
};

TEST_F(MarisaIndexCacheTest,
       PermanentlyResidentSlotRetainsStorageWithoutRepinning) {
    CreateSlot(false);
    observer->meta()->cache_warmup_policy =
        CacheWarmupPolicy::CacheWarmupPolicy_Sync;
    OpContext ctx;
    slot->Warmup(&ctx);
    ASSERT_NE(slot->TryBorrowCell(0), nullptr);
    auto index = std::make_shared<MarisaIndex>(slot, DataType::VARCHAR, 2);
    auto reader = index->CreateReader(&ctx);
    index.reset();
    const auto* values =
        dynamic_cast<const IScalarValueReader<std::string_view>*>(reader.get());
    ASSERT_NE(values, nullptr);
    EXPECT_EQ(values->Lookup(0), "apple");
    EXPECT_FALSE(values->Lookup(1).has_value());
    EXPECT_EQ(observer->loads, 1);
}

TEST_F(MarisaIndexCacheTest, MetadataStaysColdAndOperationsUseRequestContext) {
    auto index = std::make_shared<MarisaIndex>(slot, DataType::VARCHAR, 2);
    OpContext first_ctx;
    OpContext second_ctx;
    auto first = index->CreateReader(&first_ctx);
    auto second = index->CreateReader(&second_ctx);
    EXPECT_EQ(index->Metadata().count, 2);
    EXPECT_EQ(first->Count(), 2);
    EXPECT_EQ(first->ValueType(), DataType::VARCHAR);
    EXPECT_TRUE(first->Caps().predicate);
    EXPECT_EQ(observer->loads, 0);
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
            first.get());
    ASSERT_NE(predicate, nullptr);
    const std::string_view keys[]{"apple", "missing"};
    EXPECT_EQ(predicate->In(2, keys).count(), 1);
    EXPECT_EQ(observer->loads, 1);
    EXPECT_EQ(observer->last_context, &first_ctx);
    EXPECT_TRUE(slot->ManualEvict(0));
    index.reset();
    first.reset();
    const auto* nulls = dynamic_cast<const INullReader*>(second.get());
    ASSERT_NE(nulls, nullptr);
    EXPECT_EQ(nulls->IsNull().count(), 1);
    EXPECT_EQ(observer->loads, 2);
    EXPECT_EQ(observer->last_context, &second_ctx);
    EXPECT_TRUE(slot->ManualEvict(0));
}

TEST_F(MarisaIndexCacheTest, UnknownCountLoadsAndGatherRetainsPin) {
    auto index =
        std::make_shared<MarisaIndex>(slot, DataType::VARCHAR, std::nullopt);
    OpContext ctx;
    auto reader = index->CreateReader(&ctx);
    EXPECT_FALSE(index->Metadata().count.has_value());
    EXPECT_EQ(observer->loads, 0);
    EXPECT_EQ(reader->Count(), 2);
    EXPECT_EQ(observer->last_context, &ctx);
    EXPECT_TRUE(slot->ManualEvict(0));
    const auto* values =
        dynamic_cast<const IScalarValueReader<std::string_view>*>(reader.get());
    ASSERT_NE(values, nullptr);
    const int64_t rows[]{0, 1};
    values->Gather(
        rows, 2, [&](int64_t row, const std::string_view* value, bool valid) {
            EXPECT_FALSE(slot->ManualEvict(0));
            EXPECT_EQ(valid, row == 0);
            if (valid) {
                EXPECT_EQ(*value, "apple");
            }
        });
    EXPECT_EQ(observer->loads, 2);
    EXPECT_TRUE(slot->ManualEvict(0));
}

TEST_F(MarisaIndexCacheTest,
       CancelledLoadCanRetryAndWarmupCancellationDoesNotCancelQuery) {
    auto index =
        std::make_shared<MarisaIndex>(slot, DataType::VARCHAR, std::nullopt);
    folly::CancellationSource cancellation;
    cancellation.requestCancellation();
    OpContext cancelled(cancellation.getToken());
    auto first = index->CreateReader(&cancelled);
    EXPECT_ANY_THROW(first->Count());
    index->CancelWarmup();
    OpContext next;
    auto second = index->CreateReader(&next);
    EXPECT_EQ(second->Count(), 2);
    EXPECT_EQ(observer->last_context, &next);
    EXPECT_TRUE(slot->ManualEvict(0));
}

TEST(MarisaIndexReaderTest, AdvertisesVarcharQueries) {
    for (const auto name : {"MarisaVarchar",
                            "MarisaVarcharMmap",
                            "MarisaVarcharNonNull",
                            "MarisaVarcharNonNullMmap"}) {
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        const auto caps = backend.DeriveCaps();
        EXPECT_TRUE(caps.predicate) << name;
        EXPECT_TRUE(caps.pattern_match) << name;
        EXPECT_TRUE(caps.value_lookup) << name;
        EXPECT_EQ(backend.PatternPolicy(PatternOp::Match),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::PrefixMatch),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::PostfixMatch),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::InnerMatch),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::RegexMatch),
                  PatternQueryPolicy::UseAndRun);
    }
}

}  // namespace
}  // namespace milvus::index::test
