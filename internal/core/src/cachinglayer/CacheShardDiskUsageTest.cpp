// Copyright (C) 2019-2025 Zilliz. All rights reserved.
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

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cachinglayer/Manager.h"
#include "cachinglayer/Metrics.h"
#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"

namespace milvus::cachinglayer {
namespace {

struct TestDiskCell {
    explicit TestDiskCell(ResourceUsage size) : size_(size) {
    }

    ResourceUsage
    CellByteSize() const {
        return size_;
    }

    ResourceUsage size_;
};

class TestDiskTranslator : public Translator<TestDiskCell> {
 public:
    TestDiskTranslator(std::string key,
                       std::string shard,
                       std::vector<ResourceUsage> sizes)
        : key_(std::move(key)),
          sizes_(std::move(sizes)),
          meta_(StorageType::DISK,
                CellIdMappingMode::IDENTICAL,
                CellDataType::SCALAR_FIELD,
                CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                true,
                std::nullopt,
                std::move(shard)) {
    }

    size_t
    num_cells() const override {
        return sizes_.size();
    }

    cid_t
    cell_id_of(uid_t uid) const override {
        return uid;
    }

    std::pair<ResourceUsage, ResourceUsage>
    estimated_byte_size_of_cell(cid_t cid) const override {
        return {sizes_[cid], {}};
    }

    const std::string&
    key() const override {
        return key_;
    }

    Meta*
    meta() override {
        return &meta_;
    }

    int64_t
    cells_storage_bytes(const std::vector<cid_t>& cids) const override {
        int64_t total = 0;
        for (auto cid : cids) {
            total += sizes_[cid].file_bytes;
        }
        return total;
    }

    std::vector<std::pair<cid_t, std::unique_ptr<TestDiskCell>>>
    get_cells(OpContext*, const std::vector<cid_t>& cids) override {
        std::vector<std::pair<cid_t, std::unique_ptr<TestDiskCell>>> cells;
        cells.reserve(cids.size());
        for (auto cid : cids) {
            cells.emplace_back(cid, std::make_unique<TestDiskCell>(sizes_[cid]));
        }
        return cells;
    }

 private:
    std::string key_;
    std::vector<ResourceUsage> sizes_;
    Meta meta_;
};

}  // namespace

TEST(CacheShardDiskUsageTest, RecordsLoadedDiskBytesByShard) {
    const std::string shard = "cache-shard-disk-usage-test";
    const auto before = monitor::GetCacheShardDiskUsageBytesForTest(
        shard, CellDataType::SCALAR_FIELD);

    std::unique_ptr<Translator<TestDiskCell>> translator =
        std::make_unique<TestDiskTranslator>(
            "cache_shard_disk_usage",
            shard,
            std::vector<ResourceUsage>{{0, 128}, {0, 256}});
    auto slot = Manager::GetInstance().CreateCacheSlot(std::move(translator));

    {
        auto pins = SemiInlineGet(slot->PinCells(nullptr, {0, 1}));
        ASSERT_NE(pins, nullptr);
        ASSERT_NE(pins->get_ith_cell(0), nullptr);
        ASSERT_NE(pins->get_ith_cell(1), nullptr);
        EXPECT_EQ(monitor::GetCacheShardDiskUsageBytesForTest(
                      shard, CellDataType::SCALAR_FIELD),
                  before + 384);
    }

    ASSERT_TRUE(slot->ManualEvictAll());
    EXPECT_EQ(monitor::GetCacheShardDiskUsageBytesForTest(
                  shard, CellDataType::SCALAR_FIELD),
              before);
}

TEST(CacheShardDiskUsageTest, IgnoresEmptyShard) {
    std::unique_ptr<Translator<TestDiskCell>> translator =
        std::make_unique<TestDiskTranslator>(
            "cache_empty_shard_disk_usage",
            "",
            std::vector<ResourceUsage>{{0, 128}});
    auto slot = Manager::GetInstance().CreateCacheSlot(std::move(translator));

    {
        auto pins = SemiInlineGet(slot->PinCells(nullptr, {0}));
        ASSERT_NE(pins, nullptr);
        ASSERT_NE(pins->get_ith_cell(0), nullptr);
    }

    EXPECT_TRUE(slot->ManualEvictAll());
}

}  // namespace milvus::cachinglayer
