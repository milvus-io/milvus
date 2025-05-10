#include <gtest/gtest.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/InlineExecutor.h>
#include <folly/futures/Future.h>

#include <chrono>
#include <memory>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>
#include <utility>
#include <unordered_map>
#include <unordered_set>

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "cachinglayer/lrucache/DList.h"
#include "cachinglayer/lrucache/ListNode.h"
#include "cachinglayer_test_utils.h"

using namespace milvus::cachinglayer;
using namespace milvus::cachinglayer::internal;
using cl_uid_t = milvus::cachinglayer::uid_t;

struct TestCell {
    int data;
    cid_t cid;

    TestCell(int d, cid_t id) : data(d), cid(id) {
    }

    size_t
    CellByteSize() const {
        return sizeof(data) + sizeof(cid);
    }
};

class MockTranslator : public Translator<TestCell> {
 public:
    MockTranslator(std::vector<std::pair<cid_t, int64_t>> cell_sizes,
                   std::unordered_map<cl_uid_t, cid_t> uid_to_cid_map,
                   const std::string& key,
                   StorageType storage_type,
                   bool for_concurrent_test = false)
        : uid_to_cid_map_(std::move(uid_to_cid_map)),
          key_(key),
          meta_(
              storage_type, CacheWarmupPolicy::CacheWarmupPolicy_Disable, true),
          num_unique_cids_(cell_sizes.size()),
          for_concurrent_test_(for_concurrent_test) {
        cid_set_.reserve(cell_sizes.size());
        cell_sizes_.reserve(cell_sizes.size());
        for (const auto& pair : cell_sizes) {
            cid_t cid = pair.first;
            int64_t size = pair.second;
            cid_set_.insert(cid);
            cell_sizes_[cid] = size;
            cid_load_delay_ms_[cid] = 0;
        }
    }

    size_t
    num_cells() const override {
        return num_unique_cids_;
    }

    cid_t
    cell_id_of(cl_uid_t uid) const override {
        auto it = uid_to_cid_map_.find(uid);
        if (it != uid_to_cid_map_.end()) {
            if (cid_set_.count(it->second)) {
                return it->second;
            }
        }
        return static_cast<cid_t>(num_unique_cids_);
    }

    ResourceUsage
    estimated_byte_size_of_cell(cid_t cid) const override {
        auto it = cell_sizes_.find(cid);
        if (it != cell_sizes_.end()) {
            return ResourceUsage{it->second, 0};
        }
        return ResourceUsage{1, 0};
    }

    const std::string&
    key() const override {
        return key_;
    }

    Meta*
    meta() override {
        return &meta_;
    }

    std::vector<std::pair<cid_t, std::unique_ptr<TestCell>>>
    get_cells(const std::vector<cid_t>& cids) override {
        if (!for_concurrent_test_) {
            get_cells_call_count_++;
            requested_cids_.push_back(cids);
        }

        if (load_should_throw_) {
            throw std::runtime_error("Simulated load error");
        }

        std::vector<std::pair<cid_t, std::unique_ptr<TestCell>>> result;
        for (cid_t cid : cids) {
            auto delay_it = cid_load_delay_ms_.find(cid);
            if (delay_it != cid_load_delay_ms_.end() && delay_it->second > 0) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(delay_it->second));
            }

            result.emplace_back(
                cid,
                std::make_unique<TestCell>(static_cast<int>(cid * 10), cid));

            if (auto extra_cids = extra_cids_.find(cid);
                extra_cids != extra_cids_.end()) {
                for (cid_t extra_cid : extra_cids->second) {
                    // if extra cid is not explicitly requested, and not yet added as extra cell by other
                    // cells, add it to the result.
                    if (std::find(cids.begin(), cids.end(), extra_cid) ==
                            cids.end() &&
                        std::find_if(result.begin(),
                                     result.end(),
                                     [extra_cid](const auto& pair) {
                                         return pair.first == extra_cid;
                                     }) == result.end()) {
                        result.emplace_back(
                            extra_cid,
                            std::make_unique<TestCell>(
                                static_cast<int>(extra_cid * 10), extra_cid));
                    }
                }
            }
        }
        return result;
    }

    void
    SetCidLoadDelay(const std::unordered_map<cid_t, int>& delays) {
        for (const auto& pair : delays) {
            cid_load_delay_ms_[pair.first] = pair.second;
        }
    }
    void
    SetShouldThrow(bool should_throw) {
        load_should_throw_ = should_throw;
    }
    // for some cid, translator will return extra cells.
    void
    SetExtraReturnCids(
        std::unordered_map<cid_t, std::vector<cid_t>> extra_cids) {
        extra_cids_ = extra_cids;
    }
    int
    GetCellsCallCount() const {
        EXPECT_FALSE(for_concurrent_test_);
        return get_cells_call_count_;
    }
    const std::vector<std::vector<cid_t>>&
    GetRequestedCids() const {
        EXPECT_FALSE(for_concurrent_test_);
        return requested_cids_;
    }
    void
    ResetCounters() {
        ASSERT_FALSE(for_concurrent_test_);
        get_cells_call_count_ = 0;
        requested_cids_.clear();
    }

 private:
    std::unordered_map<cl_uid_t, cid_t> uid_to_cid_map_;
    std::unordered_map<cid_t, int64_t> cell_sizes_;
    std::unordered_set<cid_t> cid_set_;
    const size_t num_unique_cids_;
    const std::string key_;
    Meta meta_;

    std::unordered_map<cid_t, int> cid_load_delay_ms_;
    bool load_should_throw_ = false;
    std::unordered_map<cid_t, std::vector<cid_t>> extra_cids_;
    std::atomic<int> get_cells_call_count_ = 0;
    std::vector<std::vector<cid_t>> requested_cids_;

    // this class is not concurrent safe, so if for concurrent test, do not track usage
    bool for_concurrent_test_ = false;
};

class CacheSlotTest : public ::testing::Test {
 protected:
    std::unique_ptr<DList> dlist_;
    MockTranslator* translator_ = nullptr;
    std::shared_ptr<CacheSlot<TestCell>> cache_slot_;

    std::vector<std::pair<cid_t, int64_t>> cell_sizes_ = {
        {0, 50}, {1, 150}, {2, 100}, {3, 200}, {4, 75}};
    std::unordered_map<cl_uid_t, cid_t> uid_to_cid_map_ = {{10, 0},
                                                           {11, 0},
                                                           {20, 1},
                                                           {30, 2},
                                                           {31, 2},
                                                           {32, 2},
                                                           {40, 3},
                                                           {50, 4},
                                                           {51, 4}};

    size_t NUM_UNIQUE_CIDS = 5;
    int64_t TOTAL_CELL_SIZE_BYTES = 50 + 150 + 100 + 200 + 75;
    int64_t MEMORY_LIMIT = TOTAL_CELL_SIZE_BYTES * 2;
    static constexpr int64_t DISK_LIMIT = 0;
    const std::string SLOT_KEY = "test_slot";

    void
    SetUp() override {
        auto limit = ResourceUsage{MEMORY_LIMIT, DISK_LIMIT};
        dlist_ = std::make_unique<DList>(
            limit, limit, limit, EvictionConfig{10, 600});

        auto temp_translator_uptr = std::make_unique<MockTranslator>(
            cell_sizes_, uid_to_cid_map_, SLOT_KEY, StorageType::MEMORY);
        translator_ = temp_translator_uptr.get();
        cache_slot_ = std::make_shared<CacheSlot<TestCell>>(
            std::move(temp_translator_uptr), dlist_.get());
    }

    void
    TearDown() override {
        cache_slot_.reset();
        dlist_.reset();
    }
};

TEST_F(CacheSlotTest, Initialization) {
    ASSERT_EQ(cache_slot_->num_cells(), NUM_UNIQUE_CIDS);
}

TEST_F(CacheSlotTest, PinSingleCellSuccess) {
    cl_uid_t target_uid = 30;
    cid_t expected_cid = 2;
    ResourceUsage expected_size =
        translator_->estimated_byte_size_of_cell(expected_cid);

    translator_->ResetCounters();
    auto future = cache_slot_->PinCells({target_uid});
    auto accessor = SemiInlineGet(std::move(future));

    ASSERT_NE(accessor, nullptr);
    ASSERT_EQ(translator_->GetCellsCallCount(), 1);
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    ASSERT_EQ(translator_->GetRequestedCids()[0].size(), 1);
    EXPECT_EQ(translator_->GetRequestedCids()[0][0], expected_cid);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);

    TestCell* cell = accessor->get_cell_of(target_uid);
    ASSERT_NE(cell, nullptr);
    EXPECT_EQ(cell->cid, expected_cid);
    EXPECT_EQ(cell->data, expected_cid * 10);

    TestCell* cell_by_index = accessor->get_ith_cell(expected_cid);
    ASSERT_EQ(cell, cell_by_index);
}

TEST_F(CacheSlotTest, PinMultipleCellsSuccess) {
    std::vector<cl_uid_t> target_uids = {10, 40, 51};
    std::vector<cid_t> expected_cids = {0, 3, 4};
    std::sort(expected_cids.begin(), expected_cids.end());
    ResourceUsage expected_total_size;
    for (cid_t cid : expected_cids) {
        expected_total_size += translator_->estimated_byte_size_of_cell(cid);
    }

    translator_->ResetCounters();
    auto future = cache_slot_->PinCells(target_uids);
    auto accessor = SemiInlineGet(std::move(future));

    ASSERT_NE(accessor, nullptr);
    ASSERT_EQ(translator_->GetCellsCallCount(), 1);
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    auto requested = translator_->GetRequestedCids()[0];
    std::sort(requested.begin(), requested.end());
    ASSERT_EQ(requested.size(), expected_cids.size());
    EXPECT_EQ(requested, expected_cids);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_total_size);

    for (cl_uid_t uid : target_uids) {
        cid_t cid = uid_to_cid_map_.at(uid);
        TestCell* cell = accessor->get_cell_of(uid);
        ASSERT_NE(cell, nullptr);
        EXPECT_EQ(cell->cid, cid);
        EXPECT_EQ(cell->data, cid * 10);
    }
}

TEST_F(CacheSlotTest, PinMultipleUidsMappingToSameCid) {
    std::vector<cl_uid_t> target_uids = {30, 50, 31, 51, 32};
    std::vector<cid_t> expected_unique_cids = {2, 4};
    std::sort(expected_unique_cids.begin(), expected_unique_cids.end());
    ResourceUsage expected_total_size;
    for (cid_t cid : expected_unique_cids) {
        expected_total_size += translator_->estimated_byte_size_of_cell(cid);
    }

    translator_->ResetCounters();
    auto future = cache_slot_->PinCells(target_uids);
    auto accessor = SemiInlineGet(std::move(future));

    ASSERT_NE(accessor, nullptr);
    ASSERT_EQ(translator_->GetCellsCallCount(), 1);
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    auto requested = translator_->GetRequestedCids()[0];
    std::sort(requested.begin(), requested.end());
    ASSERT_EQ(requested.size(), expected_unique_cids.size());
    EXPECT_EQ(requested, expected_unique_cids);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_total_size);

    TestCell* cell2_uid30 = accessor->get_cell_of(30);
    TestCell* cell2_uid31 = accessor->get_cell_of(31);
    TestCell* cell4_uid50 = accessor->get_cell_of(50);
    TestCell* cell4_uid51 = accessor->get_cell_of(51);
    ASSERT_NE(cell2_uid30, nullptr);
    ASSERT_NE(cell4_uid50, nullptr);
    EXPECT_EQ(cell2_uid30->cid, 2);
    EXPECT_EQ(cell4_uid50->cid, 4);
    EXPECT_EQ(cell2_uid30, cell2_uid31);
    EXPECT_EQ(cell4_uid50, cell4_uid51);
}

TEST_F(CacheSlotTest, PinInvalidUid) {
    cl_uid_t invalid_uid = 999;
    cl_uid_t valid_uid = 10;
    std::vector<cl_uid_t> target_uids = {valid_uid, invalid_uid};

    translator_->ResetCounters();
    auto future = cache_slot_->PinCells(target_uids);

    EXPECT_THROW(
        {
            try {
                SemiInlineGet(std::move(future));
            } catch (const std::invalid_argument& e) {
                std::string error_what = e.what();
                EXPECT_TRUE(error_what.find("out of range") !=
                                std::string::npos ||
                            error_what.find("invalid") != std::string::npos);
                throw;
            }
        },
        std::invalid_argument);

    EXPECT_EQ(translator_->GetCellsCallCount(), 0);
}

TEST_F(CacheSlotTest, LoadFailure) {
    cl_uid_t target_uid = 20;
    cid_t expected_cid = 1;

    translator_->ResetCounters();
    translator_->SetShouldThrow(true);

    auto future = cache_slot_->PinCells({target_uid});

    EXPECT_THROW(
        {
            try {
                SemiInlineGet(std::move(future));
            } catch (const std::runtime_error& e) {
                std::string error_what = e.what();
                EXPECT_TRUE(error_what.find("Simulated load error") !=
                                std::string::npos ||
                            error_what.find("Failed to load") !=
                                std::string::npos ||
                            error_what.find("Exception during Future") !=
                                std::string::npos);
                throw;
            }
        },
        std::runtime_error);

    ASSERT_EQ(translator_->GetCellsCallCount(), 1);
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    ASSERT_EQ(translator_->GetRequestedCids()[0].size(), 1);
    EXPECT_EQ(translator_->GetRequestedCids()[0][0], expected_cid);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), ResourceUsage{});
}

TEST_F(CacheSlotTest, PinAlreadyLoadedCell) {
    cl_uid_t target_uid = 40;
    cid_t expected_cid = 3;
    ResourceUsage expected_size =
        translator_->estimated_byte_size_of_cell(expected_cid);

    translator_->ResetCounters();

    auto future1 = cache_slot_->PinCells({target_uid});
    auto accessor1 = SemiInlineGet(std::move(future1));
    ASSERT_NE(accessor1, nullptr);
    ASSERT_EQ(translator_->GetCellsCallCount(), 1);
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    ASSERT_EQ(translator_->GetRequestedCids()[0][0], expected_cid);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);
    TestCell* cell1 = accessor1->get_cell_of(target_uid);
    ASSERT_NE(cell1, nullptr);

    translator_->ResetCounters();
    auto future2 = cache_slot_->PinCells({target_uid});
    auto accessor2 = SemiInlineGet(std::move(future2));
    ASSERT_NE(accessor2, nullptr);

    EXPECT_EQ(translator_->GetCellsCallCount(), 0);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);

    TestCell* cell2 = accessor2->get_cell_of(target_uid);
    ASSERT_NE(cell2, nullptr);
    EXPECT_EQ(cell1, cell2);

    accessor1.reset();
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);
    TestCell* cell_after_unpin = accessor2->get_cell_of(target_uid);
    ASSERT_NE(cell_after_unpin, nullptr);
    EXPECT_EQ(cell_after_unpin, cell2);
}

TEST_F(CacheSlotTest, PinAlreadyLoadedCellViaDifferentUid) {
    cl_uid_t uid1 = 30;
    cl_uid_t uid2 = 31;
    cid_t expected_cid = 2;
    ResourceUsage expected_size =
        translator_->estimated_byte_size_of_cell(expected_cid);

    translator_->ResetCounters();

    auto future1 = cache_slot_->PinCells({uid1});
    auto accessor1 = SemiInlineGet(std::move(future1));
    ASSERT_NE(accessor1, nullptr);
    ASSERT_EQ(translator_->GetCellsCallCount(), 1);
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    ASSERT_EQ(translator_->GetRequestedCids()[0][0], expected_cid);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);
    TestCell* cell1 = accessor1->get_cell_of(uid1);
    ASSERT_NE(cell1, nullptr);
    EXPECT_EQ(cell1->cid, expected_cid);

    translator_->ResetCounters();
    auto future2 = cache_slot_->PinCells({uid2});
    auto accessor2 = SemiInlineGet(std::move(future2));
    ASSERT_NE(accessor2, nullptr);

    EXPECT_EQ(translator_->GetCellsCallCount(), 0);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);

    TestCell* cell2 = accessor2->get_cell_of(uid2);
    ASSERT_NE(cell2, nullptr);
    EXPECT_EQ(cell2->cid, expected_cid);
    EXPECT_EQ(cell1, cell2);

    accessor1.reset();
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);
    TestCell* cell_after_unpin_uid1 = accessor2->get_cell_of(uid1);
    TestCell* cell_after_unpin_uid2 = accessor2->get_cell_of(uid2);
    ASSERT_NE(cell_after_unpin_uid1, nullptr);
    ASSERT_NE(cell_after_unpin_uid2, nullptr);
    EXPECT_EQ(cell_after_unpin_uid1, cell2);
    EXPECT_EQ(cell_after_unpin_uid2, cell2);
}

TEST_F(CacheSlotTest, TranslatorReturnsExtraCells) {
    cl_uid_t requested_uid = 10;
    cid_t requested_cid = 0;
    cid_t extra_cid = 1;
    cl_uid_t extra_uid = 20;

    ResourceUsage expected_size =
        translator_->estimated_byte_size_of_cell(requested_cid) +
        translator_->estimated_byte_size_of_cell(extra_cid);

    translator_->ResetCounters();
    translator_->SetExtraReturnCids({{requested_cid, {extra_cid}}});

    auto future = cache_slot_->PinCells({requested_uid});
    auto accessor = SemiInlineGet(std::move(future));

    ASSERT_NE(accessor, nullptr);
    ASSERT_EQ(translator_->GetCellsCallCount(), 1);
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    EXPECT_EQ(translator_->GetRequestedCids()[0],
              std::vector<cid_t>{requested_cid});
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);

    TestCell* requested_cell = accessor->get_cell_of(requested_uid);
    ASSERT_NE(requested_cell, nullptr);
    EXPECT_EQ(requested_cell->cid, requested_cid);

    translator_->ResetCounters();
    auto future_extra = cache_slot_->PinCells({extra_uid});
    auto accessor_extra = SemiInlineGet(std::move(future_extra));

    ASSERT_NE(accessor_extra, nullptr);
    EXPECT_EQ(translator_->GetCellsCallCount(), 0);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), expected_size);

    TestCell* extra_cell = accessor_extra->get_cell_of(extra_uid);
    ASSERT_NE(extra_cell, nullptr);
    EXPECT_EQ(extra_cell->cid, extra_cid);
}

TEST_F(CacheSlotTest, EvictionTest) {
    // Sizes: 0:50, 1:150, 2:100, 3:200
    ResourceUsage new_limit = ResourceUsage(300, 0);
    ResourceUsage new_high_watermark = ResourceUsage(250, 0);
    ResourceUsage new_low_watermark = ResourceUsage(200, 0);
    EXPECT_TRUE(dlist_->UpdateLimit(new_limit));
    dlist_->UpdateHighWatermark(new_high_watermark);
    dlist_->UpdateLowWatermark(new_low_watermark);
    EXPECT_EQ(DListTestFriend::get_max_memory(*dlist_), new_limit);

    std::vector<cl_uid_t> uids_012 = {10, 20, 30};
    std::vector<cid_t> cids_012 = {0, 1, 2};
    ResourceUsage size_012 = translator_->estimated_byte_size_of_cell(0) +
                             translator_->estimated_byte_size_of_cell(1) +
                             translator_->estimated_byte_size_of_cell(2);
    ASSERT_EQ(size_012, ResourceUsage(50 + 150 + 100, 0));

    // 1. Load cells 0, 1, 2
    translator_->ResetCounters();
    auto future1 = cache_slot_->PinCells(uids_012);
    auto accessor1 = SemiInlineGet(std::move(future1));
    ASSERT_NE(accessor1, nullptr);
    EXPECT_EQ(translator_->GetCellsCallCount(), 1);
    auto requested1 = translator_->GetRequestedCids()[0];
    std::sort(requested1.begin(), requested1.end());
    EXPECT_EQ(requested1, cids_012);
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_), size_012);

    // 2. Unpin 0, 1, 2
    accessor1.reset();
    EXPECT_EQ(DListTestFriend::get_used_memory(*dlist_),
              size_012);  // Still in cache

    // 3. Load cell 3 (size 200), requires eviction
    cl_uid_t uid_3 = 40;
    cid_t cid_3 = 3;
    ResourceUsage size_3 = translator_->estimated_byte_size_of_cell(cid_3);
    ASSERT_EQ(size_3, ResourceUsage(200, 0));

    translator_->ResetCounters();
    auto future2 = cache_slot_->PinCells({uid_3});
    auto accessor2 = SemiInlineGet(std::move(future2));
    ASSERT_NE(accessor2, nullptr);

    EXPECT_EQ(translator_->GetCellsCallCount(),
              1);  // Load was called for cell 3
    ASSERT_EQ(translator_->GetRequestedCids().size(), 1);
    EXPECT_EQ(translator_->GetRequestedCids()[0], std::vector<cid_t>{cid_3});

    // Verify eviction happened
    ResourceUsage used_after_evict1 = DListTestFriend::get_used_memory(*dlist_);
    EXPECT_LE(used_after_evict1.memory_bytes, new_limit.memory_bytes);
    EXPECT_GE(used_after_evict1.memory_bytes, size_3.memory_bytes);
    EXPECT_LT(
        used_after_evict1.memory_bytes,
        size_012.memory_bytes + size_3.memory_bytes);  // Eviction occurred
}

class CacheSlotConcurrentTest : public CacheSlotTest,
                                public ::testing::WithParamInterface<bool> {};

TEST_P(CacheSlotConcurrentTest, ConcurrentAccessMultipleSlots) {
    // Slot 1 Cells: 0-4 (Sizes: 50, 60, 70, 80, 90) -> Total 350
    // Slot 2 Cells: 0-4 (Sizes: 55, 65, 75, 85, 95) -> Total 375
    // Total potential size = 350 + 375 = 725
    // Set limit lower than total potential size to force eviction
    ResourceUsage new_limit = ResourceUsage(700, 0);
    ResourceUsage new_high_watermark = ResourceUsage(650, 0);
    ResourceUsage new_low_watermark = ResourceUsage(600, 0);
    ASSERT_TRUE(dlist_->UpdateLimit(new_limit));
    dlist_->UpdateHighWatermark(new_high_watermark);
    dlist_->UpdateLowWatermark(new_low_watermark);
    EXPECT_EQ(DListTestFriend::get_max_memory(*dlist_).memory_bytes,
              new_limit.memory_bytes);

    // 1. Setup CacheSlots sharing dlist_
    std::vector<std::pair<cid_t, int64_t>> cell_sizes_1 = {
        {0, 50}, {1, 60}, {2, 70}, {3, 80}, {4, 90}};
    std::unordered_map<cl_uid_t, cid_t> uid_map_1 = {
        {1000, 0}, {1001, 1}, {1002, 2}, {1003, 3}, {1004, 4}};
    auto translator_1_ptr =
        std::make_unique<MockTranslator>(cell_sizes_1,
                                         uid_map_1,
                                         "slot1",
                                         StorageType::MEMORY,
                                         /*for_concurrent_test*/ true);
    MockTranslator* translator_1 = translator_1_ptr.get();
    auto slot1 = std::make_shared<CacheSlot<TestCell>>(
        std::move(translator_1_ptr), dlist_.get());

    std::vector<std::pair<cid_t, int64_t>> cell_sizes_2 = {
        {0, 55}, {1, 65}, {2, 75}, {3, 85}, {4, 95}};
    std::unordered_map<cl_uid_t, cid_t> uid_map_2 = {
        {2000, 0}, {2001, 1}, {2002, 2}, {2003, 3}, {2004, 4}};
    auto translator_2_ptr =
        std::make_unique<MockTranslator>(cell_sizes_2,
                                         uid_map_2,
                                         "slot2",
                                         StorageType::MEMORY,
                                         /*for_concurrent_test*/ true);
    MockTranslator* translator_2 = translator_2_ptr.get();
    auto slot2 = std::make_shared<CacheSlot<TestCell>>(
        std::move(translator_2_ptr), dlist_.get());

    bool with_bonus_cells = GetParam();
    if (with_bonus_cells) {
        // Configure translators to return bonus cells
        std::unordered_map<cid_t, std::vector<cid_t>> bonus_map_1{
            {0, {2}},
            {1, {3}},
            {2, {1, 4}},
            {3, {0}},
            {4, {2, 3}},
        };
        std::unordered_map<cid_t, std::vector<cid_t>> bonus_map_2{
            {0, {1, 4}},
            {1, {2, 3}},
            {2, {0}},
            {3, {0, 1}},
            {4, {2}},
        };
        translator_1->SetExtraReturnCids(bonus_map_1);
        translator_2->SetExtraReturnCids(bonus_map_2);
    }

    std::vector<std::shared_ptr<CacheSlot<TestCell>>> slots = {slot1, slot2};
    // Store uid maps in a structure easily accessible by slot index
    std::vector<std::vector<cl_uid_t>> slot_uids;
    slot_uids.resize(slots.size());
    std::vector<std::unordered_map<cl_uid_t, cid_t>> uid_to_cid_maps = {
        uid_map_1, uid_map_2};
    for (const auto& pair : uid_map_1) slot_uids[0].push_back(pair.first);
    for (const auto& pair : uid_map_2) slot_uids[1].push_back(pair.first);

    // 2. Setup Thread Pool and Concurrency Parameters
    // at most 6 cells can be pinned at the same time, thus will never exceed the limit.
    int num_threads = 6;
    int ops_per_thread = 200;
    // 1 extra thread to work with slot3
    folly::CPUThreadPoolExecutor executor(num_threads + 1);
    std::vector<folly::Future<folly::Unit>> futures;
    std::atomic<bool> test_failed{false};

    // 3. Launch Threads to Perform Concurrent Pin/Get/Verify/Unpin
    for (int i = 0; i < num_threads; ++i) {
        futures.push_back(folly::via(&executor, [&, i, tid = i]() {
            // Seed random generator uniquely for each thread
            std::mt19937 gen(
                std::hash<std::thread::id>{}(std::this_thread::get_id()) + tid);
            std::uniform_int_distribution<> slot_dist(0, slots.size() - 1);
            std::uniform_int_distribution<> sleep_dist(5, 15);

            for (int j = 0; j < ops_per_thread && !test_failed.load(); ++j) {
                int slot_idx = slot_dist(gen);
                auto& current_slot = slots[slot_idx];
                auto& current_slot_uids = slot_uids[slot_idx];
                auto& current_uid_to_cid_map = uid_to_cid_maps[slot_idx];

                std::uniform_int_distribution<> uid_idx_dist(
                    0, current_slot_uids.size() - 1);
                cl_uid_t target_uid = current_slot_uids[uid_idx_dist(gen)];
                cid_t expected_cid = current_uid_to_cid_map.at(target_uid);
                int expected_data = static_cast<int>(expected_cid * 10);

                try {
                    auto accessor = current_slot->PinCells({target_uid}).get();

                    if (!accessor) {
                        ADD_FAILURE()
                            << "T" << tid << " Op" << j
                            << ": PinCells returned null accessor for UID "
                            << target_uid;
                        test_failed = true;
                        break;
                    }

                    TestCell* cell = accessor->get_cell_of(target_uid);
                    if (!cell) {
                        ADD_FAILURE() << "T" << tid << " Op" << j
                                      << ": get_cell_of returned null for UID "
                                      << target_uid;
                        test_failed = true;
                        break;
                    }

                    if (cell->cid != expected_cid) {
                        ADD_FAILURE() << "T" << tid << " Op" << j
                                      << ": Incorrect CID for UID "
                                      << target_uid << ". Slot: " << slot_idx
                                      << ", Expected: " << expected_cid
                                      << ", Got: " << cell->cid;
                        test_failed = true;
                        break;
                    }
                    if (cell->data != expected_data) {
                        ADD_FAILURE() << "T" << tid << " Op" << j
                                      << ": Incorrect Data for UID "
                                      << target_uid << ". Slot: " << slot_idx
                                      << ", Expected: " << expected_data
                                      << ", Got: " << cell->data;
                        test_failed = true;
                        break;
                    }
                    int sleep_ms = sleep_dist(gen);
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(sleep_ms));
                } catch (const std::exception& e) {
                    ADD_FAILURE()
                        << "T" << tid << " Op" << j << ": Exception for UID "
                        << target_uid << ", Slot: " << slot_idx
                        << ". What: " << e.what();
                    test_failed = true;
                } catch (...) {
                    ADD_FAILURE() << "T" << tid << " Op" << j
                                  << ": Unknown exception for UID "
                                  << target_uid << ", Slot: " << slot_idx;
                    test_failed = true;
                }
            }
        }));
    }

    // number of ops between recreating slot3
    const int recreate_interval = 25;
    auto dlist_ptr = dlist_.get();
    std::vector<std::pair<cid_t, int64_t>> cell_sizes_3 = {
        {0, 40}, {1, 50}, {2, 60}, {3, 70}, {4, 80}};
    std::unordered_map<cl_uid_t, cid_t> uid_map_3 = {
        {3000, 0}, {3001, 1}, {3002, 2}, {3003, 3}, {3004, 4}};
    std::vector<cl_uid_t> slot3_uids = {3000, 3001, 3002, 3003, 3004};
    auto create_new_slot3 = [&]() {
        auto translator_3_ptr =
            std::make_unique<MockTranslator>(cell_sizes_3,
                                             uid_map_3,
                                             "slot3",
                                             StorageType::MEMORY,
                                             /*for_concurrent_test*/ true);
        auto sl = std::make_shared<CacheSlot<TestCell>>(
            std::move(translator_3_ptr), dlist_ptr);
        return sl;
    };
    std::shared_ptr<CacheSlot<TestCell>> slot3 = create_new_slot3();
    futures.push_back(folly::via(&executor, [&, tid = num_threads]() {
        std::mt19937 gen(
            std::hash<std::thread::id>{}(std::this_thread::get_id()) + tid);
        std::uniform_int_distribution<> sleep_dist(5, 15);
        std::uniform_int_distribution<> recreate_sleep_dist(20, 30);
        std::uniform_int_distribution<> uid_idx_dist(0, slot3_uids.size() - 1);
        int ops_since_recreate = 0;

        for (int j = 0; j < ops_per_thread && !test_failed.load(); ++j) {
            cl_uid_t target_uid = slot3_uids[uid_idx_dist(gen)];
            cid_t expected_cid = uid_map_3.at(target_uid);
            int expected_data = static_cast<int>(expected_cid * 10);
            try {
                auto accessor = slot3->PinCells({target_uid}).get();
                if (!accessor) {
                    ADD_FAILURE()
                        << "T" << tid << " Op" << j
                        << ": PinCells returned null accessor for UID "
                        << target_uid;
                    test_failed = true;
                    break;
                }

                TestCell* cell = accessor->get_cell_of(target_uid);
                if (!cell) {
                    ADD_FAILURE()
                        << "T" << tid << " Op" << j
                        << ": get_cell_of returned null for UID " << target_uid;
                    test_failed = true;
                    break;
                }

                if (cell->cid != expected_cid) {
                    ADD_FAILURE() << "T" << tid << " Op" << j
                                  << ": Incorrect CID for UID " << target_uid
                                  << ". Slot: 3"
                                  << ", Expected: " << expected_cid
                                  << ", Got: " << cell->cid;
                    test_failed = true;
                    break;
                }

                if (cell->data != expected_data) {
                    ADD_FAILURE() << "T" << tid << " Op" << j
                                  << ": Incorrect Data for UID " << target_uid
                                  << ". Slot: 3"
                                  << ", Expected: " << expected_data
                                  << ", Got: " << cell->data;
                    test_failed = true;
                    break;
                }

                if (ops_since_recreate >= recreate_interval) {
                    slot3 = nullptr;
                    int sleep_ms = recreate_sleep_dist(gen);
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(sleep_ms));
                    slot3 = create_new_slot3();
                    ops_since_recreate = 0;
                } else {
                    ops_since_recreate++;
                    int sleep_ms = sleep_dist(gen);
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(sleep_ms));
                }
            } catch (const std::exception& e) {
                ADD_FAILURE()
                    << "T" << tid << " Op" << j << ": Exception for UID "
                    << target_uid << ", Slot: 3"
                    << ". What: " << e.what();
                test_failed = true;
            } catch (...) {
                ADD_FAILURE() << "T" << tid << " Op" << j
                              << ": Unknown exception for UID " << target_uid
                              << ", Slot: 3";
                test_failed = true;
            }
        }
    }));

    // 4. Wait for all threads to complete
    try {
        folly::collectAll(futures).get();
    } catch (const std::exception& e) {
        FAIL() << "Exception waiting for thread pool completion: " << e.what();
    } catch (...) {
        FAIL() << "Unknown exception waiting for thread pool completion.";
    }

    ASSERT_FALSE(test_failed.load())
        << "Test failed due to assertion failures within threads.";

    ResourceUsage final_memory_usage =
        DListTestFriend::get_used_memory(*dlist_);

    // bonus cell may cause memory usage to exceed the limit
    if (!with_bonus_cells) {
        EXPECT_LE(final_memory_usage.memory_bytes, new_limit.memory_bytes)
            << "Final memory usage (" << final_memory_usage.memory_bytes
            << ") exceeds the limit (" << new_limit.memory_bytes
            << ") after concurrent access.";
    }

    DListTestFriend::verify_integrity(dlist_.get());
}

INSTANTIATE_TEST_SUITE_P(BonusCellParam,
                         CacheSlotConcurrentTest,
                         ::testing::Bool(),
                         [](const ::testing::TestParamInfo<bool>& info) {
                             return info.param ? "WithBonusCells"
                                               : "NoBonusCells";
                         });
