#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <thread>
#include <vector>
#include <memory>

#include "cachinglayer/lrucache/DList.h"
#include "cachinglayer/Utils.h"
#include "common/EasyAssert.h"
#include "mock_list_node.h"
#include "cachinglayer_test_utils.h"

using namespace milvus::cachinglayer;
using namespace milvus::cachinglayer::internal;
using ::testing::StrictMock;
using DLF = DListTestFriend;

class DListTest : public ::testing::Test {
 protected:
    ResourceUsage initial_limit{100, 50};
    // Set watermarks relative to the limit
    ResourceUsage low_watermark{80, 40};   // 80%
    ResourceUsage high_watermark{90, 45};  // 90%
    // Use a very long interval to disable background eviction for most tests
    EvictionConfig eviction_config_{10,   // cache_touch_window (10 ms)
                                    10};  // eviction_interval (10 ms)

    std::unique_ptr<DList> dlist;
    // Keep track of nodes to prevent them from being deleted prematurely
    std::vector<std::shared_ptr<MockListNode>> managed_nodes;

    void
    SetUp() override {
        dlist = std::make_unique<DList>(
            initial_limit, low_watermark, high_watermark, eviction_config_);
        managed_nodes.clear();
    }

    void
    TearDown() override {
        managed_nodes.clear();
        dlist.reset();
    }

    // Helper to create a mock node, simulate loading it, and add it to the list.
    // Returns a raw pointer, but ownership is managed by the shared_ptr in managed_nodes.
    MockListNode*
    add_and_load_node(ResourceUsage size,
                      const std::string& key = "key",
                      cid_t cid = 0,
                      int pin_count = 0) {
        // Check if adding this node would exceed capacity before creating/adding it.
        // We want to use add_and_load_node to create a DList in valid state.
        ResourceUsage current_usage = get_used_memory();
        ResourceUsage limit = DLF::get_max_memory(*dlist);
        if (!limit.CanHold(current_usage + size)) {
            throw std::invalid_argument(
                "Adding this node would exceed capacity");
        }

        auto node_ptr = std::make_shared<StrictMock<MockListNode>>(
            dlist.get(), size, key, cid);
        managed_nodes.push_back(node_ptr);
        MockListNode* node = node_ptr.get();

        node->test_set_state(ListNode::State::LOADED);
        node->test_set_pin_count(pin_count);

        // Manually adjust used memory and list pointers
        DLF::test_add_used_memory(dlist.get(), size);
        DLF::test_push_head(dlist.get(), node);

        return node;
    }

    ResourceUsage
    get_used_memory() const {
        return DLF::get_used_memory(*dlist);
    }
};

TEST_F(DListTest, Initialization) {
    EXPECT_TRUE(dlist->IsEmpty());
    EXPECT_EQ(get_used_memory(), ResourceUsage{});
    EXPECT_EQ(DLF::get_head(*dlist), nullptr);
    EXPECT_EQ(DLF::get_tail(*dlist), nullptr);
}

TEST_F(DListTest, UpdateLimitIncrease) {
    MockListNode* node1 = add_and_load_node({10, 5});
    EXPECT_EQ(get_used_memory(), node1->size());

    ResourceUsage new_limit{200, 100};
    EXPECT_TRUE(dlist->UpdateLimit(new_limit));

    EXPECT_EQ(get_used_memory(), node1->size());
    DLF::verify_list(dlist.get(), {node1});
}

TEST_F(DListTest, UpdateLimitDecreaseNoEviction) {
    MockListNode* node1 = add_and_load_node({10, 5});
    ResourceUsage current_usage = node1->size();
    ASSERT_EQ(get_used_memory(), current_usage);

    ResourceUsage new_limit{50, 25};
    EXPECT_TRUE(dlist->UpdateLimit(new_limit));

    EXPECT_EQ(get_used_memory(), current_usage);
    DLF::verify_list(dlist.get(), {node1});
}

TEST_F(DListTest, UpdateLimitDecreaseWithEvictionLRU) {
    MockListNode* node1 = add_and_load_node({50, 20}, "key1");
    MockListNode* node2 = add_and_load_node({50, 30}, "key2");
    ResourceUsage usage_node1 = node1->size();
    ResourceUsage usage_node2 = node2->size();
    DLF::verify_list(dlist.get(), {node1, node2});
    EXPECT_EQ(get_used_memory(), usage_node1 + usage_node2);
    EXPECT_EQ(get_used_memory(), DLF::get_max_memory(*dlist));

    // Expect node1 to be evicted
    EXPECT_CALL(*node1, clear_data()).Times(1);
    EXPECT_CALL(*node2, clear_data()).Times(0);

    ResourceUsage new_limit{70, 40};
    EXPECT_TRUE(dlist->UpdateLimit(new_limit));

    EXPECT_EQ(get_used_memory(), usage_node2);
    DLF::verify_list(dlist.get(), {node2});
    EXPECT_FALSE(dlist->IsEmpty());
}

TEST_F(DListTest, UpdateLimitDecreaseWithEvictionMultiple) {
    MockListNode* node1 = add_and_load_node({30, 10}, "key1");
    MockListNode* node2 = add_and_load_node({30, 10}, "key2");
    MockListNode* node3 = add_and_load_node({30, 10}, "key3");
    ResourceUsage usage_node1 = node1->size();
    ResourceUsage usage_node2 = node2->size();
    ResourceUsage usage_node3 = node3->size();
    DLF::verify_list(dlist.get(), {node1, node2, node3});
    ASSERT_EQ(get_used_memory(), usage_node1 + usage_node2 + usage_node3);

    EXPECT_CALL(*node1, clear_data()).Times(1);
    EXPECT_CALL(*node2, clear_data()).Times(1);
    EXPECT_CALL(*node3, clear_data()).Times(0);

    ResourceUsage new_limit{40, 15};
    EXPECT_TRUE(dlist->UpdateLimit(new_limit));

    EXPECT_EQ(get_used_memory(), usage_node3);
    DLF::verify_list(dlist.get(), {node3});
}

TEST_F(DListTest, UpdateLimitSkipsPinned) {
    MockListNode* node1 = add_and_load_node({40, 15}, "key1", 0, 1);
    MockListNode* node2 = add_and_load_node({50, 25}, "key2");
    ResourceUsage usage_node1 = node1->size();
    ResourceUsage usage_node2 = node2->size();
    DLF::verify_list(dlist.get(), {node1, node2});
    ASSERT_EQ(get_used_memory(), usage_node1 + usage_node2);

    EXPECT_CALL(*node1, clear_data()).Times(0);
    EXPECT_CALL(*node2, clear_data()).Times(1);

    ResourceUsage new_limit{70, 40};
    EXPECT_TRUE(dlist->UpdateLimit(new_limit));

    EXPECT_EQ(get_used_memory(), usage_node1);
    DLF::verify_list(dlist.get(), {node1});
}

TEST_F(DListTest, UpdateLimitToZero) {
    MockListNode* node1 = add_and_load_node({10, 0});
    MockListNode* node2 = add_and_load_node({0, 5});
    EXPECT_CALL(*node1, clear_data()).Times(1);
    EXPECT_CALL(*node2, clear_data()).Times(1);

    EXPECT_TRUE(dlist->UpdateLimit({1, 1}));

    EXPECT_EQ(get_used_memory(), ResourceUsage{});
    EXPECT_TRUE(dlist->IsEmpty());
}

TEST_F(DListTest, UpdateLimitInvalid) {
    EXPECT_THROW(dlist->UpdateLimit({-10, 0}), milvus::SegcoreError);
    EXPECT_THROW(dlist->UpdateLimit({0, -5}), milvus::SegcoreError);
}

TEST_F(DListTest, ReserveMemorySufficient) {
    ResourceUsage size{20, 10};
    EXPECT_TRUE(dlist->reserveMemory(size));
    EXPECT_EQ(get_used_memory(), size);
}

TEST_F(DListTest, ReserveMemoryRequiresEviction) {
    MockListNode* node1 = add_and_load_node({40, 15}, "key1");
    MockListNode* node2 = add_and_load_node({50, 25}, "key2");
    ResourceUsage usage_node1 = node1->size();
    ResourceUsage usage_node2 = node2->size();
    DLF::verify_list(dlist.get(), {node1, node2});

    ASSERT_EQ(get_used_memory(), usage_node1 + usage_node2);

    EXPECT_CALL(*node1, clear_data()).Times(1);
    EXPECT_CALL(*node2, clear_data()).Times(0);

    // Limit: 100, 50, current usage: 90, 40, reserve: 20, 15.
    // Potential total: 110, 55. Need to free to low watermark 80, 40.
    ResourceUsage reserve_size{20, 15};
    // Evicting node1 ({40, 15}) is sufficient.
    EXPECT_TRUE(dlist->reserveMemory(reserve_size));

    EXPECT_EQ(get_used_memory(), usage_node2 + reserve_size);
    DLF::verify_list(dlist.get(), {node2});
}

TEST_F(DListTest, ReserveMemoryEvictPinnedSkipped) {
    MockListNode* node_pinned = add_and_load_node({40, 15}, "key_pinned", 0, 1);
    MockListNode* node_evict = add_and_load_node({50, 25}, "key_evict");
    ResourceUsage usage_pinned = node_pinned->size();
    ResourceUsage usage_evict = node_evict->size();
    DLF::verify_list(dlist.get(), {node_pinned, node_evict});

    ASSERT_EQ(get_used_memory(), usage_pinned + usage_evict);

    EXPECT_CALL(*node_pinned, clear_data()).Times(0);
    EXPECT_CALL(*node_evict, clear_data()).Times(1);

    ResourceUsage reserve_size{20, 20};
    EXPECT_TRUE(dlist->reserveMemory(reserve_size));

    EXPECT_EQ(get_used_memory(), usage_pinned + reserve_size);
    DLF::verify_list(dlist.get(), {node_pinned});
}

TEST_F(DListTest, ReserveMemoryEvictLockedSkipped) {
    MockListNode* node_locked = add_and_load_node({40, 15}, "key_locked");
    MockListNode* node_evict = add_and_load_node({50, 25}, "key_evict");
    ResourceUsage usage_locked = node_locked->size();
    ResourceUsage usage_evict = node_evict->size();
    DLF::verify_list(dlist.get(), {node_locked, node_evict});

    ASSERT_EQ(get_used_memory(), usage_locked + usage_evict);

    EXPECT_CALL(*node_locked, clear_data()).Times(0);
    EXPECT_CALL(*node_evict, clear_data()).Times(1);

    // Simulate locking the node during eviction attempt
    std::unique_lock locked_node_lock(node_locked->test_get_mutex());

    ResourceUsage reserve_size{20, 20};
    EXPECT_TRUE(dlist->reserveMemory(reserve_size));

    locked_node_lock.unlock();

    EXPECT_EQ(get_used_memory(), usage_locked + reserve_size);
    DLF::verify_list(dlist.get(), {node_locked});
}

TEST_F(DListTest, ReserveMemoryInsufficientEvenWithEviction) {
    MockListNode* node1 = add_and_load_node({10, 5});
    ResourceUsage usage_node1 = node1->size();
    ASSERT_EQ(get_used_memory(), usage_node1);

    ResourceUsage reserve_size{200, 100};

    EXPECT_FALSE(dlist->reserveMemory(reserve_size));

    EXPECT_EQ(get_used_memory(), usage_node1);
    EXPECT_FALSE(dlist->IsEmpty());
}

TEST_F(DListTest, TouchItemMovesToHead) {
    MockListNode* node1 = add_and_load_node({10, 0}, "key1");
    MockListNode* node2 = add_and_load_node({10, 0}, "key2");
    MockListNode* node3 = add_and_load_node({10, 0}, "key3");

    DLF::verify_list(dlist.get(), {node1, node2, node3});

    {
        std::unique_lock node_lock(node1->test_get_mutex());
        dlist->touchItem(node1);
    }

    DLF::verify_list(dlist.get(), {node2, node3, node1});
}

TEST_F(DListTest, TouchItemRefreshWindow) {
    MockListNode* node1 = add_and_load_node({10, 0}, "key1");
    MockListNode* node2 = add_and_load_node({10, 0}, "key2");

    DLF::verify_list(dlist.get(), {node1, node2});

    {
        std::unique_lock node_lock(node1->test_get_mutex());
        dlist->touchItem(node1);
    }
    DLF::verify_list(dlist.get(), {node2, node1});

    {
        std::unique_lock node_lock(node1->test_get_mutex());
        dlist->touchItem(node1);
    }
    DLF::verify_list(dlist.get(), {node2, node1});

    // Use eviction_config from dlist
    std::this_thread::sleep_for(dlist->eviction_config().cache_touch_window +
                                std::chrono::milliseconds(10));

    {
        std::unique_lock node_lock(node1->test_get_mutex());
        dlist->touchItem(node1);
    }
    DLF::verify_list(dlist.get(), {node2, node1});

    // Use eviction_config from dlist
    std::this_thread::sleep_for(dlist->eviction_config().cache_touch_window +
                                std::chrono::milliseconds(10));

    {
        std::unique_lock node_lock(node2->test_get_mutex());
        dlist->touchItem(node2);
    }
    DLF::verify_list(dlist.get(), {node1, node2});
}

TEST_F(DListTest, releaseMemory) {
    ResourceUsage initial_size{30, 15};
    DLF::test_add_used_memory(dlist.get(), initial_size);
    ASSERT_EQ(get_used_memory(), initial_size);

    ResourceUsage failed_load_size{10, 5};
    dlist->releaseMemory(failed_load_size);

    EXPECT_EQ(get_used_memory(), initial_size - failed_load_size);
}

TEST_F(DListTest, ReserveMemoryEvictOnlyMemoryNeeded) {
    MockListNode* node_disk_only = add_and_load_node({0, 20}, "disk_only");
    MockListNode* node_mixed = add_and_load_node({50, 10}, "mixed");
    ResourceUsage usage_disk = node_disk_only->size();
    ResourceUsage usage_mixed = node_mixed->size();
    DLF::verify_list(dlist.get(), {node_disk_only, node_mixed});
    ASSERT_EQ(get_used_memory(), usage_disk + usage_mixed);

    EXPECT_CALL(*node_disk_only, clear_data()).Times(0);
    EXPECT_CALL(*node_mixed, clear_data()).Times(1);

    // node_disk_only is at tail, but it contains no memory, and disk usage is below low watermark,
    // thus evicting it does not help. We need to evict node_mixed to free up memory.
    ResourceUsage reserve_size{60, 0};
    EXPECT_TRUE(dlist->reserveMemory(reserve_size));

    EXPECT_EQ(get_used_memory(), usage_disk + reserve_size);
    DLF::verify_list(dlist.get(), {node_disk_only});
}

TEST_F(DListTest, ReserveMemoryEvictOnlyDiskNeeded) {
    MockListNode* node_memory_only = add_and_load_node({40, 0}, "memory_only");
    MockListNode* node_mixed = add_and_load_node({20, 25}, "mixed");
    ResourceUsage usage_memory = node_memory_only->size();
    ResourceUsage usage_mixed = node_mixed->size();
    DLF::verify_list(dlist.get(), {node_memory_only, node_mixed});
    ASSERT_EQ(get_used_memory(), usage_memory + usage_mixed);

    EXPECT_CALL(*node_memory_only, clear_data()).Times(0);
    EXPECT_CALL(*node_mixed, clear_data()).Times(1);

    // node_memory_only is at tail, but it contains no disk, and memory usage is below low watermark,
    // thus evicting it does not help. We need to evict node_mixed to free up disk.
    ResourceUsage reserve_size{0, 30};
    EXPECT_TRUE(dlist->reserveMemory(reserve_size));

    EXPECT_EQ(get_used_memory(), usage_memory + reserve_size);
    DLF::verify_list(dlist.get(), {node_memory_only});
}

TEST_F(DListTest, ReserveMemoryEvictBothNeeded) {
    MockListNode* node1 = add_and_load_node({30, 5}, "node1");
    MockListNode* node2 = add_and_load_node({10, 15}, "node2");
    MockListNode* node3 = add_and_load_node({50, 25}, "node3");
    ResourceUsage usage1 = node1->size();
    ResourceUsage usage2 = node2->size();
    ResourceUsage usage3 = node3->size();
    DLF::verify_list(dlist.get(), {node1, node2, node3});
    ASSERT_EQ(get_used_memory(), usage1 + usage2 + usage3);

    EXPECT_CALL(*node1, clear_data()).Times(1);
    EXPECT_CALL(*node2, clear_data()).Times(1);
    EXPECT_CALL(*node3, clear_data()).Times(0);

    ResourceUsage reserve_size{10, 15};
    EXPECT_TRUE(dlist->reserveMemory(reserve_size));

    EXPECT_EQ(get_used_memory(), usage3 + reserve_size);
    DLF::verify_list(dlist.get(), {node3});
}

TEST_F(DListTest, ReserveToAboveLowWatermarkNoEviction) {
    // initial 40, 20
    MockListNode* node1 = add_and_load_node({30, 5}, "node1");
    MockListNode* node2 = add_and_load_node({10, 15}, "node2");
    ResourceUsage usage1 = node1->size();
    ResourceUsage usage2 = node2->size();
    DLF::verify_list(dlist.get(), {node1, node2});
    ASSERT_EQ(get_used_memory(), usage1 + usage2);

    // after reserve, 45, 22, end up in 85, 42, no eviction
    ResourceUsage reserve_size{45, 22};
    EXPECT_TRUE(dlist->reserveMemory(reserve_size));

    EXPECT_EQ(get_used_memory(), usage1 + usage2 + reserve_size);
    DLF::verify_list(dlist.get(), {node1, node2});
}

TEST_F(DListTest, ReserveToAboveHighWatermarkNoEvictionThenAutoEviction) {
    // initial 40, 20
    MockListNode* node1 = add_and_load_node({30, 15}, "node1");
    MockListNode* node2 = add_and_load_node({10, 5}, "node2");
    ResourceUsage usage1 = node1->size();
    ResourceUsage usage2 = node2->size();
    DLF::verify_list(dlist.get(), {node1, node2});
    ASSERT_EQ(get_used_memory(), usage1 + usage2);

    // after reserve, 55, 26, end up in 95, 46, above high watermark, no eviction
    ResourceUsage reserve_size{55, 26};
    EXPECT_TRUE(dlist->reserveMemory(reserve_size));

    EXPECT_EQ(get_used_memory(), usage1 + usage2 + reserve_size);
    DLF::verify_list(dlist.get(), {node1, node2});

    // wait for background eviction to run, current usage 95, 46, above high watermark.
    // reserved 55, 26 is considered pinned, thus evict node 1, resulting in 65, 31, below low watermark
    EXPECT_CALL(*node1, clear_data()).Times(1);
    std::this_thread::sleep_for(dlist->eviction_config().eviction_interval +
                                std::chrono::milliseconds(10));

    EXPECT_EQ(get_used_memory(), usage2 + reserve_size);
    DLF::verify_list(dlist.get(), {node2});
}

TEST_F(DListTest, ReserveMemoryFailsAllPinned) {
    MockListNode* node1 = add_and_load_node({40, 15}, "key1", 0, 1);
    MockListNode* node2 = add_and_load_node({50, 25}, "key2", 0, 1);
    ResourceUsage usage_node1 = node1->size();
    ResourceUsage usage_node2 = node2->size();
    DLF::verify_list(dlist.get(), {node1, node2});
    ASSERT_EQ(get_used_memory(), usage_node1 + usage_node2);

    EXPECT_CALL(*node1, clear_data()).Times(0);
    EXPECT_CALL(*node2, clear_data()).Times(0);

    ResourceUsage reserve_size{20, 20};
    EXPECT_FALSE(dlist->reserveMemory(reserve_size));

    EXPECT_EQ(get_used_memory(), usage_node1 + usage_node2);
    DLF::verify_list(dlist.get(), {node1, node2});
}

TEST_F(DListTest, ReserveMemoryFailsAllLocked) {
    MockListNode* node1 = add_and_load_node({40, 15}, "key1");
    MockListNode* node2 = add_and_load_node({50, 25}, "key2");
    ResourceUsage usage_node1 = node1->size();
    ResourceUsage usage_node2 = node2->size();
    DLF::verify_list(dlist.get(), {node1, node2});
    ASSERT_EQ(get_used_memory(), usage_node1 + usage_node2);

    std::unique_lock lock1(node1->test_get_mutex());
    std::unique_lock lock2(node2->test_get_mutex());

    EXPECT_CALL(*node1, clear_data()).Times(0);
    EXPECT_CALL(*node2, clear_data()).Times(0);

    ResourceUsage reserve_size{20, 20};
    EXPECT_FALSE(dlist->reserveMemory(reserve_size));

    lock1.unlock();
    lock2.unlock();

    EXPECT_EQ(get_used_memory(), usage_node1 + usage_node2);
    DLF::verify_list(dlist.get(), {node1, node2});
}

TEST_F(DListTest, ReserveMemoryFailsSpecificPinned) {
    MockListNode* node_evict =
        add_and_load_node({80, 40}, "evict_candidate", 0, 1);
    MockListNode* node_small = add_and_load_node({10, 5}, "small");
    ResourceUsage usage_evict = node_evict->size();
    ResourceUsage usage_small = node_small->size();
    DLF::verify_list(dlist.get(), {node_evict, node_small});
    ASSERT_EQ(get_used_memory(), usage_evict + usage_small);

    EXPECT_CALL(*node_evict, clear_data()).Times(0);
    EXPECT_CALL(*node_small, clear_data()).Times(0);

    ResourceUsage reserve_size{20, 20};
    EXPECT_FALSE(dlist->reserveMemory(reserve_size));

    EXPECT_EQ(get_used_memory(), usage_evict + usage_small);
    DLF::verify_list(dlist.get(), {node_evict, node_small});
}

TEST_F(DListTest, ReserveMemoryFailsSpecificLocked) {
    MockListNode* node_evict = add_and_load_node({80, 40}, "evict_candidate");
    MockListNode* node_small = add_and_load_node({10, 5}, "small");
    ResourceUsage usage_evict = node_evict->size();
    ResourceUsage usage_small = node_small->size();
    DLF::verify_list(dlist.get(), {node_evict, node_small});
    ASSERT_EQ(get_used_memory(), usage_evict + usage_small);

    std::unique_lock lock_evict(node_evict->test_get_mutex());

    EXPECT_CALL(*node_evict, clear_data()).Times(0);
    EXPECT_CALL(*node_small, clear_data()).Times(0);

    ResourceUsage reserve_size{20, 20};
    EXPECT_FALSE(dlist->reserveMemory(reserve_size));

    lock_evict.unlock();

    EXPECT_EQ(get_used_memory(), usage_evict + usage_small);
    DLF::verify_list(dlist.get(), {node_evict, node_small});
}

TEST_F(DListTest, TouchItemHeadOutsideWindow) {
    MockListNode* node1 = add_and_load_node({10, 0}, "key1");
    MockListNode* node2 = add_and_load_node({10, 0}, "key2");
    DLF::verify_list(dlist.get(), {node1, node2});

    // Use eviction_config from dlist
    std::this_thread::sleep_for(dlist->eviction_config().cache_touch_window +
                                std::chrono::milliseconds(10));

    {
        std::unique_lock node_lock(node2->test_get_mutex());
        dlist->touchItem(node2);
    }

    DLF::verify_list(dlist.get(), {node1, node2});
}

TEST_F(DListTest, RemoveItemFromList) {
    MockListNode* node1 = add_and_load_node({10, 0}, "key1");
    MockListNode* node2 = add_and_load_node({10, 0}, "key2");
    DLF::verify_list(dlist.get(), {node1, node2});

    {
        std::unique_lock node_lock(node1->test_get_mutex());
        dlist->removeItem(node1, node1->size());
    }

    DLF::verify_list(dlist.get(), {node2});
    EXPECT_EQ(get_used_memory(), node2->size());
}

TEST_F(DListTest, PopItemNotPresent) {
    MockListNode* node1 = add_and_load_node({10, 0}, "key1");
    MockListNode* node2 = add_and_load_node({10, 0}, "key2");
    ResourceUsage initial_usage = get_used_memory();
    DLF::verify_list(dlist.get(), {node1, node2});

    auto orphan_node_ptr = std::make_unique<StrictMock<MockListNode>>(
        dlist.get(), ResourceUsage{10, 0}, "orphan", 0);
    MockListNode* orphan_node = orphan_node_ptr.get();

    {
        std::unique_lock node_lock(orphan_node->test_get_mutex());
        EXPECT_NO_THROW(DLF::test_pop_item(dlist.get(), orphan_node));
    }

    DLF::verify_list(dlist.get(), {node1, node2});
    EXPECT_EQ(get_used_memory(), initial_usage);
}

TEST_F(DListTest, UpdateLimitIncreaseMemDecreaseDisk) {
    MockListNode* node1 = add_and_load_node({20, 30}, "node1");
    MockListNode* node2 = add_and_load_node({30, 10}, "node2");
    ResourceUsage usage1 = node1->size();
    ResourceUsage usage2 = node2->size();
    DLF::verify_list(dlist.get(), {node1, node2});
    ASSERT_EQ(get_used_memory(), usage1 + usage2);

    EXPECT_CALL(*node1, clear_data()).Times(1);
    EXPECT_CALL(*node2, clear_data()).Times(0);

    ResourceUsage new_limit{200, 35};
    EXPECT_TRUE(dlist->UpdateLimit(new_limit));

    EXPECT_EQ(get_used_memory(), usage2);
    DLF::verify_list(dlist.get(), {node2});
    EXPECT_EQ(DLF::get_max_memory(*dlist), new_limit);
}

TEST_F(DListTest, EvictedNodeDestroyed) {
    MockListNode* node1 = add_and_load_node({40, 15}, "node1");
    MockListNode* node2 = add_and_load_node({50, 25}, "node2");
    ResourceUsage usage1 = node1->size();
    ResourceUsage usage2 = node2->size();
    DLF::verify_list(dlist.get(), {node1, node2});
    ASSERT_EQ(managed_nodes.size(), 2);
    ASSERT_EQ(get_used_memory(), usage1 + usage2);

    EXPECT_CALL(*node1, clear_data()).Times(1);
    EXPECT_CALL(*node2, clear_data()).Times(0);
    ResourceUsage new_limit{70, 40};
    EXPECT_TRUE(dlist->UpdateLimit(new_limit));
    DLF::verify_list(dlist.get(), {node2});
    ResourceUsage memory_after_eviction = get_used_memory();
    ASSERT_EQ(memory_after_eviction, usage2);

    // destroy node1 by removing its shared_ptr
    // node1's destructor should not decrement used_memory_ again
    auto it = std::find_if(managed_nodes.begin(),
                           managed_nodes.end(),
                           [&](const auto& ptr) { return ptr.get() == node1; });
    ASSERT_NE(it, managed_nodes.end());
    managed_nodes.erase(it);

    EXPECT_EQ(get_used_memory(), memory_after_eviction);
    DLF::verify_list(dlist.get(), {node2});
}

TEST_F(DListTest, NodeInListDestroyed) {
    MockListNode* node1 = add_and_load_node({40, 15}, "node1");
    MockListNode* node2 = add_and_load_node({50, 25}, "node2");
    ResourceUsage usage1 = node1->size();
    ResourceUsage usage2 = node2->size();
    DLF::verify_list(dlist.get(), {node1, node2});
    ASSERT_EQ(managed_nodes.size(), 2);
    ResourceUsage memory_before_destroy = get_used_memory();
    ASSERT_EQ(memory_before_destroy, usage1 + usage2);

    // destroy node1 by removing its shared_ptr
    // node1's destructor should decrement used_memory_ by node1->size() and remove node1 from the list
    auto it = std::find_if(managed_nodes.begin(),
                           managed_nodes.end(),
                           [&](const auto& ptr) { return ptr.get() == node1; });
    ASSERT_NE(it, managed_nodes.end());
    managed_nodes.erase(it);

    EXPECT_EQ(get_used_memory(), memory_before_destroy - usage1);
    DLF::verify_list(dlist.get(), {node2});
}

// New tests for watermark updates
TEST_F(DListTest, UpdateWatermarksValid) {
    ResourceUsage new_low{70, 30};
    ResourceUsage new_high{85, 40};

    // Check initial watermarks (optional, could use friend class if needed)
    // EXPECT_EQ(DLF::get_low_watermark(*dlist), low_watermark);
    // EXPECT_EQ(DLF::get_high_watermark(*dlist), high_watermark);

    EXPECT_NO_THROW(dlist->UpdateLowWatermark(new_low));
    EXPECT_NO_THROW(dlist->UpdateHighWatermark(new_high));

    // Verify new watermarks (requires friend class accessors)
    // EXPECT_EQ(DLF::get_low_watermark(*dlist), new_low);
    // EXPECT_EQ(DLF::get_high_watermark(*dlist), new_high);

    // Verify no change in list state or usage
    EXPECT_TRUE(dlist->IsEmpty());
    EXPECT_EQ(get_used_memory(), ResourceUsage{});
}

TEST_F(DListTest, UpdateWatermarksInvalid) {
    EXPECT_THROW(dlist->UpdateLowWatermark({-10, 0}), milvus::SegcoreError);
    EXPECT_THROW(dlist->UpdateLowWatermark({0, -5}), milvus::SegcoreError);
    EXPECT_THROW(dlist->UpdateHighWatermark({-10, 0}), milvus::SegcoreError);
    EXPECT_THROW(dlist->UpdateHighWatermark({0, -5}), milvus::SegcoreError);
}

TEST_F(DListTest, ReserveMemoryUsesLowWatermark) {
    // Set up: Limit 100/100, Low 80/80, High 90/90
    initial_limit = {100, 100};
    low_watermark = {80, 80};
    high_watermark = {90, 90};
    EXPECT_TRUE(dlist->UpdateLimit(initial_limit));
    dlist->UpdateLowWatermark(low_watermark);
    dlist->UpdateHighWatermark(high_watermark);

    // Add nodes totaling 95/95 usage (above high watermark)
    MockListNode* node1 = add_and_load_node({45, 45}, "node1");  // Tail
    MockListNode* node2 = add_and_load_node({50, 50}, "node2");  // Head
    ResourceUsage usage1 = node1->size();
    ResourceUsage usage2 = node2->size();
    DLF::verify_list(dlist.get(), {node1, node2});
    ASSERT_EQ(get_used_memory(), usage1 + usage2);  // 95, 95

    EXPECT_CALL(*node1, clear_data())
        .Times(1);  // Evict node1 to get below low watermark
    EXPECT_CALL(*node2, clear_data()).Times(0);

    // Reserve 10/10. Current usage 95/95. New potential usage 105/105.
    // Max limit 100/100. Min eviction needed: 5/5.
    // Expected eviction (target low watermark): 95/95 + 10/10 - 80/80 = 25/25.
    // Evicting node1 (45/45) satisfies both min and expected.
    ResourceUsage reserve_size{10, 10};
    EXPECT_TRUE(dlist->reserveMemory(reserve_size));

    // Expected usage: usage2 + reserve_size = (50,50) + (10,10) = (60,60)
    EXPECT_EQ(get_used_memory(), usage2 + reserve_size);
    DLF::verify_list(dlist.get(), {node2});
}
