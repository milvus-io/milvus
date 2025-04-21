#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <thread>
#include <vector>
#include <memory>

#include "cachinglayer/lrucache/DList.h"
#include "cachinglayer/Utils.h"
#include "mock_list_node.h"
#include "cachinglayer_test_utils.h"

using namespace milvus::cachinglayer;
using namespace milvus::cachinglayer::internal;
using ::testing::StrictMock;
using DLF = DListTestFriend;

class DListTest : public ::testing::Test {
 protected:
    ResourceUsage initial_limit{100, 50};
    DList::TouchConfig touch_config{{std::chrono::seconds(1)}};
    std::unique_ptr<DList> dlist;
    // Keep track of nodes to prevent them from being deleted prematurely
    std::vector<std::shared_ptr<MockListNode>> managed_nodes;

    void
    SetUp() override {
        dlist = std::make_unique<DList>(initial_limit, touch_config);
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

    // void
    // DLF::verify_list(dlist.get(), std::vector<MockListNode*> nodes) const {
    //     EXPECT_EQ(nodes.front(), DLF::get_tail(*dlist));
    //     EXPECT_EQ(nodes.back(), DLF::get_head(*dlist));
    //     for (size_t i = 0; i < nodes.size() - 1; ++i) {
    //         auto current = nodes[i];
    //         auto expected_prev = i == 0 ? nullptr : nodes[i - 1];
    //         auto expected_next = i == nodes.size() - 1 ? nullptr : nodes[i + 1];
    //         EXPECT_EQ(current->test_get_prev(), expected_prev);
    //         EXPECT_EQ(current->test_get_next(), expected_next);
    //     }
    // }
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
    EXPECT_THROW(dlist->UpdateLimit({-10, 0}), std::invalid_argument);
    EXPECT_THROW(dlist->UpdateLimit({0, -5}), std::invalid_argument);
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

    ResourceUsage reserve_size{20, 20};
    // Current used: 90, 40. Limit: 100, 50. Reserve: 20, 20.
    // Potential total: 110, 60. Need to free >= 10 mem, 10 disk.
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

    std::this_thread::sleep_for(touch_config.refresh_window +
                                std::chrono::milliseconds(100));

    {
        std::unique_lock node_lock(node1->test_get_mutex());
        dlist->touchItem(node1);
    }
    DLF::verify_list(dlist.get(), {node2, node1});

    std::this_thread::sleep_for(touch_config.refresh_window +
                                std::chrono::milliseconds(100));

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
    initial_limit = {100, 100};
    EXPECT_TRUE(dlist->UpdateLimit(initial_limit));

    MockListNode* node_disk_only = add_and_load_node({0, 50}, "disk_only");
    MockListNode* node_mixed = add_and_load_node({50, 50}, "mixed");
    ResourceUsage usage_disk = node_disk_only->size();
    ResourceUsage usage_mixed = node_mixed->size();
    DLF::verify_list(dlist.get(), {node_disk_only, node_mixed});
    ASSERT_EQ(get_used_memory(), usage_disk + usage_mixed);

    EXPECT_CALL(*node_disk_only, clear_data()).Times(0);
    EXPECT_CALL(*node_mixed, clear_data()).Times(1);

    // node_disk_only is at tail, but it contains no memory, thus evicting it does not help.
    // We need to evict node_mixed to free up memory.
    ResourceUsage reserve_size{60, 0};
    EXPECT_TRUE(dlist->reserveMemory(reserve_size));

    EXPECT_EQ(get_used_memory(), usage_disk + reserve_size);
    DLF::verify_list(dlist.get(), {node_disk_only});
}

TEST_F(DListTest, ReserveMemoryEvictOnlyDiskNeeded) {
    initial_limit = {100, 100};
    EXPECT_TRUE(dlist->UpdateLimit(initial_limit));

    MockListNode* node_mem_only = add_and_load_node({50, 0}, "mem_only");
    MockListNode* node_mixed = add_and_load_node({50, 50}, "mixed");
    ResourceUsage usage_mem = node_mem_only->size();
    ResourceUsage usage_mixed = node_mixed->size();
    DLF::verify_list(dlist.get(), {node_mem_only, node_mixed});
    ASSERT_EQ(get_used_memory(), usage_mem + usage_mixed);

    EXPECT_CALL(*node_mem_only, clear_data()).Times(0);
    EXPECT_CALL(*node_mixed, clear_data()).Times(1);

    ResourceUsage reserve_size{0, 60};
    EXPECT_TRUE(dlist->reserveMemory(reserve_size));

    EXPECT_EQ(get_used_memory(), usage_mem + reserve_size);
    DLF::verify_list(dlist.get(), {node_mem_only});
}

TEST_F(DListTest, ReserveMemoryEvictBothNeeded) {
    initial_limit = {100, 100};
    EXPECT_TRUE(dlist->UpdateLimit(initial_limit));

    MockListNode* node1 = add_and_load_node({30, 10}, "node1");
    MockListNode* node2 = add_and_load_node({10, 30}, "node2");
    MockListNode* node3 = add_and_load_node({50, 50}, "node3");
    ResourceUsage usage1 = node1->size();
    ResourceUsage usage2 = node2->size();
    ResourceUsage usage3 = node3->size();
    DLF::verify_list(dlist.get(), {node1, node2, node3});
    ASSERT_EQ(get_used_memory(), usage1 + usage2 + usage3);

    EXPECT_CALL(*node1, clear_data()).Times(1);
    EXPECT_CALL(*node2, clear_data()).Times(1);
    EXPECT_CALL(*node3, clear_data()).Times(0);

    ResourceUsage reserve_size{50, 50};
    EXPECT_TRUE(dlist->reserveMemory(reserve_size));

    EXPECT_EQ(get_used_memory(), usage3 + reserve_size);
    DLF::verify_list(dlist.get(), {node3});
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

    std::this_thread::sleep_for(touch_config.refresh_window +
                                std::chrono::milliseconds(100));

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
