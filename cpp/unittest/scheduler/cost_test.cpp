#include "scheduler/TaskTable.h"
#include "scheduler/Cost.h"
#include <gtest/gtest.h>


using namespace zilliz::milvus::engine;

class CostTest : public ::testing::Test {
protected:
    void
    SetUp() override {
        for (uint64_t i = 0; i < 7; ++i) {
            auto task = std::make_shared<XSearchTask>();
            table_.Put(task);
        }
        table_.Get(0)->state = TaskTableItemState::INVALID;
        table_.Get(1)->state = TaskTableItemState::START;
        table_.Get(2)->state = TaskTableItemState::LOADING;
        table_.Get(3)->state = TaskTableItemState::LOADED;
        table_.Get(4)->state = TaskTableItemState::EXECUTING;
        table_.Get(5)->state = TaskTableItemState::EXECUTED;
        table_.Get(6)->state = TaskTableItemState::MOVING;
        table_.Get(7)->state = TaskTableItemState::MOVED;
    }


    TaskTable table_;
};

TEST_F(CostTest, pick_to_move) {
    CacheMgr cache;
    auto indexes = PickToMove(table_, cache, 10);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0], 3);
}

TEST_F(CostTest, pick_to_load) {
    auto indexes = PickToLoad(table_, 10);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0], 1);
}

TEST_F(CostTest, pick_to_executed) {
    auto indexes = PickToExecute(table_, 10);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0], 3);
}
