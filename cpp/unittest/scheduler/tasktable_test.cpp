#include "scheduler/TaskTable.h"
#include "scheduler/task/TestTask.h"
#include <gtest/gtest.h>


using namespace zilliz::milvus::engine;

class TaskTableItemTest : public ::testing::Test {
protected:
    void
    SetUp() override {
        item1_.id = 0;
        item1_.state = TaskTableItemState::MOVED;
        item1_.priority = 10;
    }

    TaskTableItem default_;
    TaskTableItem item1_;
};

TEST_F(TaskTableItemTest, construct) {
    ASSERT_EQ(default_.id, 0);
    ASSERT_EQ(default_.state, TaskTableItemState::INVALID);
    ASSERT_EQ(default_.priority, 0);
}

TEST_F(TaskTableItemTest, copy) {
    TaskTableItem another(item1_);
    ASSERT_EQ(another.id, item1_.id);
    ASSERT_EQ(another.state, item1_.state);
    ASSERT_EQ(another.priority, item1_.priority);
}

TEST_F(TaskTableItemTest, destruct) {
    auto p_item = new TaskTableItem();
    delete p_item;
}


/************ TaskTableBaseTest ************/

class TaskTableBaseTest : public ::testing::Test {
protected:
    void
    SetUp() override {
        invalid_task_ = nullptr;
        task1_ = std::make_shared<TestTask>();
        task2_ = std::make_shared<TestTask>();
    }

    TaskPtr invalid_task_;
    TaskPtr task1_;
    TaskPtr task2_;
    TaskTable empty_table_;
};


TEST_F(TaskTableBaseTest, put_task) {
    empty_table_.Put(task1_);
    ASSERT_EQ(empty_table_.Get(0)->task, task1_);
}

TEST_F(TaskTableBaseTest, put_invalid_test) {
    empty_table_.Put(invalid_task_);
    ASSERT_EQ(empty_table_.Get(0)->task, invalid_task_);
}

TEST_F(TaskTableBaseTest, put_batch) {
    std::vector<TaskPtr> tasks{task1_, task2_};
    empty_table_.Put(tasks);
    ASSERT_EQ(empty_table_.Get(0)->task, task1_);
    ASSERT_EQ(empty_table_.Get(1)->task, task2_);
}

TEST_F(TaskTableBaseTest, put_empty_batch) {
    std::vector<TaskPtr> tasks{};
    empty_table_.Put(tasks);
}

/************ TaskTableAdvanceTest ************/

class TaskTableAdvanceTest : public ::testing::Test {
protected:
    void
    SetUp() override {
        for (uint64_t i = 0; i < 8; ++i) {
            auto task = std::make_shared<TestTask>();
            table1_.Put(task);
        }

        table1_.Get(0)->state = TaskTableItemState::INVALID;
        table1_.Get(1)->state = TaskTableItemState::START;
        table1_.Get(2)->state = TaskTableItemState::LOADING;
        table1_.Get(3)->state = TaskTableItemState::LOADED;
        table1_.Get(4)->state = TaskTableItemState::EXECUTING;
        table1_.Get(5)->state = TaskTableItemState::EXECUTED;
        table1_.Get(6)->state = TaskTableItemState::MOVING;
        table1_.Get(7)->state = TaskTableItemState::MOVED;
    }

    TaskTable table1_;
};

TEST_F(TaskTableAdvanceTest, load) {
    table1_.Load(1);
    table1_.Loaded(2);

    ASSERT_EQ(table1_.Get(1)->state, TaskTableItemState::LOADING);
    ASSERT_EQ(table1_.Get(2)->state, TaskTableItemState::LOADED);
}

TEST_F(TaskTableAdvanceTest, execute) {
    table1_.Execute(3);
    table1_.Executed(4);

    ASSERT_EQ(table1_.Get(3)->state, TaskTableItemState::EXECUTING);
    ASSERT_EQ(table1_.Get(4)->state, TaskTableItemState::EXECUTED);
}

TEST_F(TaskTableAdvanceTest, move) {
    table1_.Move(3);
    table1_.Moved(6);

    ASSERT_EQ(table1_.Get(3)->state, TaskTableItemState::MOVING);
    ASSERT_EQ(table1_.Get(6)->state, TaskTableItemState::MOVED);
}
