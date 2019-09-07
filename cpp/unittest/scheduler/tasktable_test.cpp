#include "scheduler/TaskTable.h"
#include "scheduler/task/TestTask.h"
#include <gtest/gtest.h>


using namespace zilliz::milvus::engine;


/************ TaskTableBaseTest ************/

class TaskTableItemTest : public ::testing::Test {
protected:
    void
    SetUp() override {
        std::vector<TaskTableItemState> states{
            TaskTableItemState::INVALID,
            TaskTableItemState::START,
            TaskTableItemState::LOADING,
            TaskTableItemState::LOADED,
            TaskTableItemState::EXECUTING,
            TaskTableItemState::EXECUTED,
            TaskTableItemState::MOVING,
            TaskTableItemState::MOVED};
        for (auto &state : states) {
            auto item = std::make_shared<TaskTableItem>();
            item->state = state;
            items_.emplace_back(item);
        }
    }

    TaskTableItem default_;
    std::vector<TaskTableItemPtr> items_;
};

TEST_F(TaskTableItemTest, construct) {
    ASSERT_EQ(default_.id, 0);
    ASSERT_EQ(default_.task, nullptr);
    ASSERT_EQ(default_.state, TaskTableItemState::INVALID);
}

TEST_F(TaskTableItemTest, destruct) {
    auto p_item = new TaskTableItem();
    delete p_item;
}

TEST_F(TaskTableItemTest, is_finish) {
    for (auto &item : items_) {
        if (item->state == TaskTableItemState::EXECUTED
            || item->state == TaskTableItemState::MOVED) {
            ASSERT_TRUE(item->IsFinish());
        } else {
            ASSERT_FALSE(item->IsFinish());
        }
    }
}

TEST_F(TaskTableItemTest, dump) {
    for (auto &item : items_) {
        ASSERT_FALSE(item->Dump().empty());
    }
}

TEST_F(TaskTableItemTest, load) {
    for (auto &item : items_) {
        auto before_state = item->state;
        auto ret = item->Load();
        if (before_state == TaskTableItemState::START) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, TaskTableItemState::LOADING);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}

TEST_F(TaskTableItemTest, loaded) {
    for (auto &item : items_) {
        auto before_state = item->state;
        auto ret = item->Loaded();
        if (before_state == TaskTableItemState::LOADING) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, TaskTableItemState::LOADED);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}

TEST_F(TaskTableItemTest, execute) {
    for (auto &item : items_) {
        auto before_state = item->state;
        auto ret = item->Execute();
        if (before_state == TaskTableItemState::LOADED) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, TaskTableItemState::EXECUTING);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}


TEST_F(TaskTableItemTest, executed) {
    for (auto &item : items_) {
        auto before_state = item->state;
        auto ret = item->Executed();
        if (before_state == TaskTableItemState::EXECUTING) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, TaskTableItemState::EXECUTED);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}

TEST_F(TaskTableItemTest, move) {
    for (auto &item : items_) {
        auto before_state = item->state;
        auto ret = item->Move();
        if (before_state == TaskTableItemState::LOADED) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, TaskTableItemState::MOVING);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}

TEST_F(TaskTableItemTest, moved) {
    for (auto &item : items_) {
        auto before_state = item->state;
        auto ret = item->Moved();
        if (before_state == TaskTableItemState::MOVING) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, TaskTableItemState::MOVED);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}

/************ TaskTableBaseTest ************/

class TaskTableBaseTest : public ::testing::Test {
protected:
    void
    SetUp() override {
        TableFileSchemaPtr dummy = nullptr;
        invalid_task_ = nullptr;
        task1_ = std::make_shared<TestTask>(dummy);
        task2_ = std::make_shared<TestTask>(dummy);
    }

    TaskPtr invalid_task_;
    TaskPtr task1_;
    TaskPtr task2_;
    TaskTable empty_table_;
};

TEST_F(TaskTableBaseTest, subscriber) {
    bool flag = false;
    auto callback = [&]() {
        flag = true;
    };
    empty_table_.RegisterSubscriber(callback);
    empty_table_.Put(task1_);
    ASSERT_TRUE(flag);
}


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

TEST_F(TaskTableBaseTest, empty) {
    ASSERT_TRUE(empty_table_.Empty());
    empty_table_.Put(task1_);
    ASSERT_FALSE(empty_table_.Empty());
}

TEST_F(TaskTableBaseTest, size) {
    ASSERT_EQ(empty_table_.Size(), 0);
    empty_table_.Put(task1_);
    ASSERT_EQ(empty_table_.Size(), 1);
}

TEST_F(TaskTableBaseTest, operator_) {
    empty_table_.Put(task1_);
    ASSERT_EQ(empty_table_.Get(0), empty_table_[0]);
}

TEST_F(TaskTableBaseTest, pick_to_load) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = TaskTableItemState::MOVED;
    empty_table_[1]->state = TaskTableItemState::EXECUTED;

    auto indexes = empty_table_.PickToLoad(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0], 2);
}

TEST_F(TaskTableBaseTest, pick_to_load_limit) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = TaskTableItemState::MOVED;
    empty_table_[1]->state = TaskTableItemState::EXECUTED;

    auto indexes = empty_table_.PickToLoad(3);
    ASSERT_EQ(indexes.size(), 3);
    ASSERT_EQ(indexes[0], 2);
    ASSERT_EQ(indexes[1], 3);
    ASSERT_EQ(indexes[2], 4);
}

TEST_F(TaskTableBaseTest, pick_to_load_cache) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = TaskTableItemState::MOVED;
    empty_table_[1]->state = TaskTableItemState::EXECUTED;

    // first pick, non-cache
    auto indexes = empty_table_.PickToLoad(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0], 2);

    // second pick, iterate from 2
    // invalid state change
    empty_table_[1]->state = TaskTableItemState::START;
    indexes = empty_table_.PickToLoad(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0], 2);
}

TEST_F(TaskTableBaseTest, pick_to_execute) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = TaskTableItemState::MOVED;
    empty_table_[1]->state = TaskTableItemState::EXECUTED;
    empty_table_[2]->state = TaskTableItemState::LOADED;

    auto indexes = empty_table_.PickToExecute(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0], 2);
}

TEST_F(TaskTableBaseTest, pick_to_execute_limit) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = TaskTableItemState::MOVED;
    empty_table_[1]->state = TaskTableItemState::EXECUTED;
    empty_table_[2]->state = TaskTableItemState::LOADED;
    empty_table_[3]->state = TaskTableItemState::LOADED;

    auto indexes = empty_table_.PickToExecute(3);
    ASSERT_EQ(indexes.size(), 2);
    ASSERT_EQ(indexes[0], 2);
    ASSERT_EQ(indexes[1], 3);
}

TEST_F(TaskTableBaseTest, pick_to_execute_cache) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = TaskTableItemState::MOVED;
    empty_table_[1]->state = TaskTableItemState::EXECUTED;
    empty_table_[2]->state = TaskTableItemState::LOADED;

    // first pick, non-cache
    auto indexes = empty_table_.PickToExecute(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0], 2);

    // second pick, iterate from 2
    // invalid state change
    empty_table_[1]->state = TaskTableItemState::START;
    indexes = empty_table_.PickToExecute(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0], 2);
}


/************ TaskTableAdvanceTest ************/

class TaskTableAdvanceTest : public ::testing::Test {
protected:
    void
    SetUp() override {
        TableFileSchemaPtr dummy = nullptr;
        for (uint64_t i = 0; i < 8; ++i) {
            auto task = std::make_shared<TestTask>(dummy);
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
    std::vector<TaskTableItemState> before_state;
    for (auto &task : table1_) {
        before_state.push_back(task->state);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        table1_.Load(i);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        if (before_state[i] == TaskTableItemState::START) {
            ASSERT_EQ(table1_.Get(i)->state, TaskTableItemState::LOADING);
        } else {
            ASSERT_EQ(table1_.Get(i)->state, before_state[i]);
        }
    }
}

TEST_F(TaskTableAdvanceTest, loaded) {
    std::vector<TaskTableItemState> before_state;
    for (auto &task : table1_) {
        before_state.push_back(task->state);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        table1_.Loaded(i);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        if (before_state[i] == TaskTableItemState::LOADING) {
            ASSERT_EQ(table1_.Get(i)->state, TaskTableItemState::LOADED);
        } else {
            ASSERT_EQ(table1_.Get(i)->state, before_state[i]);
        }
    }
}

TEST_F(TaskTableAdvanceTest, execute) {
    std::vector<TaskTableItemState> before_state;
    for (auto &task : table1_) {
        before_state.push_back(task->state);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        table1_.Execute(i);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        if (before_state[i] == TaskTableItemState::LOADED) {
            ASSERT_EQ(table1_.Get(i)->state, TaskTableItemState::EXECUTING);
        } else {
            ASSERT_EQ(table1_.Get(i)->state, before_state[i]);
        }
    }
}

TEST_F(TaskTableAdvanceTest, executed) {
    std::vector<TaskTableItemState> before_state;
    for (auto &task : table1_) {
        before_state.push_back(task->state);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        table1_.Executed(i);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        if (before_state[i] == TaskTableItemState::EXECUTING) {
            ASSERT_EQ(table1_.Get(i)->state, TaskTableItemState::EXECUTED);
        } else {
            ASSERT_EQ(table1_.Get(i)->state, before_state[i]);
        }
    }
}

TEST_F(TaskTableAdvanceTest, move) {
    std::vector<TaskTableItemState> before_state;
    for (auto &task : table1_) {
        before_state.push_back(task->state);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        table1_.Move(i);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        if (before_state[i] == TaskTableItemState::LOADED) {
            ASSERT_EQ(table1_.Get(i)->state, TaskTableItemState::MOVING);
        } else {
            ASSERT_EQ(table1_.Get(i)->state, before_state[i]);
        }
    }
}

TEST_F(TaskTableAdvanceTest, moved) {
    std::vector<TaskTableItemState> before_state;
    for (auto &task : table1_) {
        before_state.push_back(task->state);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        table1_.Moved(i);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        if (before_state[i] == TaskTableItemState::MOVING) {
            ASSERT_EQ(table1_.Get(i)->state, TaskTableItemState::MOVED);
        } else {
            ASSERT_EQ(table1_.Get(i)->state, before_state[i]);
        }
    }
}

