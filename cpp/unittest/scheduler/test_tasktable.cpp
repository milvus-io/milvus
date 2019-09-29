// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#include "scheduler/TaskTable.h"
#include "scheduler/task/TestTask.h"
#include <gtest/gtest.h>

namespace {

namespace ms = zilliz::milvus::scheduler;

} // namespace

/************ TaskTableBaseTest ************/

class TaskTableItemTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        std::vector<ms::TaskTableItemState> states{
            ms::TaskTableItemState::INVALID,
            ms::TaskTableItemState::START,
            ms::TaskTableItemState::LOADING,
            ms::TaskTableItemState::LOADED,
            ms::TaskTableItemState::EXECUTING,
            ms::TaskTableItemState::EXECUTED,
            ms::TaskTableItemState::MOVING,
            ms::TaskTableItemState::MOVED};
        for (auto &state : states) {
            auto item = std::make_shared<ms::TaskTableItem>();
            item->state = state;
            items_.emplace_back(item);
        }
    }

    ms::TaskTableItem default_;
    std::vector<ms::TaskTableItemPtr> items_;
};

TEST_F(TaskTableItemTest, CONSTRUCT) {
    ASSERT_EQ(default_.id, 0);
    ASSERT_EQ(default_.task, nullptr);
    ASSERT_EQ(default_.state, ms::TaskTableItemState::INVALID);
}

TEST_F(TaskTableItemTest, DESTRUCT) {
    auto p_item = new ms::TaskTableItem();
    delete p_item;
}

TEST_F(TaskTableItemTest, IS_FINISH) {
    for (auto &item : items_) {
        if (item->state == ms::TaskTableItemState::EXECUTED
            || item->state == ms::TaskTableItemState::MOVED) {
            ASSERT_TRUE(item->IsFinish());
        } else {
            ASSERT_FALSE(item->IsFinish());
        }
    }
}

TEST_F(TaskTableItemTest, DUMP) {
    for (auto &item : items_) {
        ASSERT_FALSE(item->Dump().empty());
    }
}

TEST_F(TaskTableItemTest, LOAD) {
    for (auto &item : items_) {
        auto before_state = item->state;
        auto ret = item->Load();
        if (before_state == ms::TaskTableItemState::START) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, ms::TaskTableItemState::LOADING);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}

TEST_F(TaskTableItemTest, LOADED) {
    for (auto &item : items_) {
        auto before_state = item->state;
        auto ret = item->Loaded();
        if (before_state == ms::TaskTableItemState::LOADING) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, ms::TaskTableItemState::LOADED);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}

TEST_F(TaskTableItemTest, EXECUTE) {
    for (auto &item : items_) {
        auto before_state = item->state;
        auto ret = item->Execute();
        if (before_state == ms::TaskTableItemState::LOADED) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, ms::TaskTableItemState::EXECUTING);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}

TEST_F(TaskTableItemTest, EXECUTED) {
    for (auto &item : items_) {
        auto before_state = item->state;
        auto ret = item->Executed();
        if (before_state == ms::TaskTableItemState::EXECUTING) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, ms::TaskTableItemState::EXECUTED);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}

TEST_F(TaskTableItemTest, MOVE) {
    for (auto &item : items_) {
        auto before_state = item->state;
        auto ret = item->Move();
        if (before_state == ms::TaskTableItemState::LOADED) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, ms::TaskTableItemState::MOVING);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}

TEST_F(TaskTableItemTest, MOVED) {
    for (auto &item : items_) {
        auto before_state = item->state;
        auto ret = item->Moved();
        if (before_state == ms::TaskTableItemState::MOVING) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, ms::TaskTableItemState::MOVED);
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
        ms::TableFileSchemaPtr dummy = nullptr;
        invalid_task_ = nullptr;
        task1_ = std::make_shared<ms::TestTask>(dummy);
        task2_ = std::make_shared<ms::TestTask>(dummy);
    }

    ms::TaskPtr invalid_task_;
    ms::TaskPtr task1_;
    ms::TaskPtr task2_;
    ms::TaskTable empty_table_;
};

TEST_F(TaskTableBaseTest, SUBSCRIBER) {
    bool flag = false;
    auto callback = [&]() {
        flag = true;
    };
    empty_table_.RegisterSubscriber(callback);
    empty_table_.Put(task1_);
    ASSERT_TRUE(flag);
}

TEST_F(TaskTableBaseTest, PUT_TASK) {
    empty_table_.Put(task1_);
    ASSERT_EQ(empty_table_.Get(0)->task, task1_);
}

TEST_F(TaskTableBaseTest, PUT_INVALID_TEST) {
    empty_table_.Put(invalid_task_);
    ASSERT_EQ(empty_table_.Get(0)->task, invalid_task_);
}

TEST_F(TaskTableBaseTest, PUT_BATCH) {
    std::vector<ms::TaskPtr> tasks{task1_, task2_};
    empty_table_.Put(tasks);
    ASSERT_EQ(empty_table_.Get(0)->task, task1_);
    ASSERT_EQ(empty_table_.Get(1)->task, task2_);
}

TEST_F(TaskTableBaseTest, PUT_EMPTY_BATCH) {
    std::vector<ms::TaskPtr> tasks{};
    empty_table_.Put(tasks);
}

TEST_F(TaskTableBaseTest, EMPTY) {
    ASSERT_TRUE(empty_table_.Empty());
    empty_table_.Put(task1_);
    ASSERT_FALSE(empty_table_.Empty());
}

TEST_F(TaskTableBaseTest, SIZE) {
    ASSERT_EQ(empty_table_.Size(), 0);
    empty_table_.Put(task1_);
    ASSERT_EQ(empty_table_.Size(), 1);
}

TEST_F(TaskTableBaseTest, OPERATOR) {
    empty_table_.Put(task1_);
    ASSERT_EQ(empty_table_.Get(0), empty_table_[0]);
}

TEST_F(TaskTableBaseTest, PICK_TO_LOAD) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = ms::TaskTableItemState::MOVED;
    empty_table_[1]->state = ms::TaskTableItemState::EXECUTED;

    auto indexes = empty_table_.PickToLoad(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0], 2);
}

TEST_F(TaskTableBaseTest, PICK_TO_LOAD_LIMIT) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = ms::TaskTableItemState::MOVED;
    empty_table_[1]->state = ms::TaskTableItemState::EXECUTED;

    auto indexes = empty_table_.PickToLoad(3);
    ASSERT_EQ(indexes.size(), 3);
    ASSERT_EQ(indexes[0], 2);
    ASSERT_EQ(indexes[1], 3);
    ASSERT_EQ(indexes[2], 4);
}

TEST_F(TaskTableBaseTest, PICK_TO_LOAD_CACHE) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = ms::TaskTableItemState::MOVED;
    empty_table_[1]->state = ms::TaskTableItemState::EXECUTED;

    // first pick, non-cache
    auto indexes = empty_table_.PickToLoad(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0], 2);

    // second pick, iterate from 2
    // invalid state change
    empty_table_[1]->state = ms::TaskTableItemState::START;
    indexes = empty_table_.PickToLoad(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0], 2);
}

TEST_F(TaskTableBaseTest, PICK_TO_EXECUTE) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = ms::TaskTableItemState::MOVED;
    empty_table_[1]->state = ms::TaskTableItemState::EXECUTED;
    empty_table_[2]->state = ms::TaskTableItemState::LOADED;

    auto indexes = empty_table_.PickToExecute(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0], 2);
}

TEST_F(TaskTableBaseTest, PICK_TO_EXECUTE_LIMIT) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = ms::TaskTableItemState::MOVED;
    empty_table_[1]->state = ms::TaskTableItemState::EXECUTED;
    empty_table_[2]->state = ms::TaskTableItemState::LOADED;
    empty_table_[3]->state = ms::TaskTableItemState::LOADED;

    auto indexes = empty_table_.PickToExecute(3);
    ASSERT_EQ(indexes.size(), 2);
    ASSERT_EQ(indexes[0], 2);
    ASSERT_EQ(indexes[1], 3);
}

TEST_F(TaskTableBaseTest, PICK_TO_EXECUTE_CACHE) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = ms::TaskTableItemState::MOVED;
    empty_table_[1]->state = ms::TaskTableItemState::EXECUTED;
    empty_table_[2]->state = ms::TaskTableItemState::LOADED;

    // first pick, non-cache
    auto indexes = empty_table_.PickToExecute(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0], 2);

    // second pick, iterate from 2
    // invalid state change
    empty_table_[1]->state = ms::TaskTableItemState::START;
    indexes = empty_table_.PickToExecute(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0], 2);
}

/************ TaskTableAdvanceTest ************/

class TaskTableAdvanceTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        ms::TableFileSchemaPtr dummy = nullptr;
        for (uint64_t i = 0; i < 8; ++i) {
            auto task = std::make_shared<ms::TestTask>(dummy);
            table1_.Put(task);
        }

        table1_.Get(0)->state = ms::TaskTableItemState::INVALID;
        table1_.Get(1)->state = ms::TaskTableItemState::START;
        table1_.Get(2)->state = ms::TaskTableItemState::LOADING;
        table1_.Get(3)->state = ms::TaskTableItemState::LOADED;
        table1_.Get(4)->state = ms::TaskTableItemState::EXECUTING;
        table1_.Get(5)->state = ms::TaskTableItemState::EXECUTED;
        table1_.Get(6)->state = ms::TaskTableItemState::MOVING;
        table1_.Get(7)->state = ms::TaskTableItemState::MOVED;
    }

    ms::TaskTable table1_;
};

TEST_F(TaskTableAdvanceTest, LOAD) {
    std::vector<ms::TaskTableItemState> before_state;
    for (auto &task : table1_) {
        before_state.push_back(task->state);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        table1_.Load(i);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        if (before_state[i] == ms::TaskTableItemState::START) {
            ASSERT_EQ(table1_.Get(i)->state, ms::TaskTableItemState::LOADING);
        } else {
            ASSERT_EQ(table1_.Get(i)->state, before_state[i]);
        }
    }
}

TEST_F(TaskTableAdvanceTest, LOADED) {
    std::vector<ms::TaskTableItemState> before_state;
    for (auto &task : table1_) {
        before_state.push_back(task->state);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        table1_.Loaded(i);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        if (before_state[i] == ms::TaskTableItemState::LOADING) {
            ASSERT_EQ(table1_.Get(i)->state, ms::TaskTableItemState::LOADED);
        } else {
            ASSERT_EQ(table1_.Get(i)->state, before_state[i]);
        }
    }
}

TEST_F(TaskTableAdvanceTest, EXECUTE) {
    std::vector<ms::TaskTableItemState> before_state;
    for (auto &task : table1_) {
        before_state.push_back(task->state);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        table1_.Execute(i);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        if (before_state[i] == ms::TaskTableItemState::LOADED) {
            ASSERT_EQ(table1_.Get(i)->state, ms::TaskTableItemState::EXECUTING);
        } else {
            ASSERT_EQ(table1_.Get(i)->state, before_state[i]);
        }
    }
}

TEST_F(TaskTableAdvanceTest, EXECUTED) {
    std::vector<ms::TaskTableItemState> before_state;
    for (auto &task : table1_) {
        before_state.push_back(task->state);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        table1_.Executed(i);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        if (before_state[i] == ms::TaskTableItemState::EXECUTING) {
            ASSERT_EQ(table1_.Get(i)->state, ms::TaskTableItemState::EXECUTED);
        } else {
            ASSERT_EQ(table1_.Get(i)->state, before_state[i]);
        }
    }
}

TEST_F(TaskTableAdvanceTest, MOVE) {
    std::vector<ms::TaskTableItemState> before_state;
    for (auto &task : table1_) {
        before_state.push_back(task->state);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        table1_.Move(i);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        if (before_state[i] == ms::TaskTableItemState::LOADED) {
            ASSERT_EQ(table1_.Get(i)->state, ms::TaskTableItemState::MOVING);
        } else {
            ASSERT_EQ(table1_.Get(i)->state, before_state[i]);
        }
    }
}

TEST_F(TaskTableAdvanceTest, MOVED) {
    std::vector<ms::TaskTableItemState> before_state;
    for (auto &task : table1_) {
        before_state.push_back(task->state);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        table1_.Moved(i);
    }

    for (size_t i = 0; i < table1_.Size(); ++i) {
        if (before_state[i] == ms::TaskTableItemState::MOVING) {
            ASSERT_EQ(table1_.Get(i)->state, ms::TaskTableItemState::MOVED);
        } else {
            ASSERT_EQ(table1_.Get(i)->state, before_state[i]);
        }
    }
}

