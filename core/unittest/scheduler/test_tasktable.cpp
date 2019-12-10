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

#include <gtest/gtest.h>

#include "scheduler/TaskTable.h"
#include "scheduler/task/TestTask.h"

/************ TaskTableBaseTest ************/

class TaskTableItemTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        std::vector<milvus::scheduler::TaskTableItemState> states{
            milvus::scheduler::TaskTableItemState::INVALID,   milvus::scheduler::TaskTableItemState::START,
            milvus::scheduler::TaskTableItemState::LOADING,   milvus::scheduler::TaskTableItemState::LOADED,
            milvus::scheduler::TaskTableItemState::EXECUTING, milvus::scheduler::TaskTableItemState::EXECUTED,
            milvus::scheduler::TaskTableItemState::MOVING,    milvus::scheduler::TaskTableItemState::MOVED};
        for (auto& state : states) {
            auto item = std::make_shared<milvus::scheduler::TaskTableItem>();
            item->state = state;
            items_.emplace_back(item);
        }
    }

    milvus::scheduler::TaskTableItem default_;
    std::vector<milvus::scheduler::TaskTableItemPtr> items_;
};

TEST_F(TaskTableItemTest, CONSTRUCT) {
    ASSERT_EQ(default_.id, 0);
    ASSERT_EQ(default_.task, nullptr);
    ASSERT_EQ(default_.state, milvus::scheduler::TaskTableItemState::INVALID);
}

TEST_F(TaskTableItemTest, DESTRUCT) {
    auto p_item = new milvus::scheduler::TaskTableItem();
    delete p_item;
}

TEST_F(TaskTableItemTest, IS_FINISH) {
    for (auto& item : items_) {
        if (item->state == milvus::scheduler::TaskTableItemState::EXECUTED ||
            item->state == milvus::scheduler::TaskTableItemState::MOVED) {
            ASSERT_TRUE(item->IsFinish());
        } else {
            ASSERT_FALSE(item->IsFinish());
        }
    }
}

TEST_F(TaskTableItemTest, DUMP) {
    for (auto& item : items_) {
        ASSERT_FALSE(item->Dump().empty());
    }
}

TEST_F(TaskTableItemTest, LOAD) {
    for (auto& item : items_) {
        auto before_state = item->state;
        auto ret = item->Load();
        if (before_state == milvus::scheduler::TaskTableItemState::START) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, milvus::scheduler::TaskTableItemState::LOADING);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}

TEST_F(TaskTableItemTest, LOADED) {
    for (auto& item : items_) {
        auto before_state = item->state;
        auto ret = item->Loaded();
        if (before_state == milvus::scheduler::TaskTableItemState::LOADING) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, milvus::scheduler::TaskTableItemState::LOADED);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}

TEST_F(TaskTableItemTest, EXECUTE) {
    for (auto& item : items_) {
        auto before_state = item->state;
        auto ret = item->Execute();
        if (before_state == milvus::scheduler::TaskTableItemState::LOADED) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, milvus::scheduler::TaskTableItemState::EXECUTING);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}

TEST_F(TaskTableItemTest, EXECUTED) {
    for (auto& item : items_) {
        auto before_state = item->state;
        auto ret = item->Executed();
        if (before_state == milvus::scheduler::TaskTableItemState::EXECUTING) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, milvus::scheduler::TaskTableItemState::EXECUTED);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}

TEST_F(TaskTableItemTest, MOVE) {
    for (auto& item : items_) {
        auto before_state = item->state;
        auto ret = item->Move();
        if (before_state == milvus::scheduler::TaskTableItemState::LOADED) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, milvus::scheduler::TaskTableItemState::MOVING);
        } else {
            ASSERT_FALSE(ret);
            ASSERT_EQ(item->state, before_state);
        }
    }
}

TEST_F(TaskTableItemTest, MOVED) {
    for (auto& item : items_) {
        auto before_state = item->state;
        auto ret = item->Moved();
        if (before_state == milvus::scheduler::TaskTableItemState::MOVING) {
            ASSERT_TRUE(ret);
            ASSERT_EQ(item->state, milvus::scheduler::TaskTableItemState::MOVED);
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
        milvus::scheduler::TableFileSchemaPtr dummy = nullptr;
        invalid_task_ = nullptr;
        task1_ = std::make_shared<milvus::scheduler::TestTask>(
            std::make_shared<milvus::server::Context>("dummy_request_id"), dummy, nullptr);
        task2_ = std::make_shared<milvus::scheduler::TestTask>(
            std::make_shared<milvus::server::Context>("dummy_request_id"), dummy, nullptr);
    }

    milvus::scheduler::TaskPtr invalid_task_;
    milvus::scheduler::TaskPtr task1_;
    milvus::scheduler::TaskPtr task2_;
    milvus::scheduler::TaskTable empty_table_;
};

TEST_F(TaskTableBaseTest, SUBSCRIBER) {
    bool flag = false;
    auto callback = [&]() { flag = true; };
    empty_table_.RegisterSubscriber(callback);
    empty_table_.Put(task1_);
    ASSERT_TRUE(flag);
}

TEST_F(TaskTableBaseTest, PUT_TASK) {
    empty_table_.Put(task1_);
    ASSERT_EQ(empty_table_.at(0)->task, task1_);
}

TEST_F(TaskTableBaseTest, PUT_INVALID_TEST) {
    empty_table_.Put(invalid_task_);
    ASSERT_EQ(empty_table_.at(0)->task, invalid_task_);
}

TEST_F(TaskTableBaseTest, PUT_BATCH) {
    std::vector<milvus::scheduler::TaskPtr> tasks{task1_, task2_};
    for (auto& task : tasks) {
        empty_table_.Put(task);
    }
    ASSERT_EQ(empty_table_.at(0)->task, task1_);
    ASSERT_EQ(empty_table_.at(1)->task, task2_);
}

TEST_F(TaskTableBaseTest, SIZE) {
    ASSERT_EQ(empty_table_.size(), 0);
    empty_table_.Put(task1_);
    ASSERT_EQ(empty_table_.size(), 1);
}

TEST_F(TaskTableBaseTest, OPERATOR) {
    empty_table_.Put(task1_);
    ASSERT_EQ(empty_table_.at(0), empty_table_[0]);
}

TEST_F(TaskTableBaseTest, PICK_TO_LOAD) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = milvus::scheduler::TaskTableItemState::MOVED;
    empty_table_[1]->state = milvus::scheduler::TaskTableItemState::EXECUTED;

    auto indexes = empty_table_.PickToLoad(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0] % empty_table_.capacity(), 2);
}

TEST_F(TaskTableBaseTest, PICK_TO_LOAD_LIMIT) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = milvus::scheduler::TaskTableItemState::MOVED;
    empty_table_[1]->state = milvus::scheduler::TaskTableItemState::EXECUTED;

    auto indexes = empty_table_.PickToLoad(3);
    ASSERT_EQ(indexes.size(), 3);
    ASSERT_EQ(indexes[0] % empty_table_.capacity(), 2);
    ASSERT_EQ(indexes[1] % empty_table_.capacity(), 3);
    ASSERT_EQ(indexes[2] % empty_table_.capacity(), 4);
}

TEST_F(TaskTableBaseTest, PICK_TO_LOAD_CACHE) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = milvus::scheduler::TaskTableItemState::MOVED;
    empty_table_[1]->state = milvus::scheduler::TaskTableItemState::EXECUTED;

    // first pick, non-cache
    auto indexes = empty_table_.PickToLoad(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0] % empty_table_.capacity(), 2);

    // second pick, iterate from 2
    // invalid state change
    empty_table_[1]->state = milvus::scheduler::TaskTableItemState::START;
    indexes = empty_table_.PickToLoad(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0] % empty_table_.capacity(), 2);
}

TEST_F(TaskTableBaseTest, PICK_TO_EXECUTE) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = milvus::scheduler::TaskTableItemState::MOVED;
    empty_table_[1]->state = milvus::scheduler::TaskTableItemState::EXECUTED;
    empty_table_[2]->state = milvus::scheduler::TaskTableItemState::LOADED;

    auto indexes = empty_table_.PickToExecute(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0] % empty_table_.capacity(), 2);
}

TEST_F(TaskTableBaseTest, PICK_TO_EXECUTE_LIMIT) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = milvus::scheduler::TaskTableItemState::MOVED;
    empty_table_[1]->state = milvus::scheduler::TaskTableItemState::EXECUTED;
    empty_table_[2]->state = milvus::scheduler::TaskTableItemState::LOADED;
    empty_table_[3]->state = milvus::scheduler::TaskTableItemState::LOADED;

    auto indexes = empty_table_.PickToExecute(3);
    ASSERT_EQ(indexes.size(), 2);
    ASSERT_EQ(indexes[0] % empty_table_.capacity(), 2);
    ASSERT_EQ(indexes[1] % empty_table_.capacity(), 3);
}

TEST_F(TaskTableBaseTest, PICK_TO_EXECUTE_CACHE) {
    const size_t NUM_TASKS = 10;
    for (size_t i = 0; i < NUM_TASKS; ++i) {
        empty_table_.Put(task1_);
    }
    empty_table_[0]->state = milvus::scheduler::TaskTableItemState::MOVED;
    empty_table_[1]->state = milvus::scheduler::TaskTableItemState::EXECUTED;
    empty_table_[2]->state = milvus::scheduler::TaskTableItemState::LOADED;

    // first pick, non-cache
    auto indexes = empty_table_.PickToExecute(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0] % empty_table_.capacity(), 2);

    // second pick, iterate from 2
    // invalid state change
    empty_table_[1]->state = milvus::scheduler::TaskTableItemState::START;
    indexes = empty_table_.PickToExecute(1);
    ASSERT_EQ(indexes.size(), 1);
    ASSERT_EQ(indexes[0] % empty_table_.capacity(), 2);
}

/************ TaskTableAdvanceTest ************/

class TaskTableAdvanceTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        milvus::scheduler::TableFileSchemaPtr dummy = nullptr;
        for (uint64_t i = 0; i < 8; ++i) {
            auto task = std::make_shared<milvus::scheduler::TestTask>(
                std::make_shared<milvus::server::Context>("dummy_request_id"), dummy, nullptr);
            table1_.Put(task);
        }

        table1_.at(0)->state = milvus::scheduler::TaskTableItemState::INVALID;
        table1_.at(1)->state = milvus::scheduler::TaskTableItemState::START;
        table1_.at(2)->state = milvus::scheduler::TaskTableItemState::LOADING;
        table1_.at(3)->state = milvus::scheduler::TaskTableItemState::LOADED;
        table1_.at(4)->state = milvus::scheduler::TaskTableItemState::EXECUTING;
        table1_.at(5)->state = milvus::scheduler::TaskTableItemState::EXECUTED;
        table1_.at(6)->state = milvus::scheduler::TaskTableItemState::MOVING;
        table1_.at(7)->state = milvus::scheduler::TaskTableItemState::MOVED;
    }

    milvus::scheduler::TaskTable table1_;
};

TEST_F(TaskTableAdvanceTest, LOAD) {
    std::vector<milvus::scheduler::TaskTableItemState> before_state;
    for (size_t i = 0; i < table1_.size(); ++i) {
        before_state.push_back(table1_[i]->state);
    }

    for (size_t i = 0; i < table1_.size(); ++i) {
        table1_.Load(i);
    }

    for (size_t i = 0; i < table1_.size(); ++i) {
        if (before_state[i] == milvus::scheduler::TaskTableItemState::START) {
            ASSERT_EQ(table1_.at(i)->state, milvus::scheduler::TaskTableItemState::LOADING);
        } else {
            ASSERT_EQ(table1_.at(i)->state, before_state[i]);
        }
    }
}

TEST_F(TaskTableAdvanceTest, LOADED) {
    std::vector<milvus::scheduler::TaskTableItemState> before_state;
    for (size_t i = 0; i < table1_.size(); ++i) {
        before_state.push_back(table1_[i]->state);
    }

    for (size_t i = 0; i < table1_.size(); ++i) {
        table1_.Loaded(i);
    }

    for (size_t i = 0; i < table1_.size(); ++i) {
        if (before_state[i] == milvus::scheduler::TaskTableItemState::LOADING) {
            ASSERT_EQ(table1_.at(i)->state, milvus::scheduler::TaskTableItemState::LOADED);
        } else {
            ASSERT_EQ(table1_.at(i)->state, before_state[i]);
        }
    }
}

TEST_F(TaskTableAdvanceTest, EXECUTE) {
    std::vector<milvus::scheduler::TaskTableItemState> before_state;
    for (size_t i = 0; i < table1_.size(); ++i) {
        before_state.push_back(table1_[i]->state);
    }

    for (size_t i = 0; i < table1_.size(); ++i) {
        table1_.Execute(i);
    }

    for (size_t i = 0; i < table1_.size(); ++i) {
        if (before_state[i] == milvus::scheduler::TaskTableItemState::LOADED) {
            ASSERT_EQ(table1_.at(i)->state, milvus::scheduler::TaskTableItemState::EXECUTING);
        } else {
            ASSERT_EQ(table1_.at(i)->state, before_state[i]);
        }
    }
}

TEST_F(TaskTableAdvanceTest, EXECUTED) {
    std::vector<milvus::scheduler::TaskTableItemState> before_state;
    for (size_t i = 0; i < table1_.size(); ++i) {
        before_state.push_back(table1_[i]->state);
    }

    for (size_t i = 0; i < table1_.size(); ++i) {
        table1_.Executed(i);
    }

    for (size_t i = 0; i < table1_.size(); ++i) {
        if (before_state[i] == milvus::scheduler::TaskTableItemState::EXECUTING) {
            ASSERT_EQ(table1_.at(i)->state, milvus::scheduler::TaskTableItemState::EXECUTED);
        } else {
            ASSERT_EQ(table1_.at(i)->state, before_state[i]);
        }
    }
}

TEST_F(TaskTableAdvanceTest, MOVE) {
    std::vector<milvus::scheduler::TaskTableItemState> before_state;
    for (size_t i = 0; i < table1_.size(); ++i) {
        before_state.push_back(table1_[i]->state);
    }

    for (size_t i = 0; i < table1_.size(); ++i) {
        table1_.Move(i);
    }

    for (size_t i = 0; i < table1_.size(); ++i) {
        if (before_state[i] == milvus::scheduler::TaskTableItemState::LOADED) {
            ASSERT_EQ(table1_.at(i)->state, milvus::scheduler::TaskTableItemState::MOVING);
        } else {
            ASSERT_EQ(table1_.at(i)->state, before_state[i]);
        }
    }
}

TEST_F(TaskTableAdvanceTest, MOVED) {
    std::vector<milvus::scheduler::TaskTableItemState> before_state;
    for (size_t i = 0; i < table1_.size(); ++i) {
        before_state.push_back(table1_[i]->state);
    }

    for (size_t i = 0; i < table1_.size(); ++i) {
        table1_.Moved(i);
    }

    for (size_t i = 0; i < table1_.size(); ++i) {
        if (before_state[i] == milvus::scheduler::TaskTableItemState::MOVING) {
            ASSERT_EQ(table1_.at(i)->state, milvus::scheduler::TaskTableItemState::MOVED);
        } else {
            ASSERT_EQ(table1_.at(i)->state, before_state[i]);
        }
    }
}
