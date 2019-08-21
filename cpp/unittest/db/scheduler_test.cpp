////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include <thread>
#include <easylogging++.h>
#include <boost/filesystem.hpp>

#include "db/scheduler/TaskScheduler.h"
#include "db/scheduler/TaskDispatchStrategy.h"
#include "db/scheduler/TaskDispatchQueue.h"
#include "db/scheduler/task/SearchTask.h"
#include "db/scheduler/task/DeleteTask.h"
#include "db/scheduler/task/IndexLoadTask.h"

using namespace zilliz::milvus;

namespace {

engine::TableFileSchemaPtr CreateTabileFileStruct(size_t id, const std::string& table_id) {
    auto file = std::make_shared<engine::meta::TableFileSchema>();
    file->id_ = id;
    file->table_id_ = table_id;
    return file;
}

}

TEST(DBSchedulerTest, TASK_QUEUE_TEST) {
    engine::TaskDispatchQueue queue;
    queue.SetCapacity(1000);
    queue.Put(nullptr);
    ASSERT_EQ(queue.Size(), 1UL);

    auto ptr = queue.Take();
    ASSERT_EQ(ptr, nullptr);
    ASSERT_TRUE(queue.Empty());

    engine::SearchContextPtr context_ptr = std::make_shared<engine::SearchContext>(1, 1, 10, nullptr);
    for(size_t i = 0; i < 10; i++) {
        auto file = CreateTabileFileStruct(i, "tbl");
        context_ptr->AddIndexFile(file);
    }

    queue.Put(context_ptr);
    ASSERT_EQ(queue.Size(), 10);

    auto index_files = context_ptr->GetIndexMap();

    ptr = queue.Front();
    ASSERT_EQ(ptr->type(), engine::ScheduleTaskType::kIndexLoad);
    engine::IndexLoadTaskPtr load_task = std::static_pointer_cast<engine::IndexLoadTask>(ptr);
    ASSERT_EQ(load_task->file_->id_, index_files.begin()->first);

    ptr = queue.Back();
    ASSERT_EQ(ptr->type(), engine::ScheduleTaskType::kIndexLoad);
    load_task->Execute();

}

TEST(DBSchedulerTest, SEARCH_SCHEDULER_TEST) {
    std::list<engine::ScheduleTaskPtr> task_list;
    bool ret = engine::TaskDispatchStrategy::Schedule(nullptr, task_list);
    ASSERT_FALSE(ret);

    for(size_t i = 10; i < 30; i++) {
        engine::IndexLoadTaskPtr task_ptr = std::make_shared<engine::IndexLoadTask>();
        task_ptr->file_ = CreateTabileFileStruct(i, "tbl");
        task_list.push_back(task_ptr);
    }

    engine::SearchContextPtr context_ptr = std::make_shared<engine::SearchContext>(1, 1, 10, nullptr);
    for(size_t i = 0; i < 20; i++) {
        auto file = CreateTabileFileStruct(i, "tbl");
        context_ptr->AddIndexFile(file);
    }

    ret = engine::TaskDispatchStrategy::Schedule(context_ptr, task_list);
    ASSERT_TRUE(ret);
    ASSERT_EQ(task_list.size(), 30);
}

TEST(DBSchedulerTest, DELETE_SCHEDULER_TEST) {
    std::list<engine::ScheduleTaskPtr> task_list;
    bool ret = engine::TaskDispatchStrategy::Schedule(nullptr, task_list);
    ASSERT_FALSE(ret);

    const std::string table_id = "to_delete_table";
    for(size_t i = 0; i < 10; i++) {
        engine::IndexLoadTaskPtr task_ptr = std::make_shared<engine::IndexLoadTask>();
        task_ptr->file_ = CreateTabileFileStruct(i, table_id);
        task_list.push_back(task_ptr);
    }

    for(size_t i = 0; i < 10; i++) {
        engine::IndexLoadTaskPtr task_ptr = std::make_shared<engine::IndexLoadTask>();
        task_ptr->file_ = CreateTabileFileStruct(i, "other_table");
        task_list.push_back(task_ptr);
    }

    engine::meta::Meta::Ptr meta_ptr;
    engine::DeleteContextPtr context_ptr = std::make_shared<engine::DeleteContext>(table_id, meta_ptr);
    ret = engine::TaskDispatchStrategy::Schedule(context_ptr, task_list);
    ASSERT_TRUE(ret);
    ASSERT_EQ(task_list.size(), 21);

    auto temp_list = task_list;
    for(size_t i = 0; ; i++) {
        engine::ScheduleTaskPtr task_ptr = temp_list.front();
        temp_list.pop_front();
        if(task_ptr->type() == engine::ScheduleTaskType::kDelete) {
            ASSERT_EQ(i, 10);
            break;
        }
    }

    context_ptr = std::make_shared<engine::DeleteContext>("no_task_table", meta_ptr);
    ret = engine::TaskDispatchStrategy::Schedule(context_ptr, task_list);
    ASSERT_TRUE(ret);
    ASSERT_EQ(task_list.size(), 22);

    engine::ScheduleTaskPtr task_ptr = task_list.front();
    ASSERT_EQ(task_ptr->type(), engine::ScheduleTaskType::kDelete);
}
