/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "Resource.h"


namespace zilliz {
namespace milvus {
namespace engine {

Resource::Resource(std::string name, ResourceType type)
    : name_(std::move(name)),
      type_(type),
      running_(false),
      load_flag_(false),
      exec_flag_(false) {
}

void Resource::Start() {
    loader_thread_ = std::thread(&Resource::loader_function, this);
    executor_thread_ = std::thread(&Resource::executor_function, this);
}

void Resource::Stop() {
    running_ = false;
    WakeupLoader();
    WakeupExecutor();
}

TaskTable &Resource::task_table() {
    return task_table_;
}

void Resource::WakeupExecutor() {
    exec_cv_.notify_one();
}

void Resource::WakeupLoader() {
    load_cv_.notify_one();
}

TaskTableItemPtr Resource::pick_task_load() {
    auto indexes = PickToLoad(task_table_, 3);
    for (auto index : indexes) {
        // try to set one task loading, then return
        if (task_table_.Load(index))
            return task_table_.Get(index);
        // else try next
    }
    return nullptr;
}

TaskTableItemPtr Resource::pick_task_execute() {
    auto indexes = PickToExecute(task_table_, 3);
    for (auto index : indexes) {
        // try to set one task executing, then return
        if (task_table_.Execute(index))
            return task_table_.Get(index);
        // else try next
    }
    return nullptr;
}

void Resource::loader_function() {
    while (running_) {
        std::unique_lock<std::mutex> lock(load_mutex_);
        load_cv_.wait(lock, [&] { return load_flag_; });
        auto task_item = pick_task_load();
        if (task_item) {
            LoadFile(task_item->task);
            if (subscriber_) {
                auto event = std::make_shared<CopyCompletedEvent>(shared_from_this(), task_item);
                subscriber_(std::static_pointer_cast<Event>(event));
            }
        }
    }
}

void Resource::executor_function() {
    GetRegisterFunc(RegisterType::START_UP)->Exec();
    if (subscriber_) {
        auto event = std::make_shared<StartUpEvent>(shared_from_this());
        subscriber_(std::static_pointer_cast<Event>(event));
    }
    while (running_) {
        std::unique_lock<std::mutex> lock(exec_mutex_);
        exec_cv_.wait(lock, [&] { return exec_flag_; });
        auto task_item = pick_task_execute();
        if (task_item) {
            Process(task_item->task);
            if (subscriber_) {
                auto event = std::make_shared<FinishTaskEvent>(shared_from_this(), task_item);
                subscriber_(std::static_pointer_cast<Event>(event));
            }
        }
    }
}

RegisterHandlerPtr Resource::GetRegisterFunc(const RegisterType &type) {
    // construct object each time.
    return register_table_[type]();
}

}
}
}