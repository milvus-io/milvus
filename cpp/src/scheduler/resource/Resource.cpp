/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "Resource.h"


namespace zilliz {
namespace milvus {
namespace engine {

std::ostream &operator<<(std::ostream &out, const Resource &resource) {
    out << resource.Dump();
    return out;
}

Resource::Resource(std::string name,
                   ResourceType type,
                   bool enable_loader,
                   bool enable_executor)
    : name_(std::move(name)),
      type_(type),
      running_(false),
      enable_loader_(enable_loader),
      enable_executor_(enable_executor),
      load_flag_(false),
      exec_flag_(false) {
    task_table_.RegisterSubscriber([&] {
        if (subscriber_) {
            auto event = std::make_shared<TaskTableUpdatedEvent>(shared_from_this());
            subscriber_(std::static_pointer_cast<Event>(event));
        }
    });
}

void Resource::Start() {
    running_ = true;
    if (enable_loader_) {
        loader_thread_ = std::thread(&Resource::loader_function, this);
    }
    if (enable_executor_) {
        executor_thread_ = std::thread(&Resource::executor_function, this);
    }
}

void Resource::Stop() {
    running_ = false;
    if (enable_loader_) {
        WakeupLoader();
        loader_thread_.join();
    }
    if (enable_executor_) {
        WakeupExecutor();
        executor_thread_.join();
    }
}

TaskTable &Resource::task_table() {
    return task_table_;
}

void Resource::WakeupLoader() {
    std::lock_guard<std::mutex> lock(load_mutex_);
    load_flag_ = true;
    load_cv_.notify_one();
}

void Resource::WakeupExecutor() {
    std::lock_guard<std::mutex> lock(exec_mutex_);
    exec_flag_ = true;
    exec_cv_.notify_one();
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
        load_flag_ = false;
        auto task_item = pick_task_load();
        if (task_item) {
            LoadFile(task_item->task);
            // TODO: wrapper loaded
            task_item->state = TaskTableItemState::LOADED;
            if (subscriber_) {
                auto event = std::make_shared<CopyCompletedEvent>(shared_from_this(), task_item);
                subscriber_(std::static_pointer_cast<Event>(event));
            }
        }
    }
}

void Resource::executor_function() {
    if (subscriber_) {
        auto event = std::make_shared<StartUpEvent>(shared_from_this());
        subscriber_(std::static_pointer_cast<Event>(event));
    }
    while (running_) {
        std::unique_lock<std::mutex> lock(exec_mutex_);
        exec_cv_.wait(lock, [&] { return exec_flag_; });
        exec_flag_ = false;
        auto task_item = pick_task_execute();
        if (task_item) {
            Process(task_item->task);
            task_item->state = TaskTableItemState::EXECUTED;
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