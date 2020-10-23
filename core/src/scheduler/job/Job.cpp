// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "scheduler/job/Job.h"

namespace milvus {
namespace scheduler {

namespace {
std::mutex unique_job_mutex;
uint64_t unique_job_id = 0;

std::string
JobTypeToString(JobType type) {
    switch (type) {
        case JobType::BUILD:
            return "build_index";
        case JobType::SEARCH:
            return "search";
        default:
            return "unknown";
    }
}

}  // namespace

Job::Job(JobType type) : type_(type) {
    std::lock_guard<std::mutex> lock(unique_job_mutex);
    id_ = unique_job_id++;
}

json
Job::Dump() const {
    json ret{
        {"job_id", id_},
        {"job_type", JobTypeToString(type_)},
    };

    json tasks_dump;
    for (auto& task : tasks_) {
        auto json = task->Dump();
        tasks_dump.push_back(json);
    }
    ret["tasks"] = tasks_dump;

    LOG_SERVER_DEBUG_ << ret.dump();

    return ret;
}

JobTasks
Job::CreateTasks() {
    std::unique_lock<std::mutex> lock(mutex_);
    tasks_.clear();
    OnCreateTasks(tasks_);
    tasks_created_ = true;
    if (tasks_.empty()) {
        cv_.notify_all();
    }

    //    Dump(); // uncomment it when you want to debug
    return tasks_;
}

void
Job::TaskDone(Task* task) {
    if (task == nullptr) {
        return;
    }

    auto json = task->Dump();
    std::string task_desc = json.dump();
    LOG_SERVER_DEBUG_ << LogOut("scheduler job [%ld] task %s finish", id(), task_desc.c_str());

    std::unique_lock<std::mutex> lock(mutex_);
    for (auto iter = tasks_.begin(); iter != tasks_.end(); ++iter) {
        if (task == (*iter).get()) {
            tasks_.erase(iter);
            break;
        }
    }
    if (tasks_.empty()) {
        cv_.notify_all();
    }
}

void
Job::WaitFinish() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return tasks_created_ && tasks_.empty(); });

    LOG_SERVER_DEBUG_ << "scheduler job " << id() << " all done.";
}

}  // namespace scheduler
}  // namespace milvus
