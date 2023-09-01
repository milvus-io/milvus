// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/Types.h"
#include "exec/Driver.h"
#include "exec/QueryContext.h"
#include "plan/PlanNode.h"

namespace milvus {
namespace exec {

enum class TaskState { kRunning, kFinished, kCanceled, kAborted, kFailed };

std::string
MakeUuid();
class Task : public std::enable_shared_from_this<Task> {
 public:
    static std::shared_ptr<Task>
    Create(const std::string& task_id,
           plan::PlanFragment plan_fragment,
           int destination,
           std::shared_ptr<QueryContext> query_context,
           Consumer consumer = nullptr,
           std::function<void(std::exception_ptr)> on_error = nullptr);

    static std::shared_ptr<Task>
    Create(const std::string& task_id,
           const plan::PlanFragment& plan_fragment,
           int destination,
           std::shared_ptr<QueryContext> query_ctx,
           ConsumerSupplier supplier,
           std::function<void(std::exception_ptr)> on_error = nullptr);

    Task(const std::string& task_id,
         plan::PlanFragment plan_fragment,
         int destination,
         std::shared_ptr<QueryContext> query_ctx,
         ConsumerSupplier consumer_supplier,
         std::function<void(std::exception_ptr)> on_error)
        : uuid_{MakeUuid()},
          taskid_(task_id),
          plan_fragment_(std::move(plan_fragment)),
          destination_(destination),
          query_context_(std::move(query_ctx)),
          consumer_supplier_(std::move(consumer_supplier)),
          on_error_(on_error) {
    }

    ~Task() {
    }

    const std::string&
    uuid() const {
        return uuid_;
    }

    const std::string&
    taskid() const {
        return taskid_;
    }

    const int
    destination() const {
        return destination_;
    }

    const std::shared_ptr<QueryContext>&
    query_context() const {
        return query_context_;
    }

    static void
    Start(std::shared_ptr<Task> self,
          uint32_t max_drivers,
          uint32_t concurrent_split_groups = 1);

    static void
    RemoveDriver(std::shared_ptr<Task> self, Driver* instance) {
        std::lock_guard<std::mutex> lock(self->mutex_);
        for (auto& driver_ptr : self->drivers_) {
            if (driver_ptr.get() != instance) {
                continue;
            }
            driver_ptr = nullptr;
            self->DriverClosedLocked();
        }
    }

    bool
    SupportsSingleThreadedExecution() const {
        if (consumer_supplier_) {
            return false;
        }
    }

    RowVectorPtr
    Next(ContinueFuture* future = nullptr);

    void
    CreateDriversLocked(std::shared_ptr<Task>& self,
                        uint32_t split_groupid,
                        std::vector<std::shared_ptr<Driver>>& out);

    void
    SetError(const std::exception_ptr& exception);

    void
    SetError(const std::string& message);

    bool
    IsRunning() const {
        std::lock_guard<std::mutex> l(mutex_);
        return (state_ == TaskState::kRunning);
    }

    bool
    IsFinished() const {
        std::lock_guard<std::mutex> l(mutex_);
        return (state_ == TaskState::kFinished);
    }

    bool
    IsRunningLocked() const {
        return (state_ == TaskState::kRunning);
    }

    bool
    IsFinishedLocked() const {
        return (state_ == TaskState::kFinished);
    }

    void
    Terminate(TaskState state) {
    }

    std::exception_ptr
    error() const {
        std::lock_guard<std::mutex> l(mutex_);
        return exception_;
    }

    void
    DriverClosedLocked() {
        if (IsRunningLocked()) {
            --num_running_drivers_;
        }

        num_finished_drivers_++;
    }

 private:
    std::string uuid_;

    std::string taskid_;

    plan::PlanFragment plan_fragment_;

    int destination_;

    std::shared_ptr<QueryContext> query_context_;

    std::exception_ptr exception_ = nullptr;

    std::function<void(std::exception_ptr)> on_error_;

    std::vector<std::unique_ptr<DriverFactory>> driver_factories_;

    std::vector<std::shared_ptr<Driver>> drivers_;

    ConsumerSupplier consumer_supplier_;

    mutable std::mutex mutex_;

    TaskState state_ = TaskState::kRunning;

    uint32_t num_running_drivers_{0};

    uint32_t num_total_drivers_{0};

    uint32_t num_ungrouped_drivers_{0};

    uint32_t num_finished_drivers_{0};
};

}  // namespace exec
}  // namespace milvus