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
#include "exec/TaskStats.h"
#include "plan/PlanNode.h"

namespace milvus {
namespace exec {

enum class TaskState { kRunning, kFinished, kCanceled, kAborted, kFailed };

inline std::string
TaskStateToString(TaskState state) {
    switch (state) {
        case TaskState::kRunning:
            return "Running";
        case TaskState::kFinished:
            return "Finished";
        case TaskState::kCanceled:
            return "Canceled";
        case TaskState::kAborted:
            return "Aborted";
        case TaskState::kFailed:
            return "Failed";
        default:
            return fmt::format("unknown task state {}",
                               static_cast<int>(state));
    }
}

std::string
MakeUuid();

class TaskListener {
 public:
    virtual ~TaskListener() = default;

    virtual void
    OnTaskCompleted(const std::string& task_uuid,
                    const std::string& taskid,
                    TaskState state,
                    std::exception_ptr error) = 0;

    virtual std::string
    name() const = 0;
};

class EventCompletedNotifier {
 public:
    ~EventCompletedNotifier() {
        Notify();
    }

    void
    Activate(std::vector<ContinuePromise> promises,
             std::function<void()> callback = nullptr) {
        activate_ = true;
        callback_ = callback;
        promises_ = std::move(promises);
    }

    void
    Notify() {
        if (activate_) {
            for (auto& promise : promises_) {
                promise.setValue();
            }
            promises_.clear();
            if (callback_) {
                callback_();
            }
            activate_ = false;
        }
    }

 private:
    bool activate_{false};
    std::function<void()> callback_{nullptr};
    std::vector<ContinuePromise> promises_;
};
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

    ConsumerSupplier
    consumer_supplier() const {
        return consumer_supplier_;
    }

    bool
    PauseRequested() const {
        return pause_requested_;
    }

    std::timed_mutex&
    mutex() {
        return mutex_;
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
        std::lock_guard<std::timed_mutex> lock(self->mutex_);
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

    std::vector<std::shared_ptr<Driver>>
    CreateDriversLocked(uint32_t split_groupid);

    void
    ProcessSplitGroupsDriversLocked();

    void
    SetError(const std::exception_ptr& exception);

    void
    SetError(const std::string& message);

    bool
    IsRunning() const {
        std::lock_guard<std::timed_mutex> l(mutex_);
        return (state_ == TaskState::kRunning);
    }

    bool
    IsFinished() const {
        std::lock_guard<std::timed_mutex> l(mutex_);
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

    ContinueFuture
    Terminate(TaskState state);

    std::exception_ptr
    error() const {
        std::lock_guard<std::timed_mutex> l(mutex_);
        return exception_;
    }

    std::string
    ErrorMessageImpl(const std::exception_ptr& exception) const {
        if (!exception) {
            return "";
        }

        std::string message;
        try {
            std::rethrow_exception(exception);
        } catch (const std::exception& e) {
            message = e.what();
        } catch (...) {
            message = "Unknown exception";
        }
        return message;
    }

    std::string
    ErrorMessageLocked() const {
        return ErrorMessageImpl(exception_);
    }

    std::string
    ErrorMessage() const {
        std::lock_guard<std::timed_mutex> l(mutex_);
        return ErrorMessageImpl(exception_);
    }

    void
    DriverClosedLocked() {
        if (IsRunningLocked()) {
            --num_running_drivers_;
        }

        num_finished_drivers_++;
    }

    void
    RequestCancel() {
        Terminate(TaskState::kCanceled);
    }

    StopReason
    ShouldStopLocked() {
        if (terminate_requested_) {
            return StopReason::kTerminate;
        }
        if (pause_requested_) {
            return StopReason::kPause;
        }

        if (to_yield_) {
            --to_yield_;
            return StopReason::kYield;
        }
        return StopReason::kNone;
    }

    StopReason
    ShouldStop() {
        if (pause_requested_) {
            return StopReason::kPause;
        }
        if (terminate_requested_) {
            return StopReason::kTerminate;
        }
        if (to_yield_) {
            std::lock_guard<std::timed_mutex> l(mutex_);
            return ShouldStopLocked();
        }
        return StopReason::kNone;
    }

    ContinueFuture
    MakeFinishFutureLocked(const char* comment) {
        auto [promise, future] = MakeMilvusContinuePromiseContract(comment);

        if (num_threads_ == 0) {
            promise.setValue();
            return std::move(future);
        }

        thread_finish_promises_.push_back(std::move(promise));
        return std::move(future);
    }

    ContinueFuture
    TaskCompletionFuture() {
        std::lock_guard<std::timed_mutex> lock(mutex_);
        if (!IsRunningLocked()) {
            return MakeFinishFutureLocked(
                fmt::format("Task::TaskCompletionFuture {}", taskid_).data());
        }

        auto [promise, future] = MakeMilvusContinuePromiseContract(
            fmt::format("Task::TaskCompletionFuture {}", taskid_));
        task_completed_promises_.push_back(std::move(promise));
        return std::move(future);
    }

    StopReason
    EnterForTerminateLocked(ThreadState& state) {
        if (state.IsOnThread() || state.is_terminated_) {
            state.is_terminated_ = true;
            return StopReason::kAlreadyOnThread;
        }

        if (pause_requested_) {
            return StopReason::kPause;
        }

        state.is_terminated_ = true;
        state.SetThread();
        return StopReason::kTerminate;
    }

 private:
    std::string uuid_;

    std::string taskid_;

    plan::PlanFragment plan_fragment_;

    int destination_;

    ConsumerSupplier consumer_supplier_;

    std::shared_ptr<QueryContext> query_context_;

    std::exception_ptr exception_ = nullptr;

    std::function<void(std::exception_ptr)> on_error_;

    std::vector<std::unique_ptr<DriverFactory>> driver_factories_;

    std::vector<std::shared_ptr<Driver>> drivers_;

    TaskState state_ = TaskState::kRunning;

    TaskStats stats_;

    uint32_t num_running_drivers_{0};

    uint32_t num_total_drivers_{0};

    uint32_t num_drivers_per_split_group_{0};

    uint32_t num_ungrouped_drivers_{0};

    uint32_t num_finished_drivers_{0};

    uint32_t concurrent_split_groups_{1};

    mutable std::timed_mutex mutex_;

    std::atomic<bool> pause_requested_{false};

    std::atomic<bool> terminate_requested_{false};

    std::atomic<int32_t> to_yield_{0};

    int32_t num_threads_{0};

    std::vector<ContinuePromise> task_completed_promises_;

    std::vector<ContinuePromise> thread_finish_promises_;
};

}  // namespace exec
}  // namespace milvus