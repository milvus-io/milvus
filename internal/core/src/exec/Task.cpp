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

#include "Task.h"

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "log/Log.h"

namespace milvus {
namespace exec {

// Special group id to reflect the ungrouped execution.
constexpr uint32_t kUngroupedGroupId{std::numeric_limits<uint32_t>::max()};

std::string
MakeUuid() {
    return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
}

folly::Synchronized<std::vector<std::shared_ptr<TaskListener>>>&
Listeners() {
    static folly::Synchronized<std::vector<std::shared_ptr<TaskListener>>>
        listeners;
    return listeners;
}

bool
RegisterTaskListener(std::shared_ptr<TaskListener> listener) {
    return Listeners().withWLock([&](auto& listeners) {
        for (const auto& existing_listener : listeners) {
            if (existing_listener == listener) {
                return false;
            }
        }
        listeners.push_back(listener);
        return true;
    });
}

bool
UnRegisterTaskListener(std::shared_ptr<TaskListener> listener) {
    return Listeners().withWLock([&](auto& listeners) {
        for (auto it = listeners.begin(); it != listeners.end();) {
            if (*it == listener) {
                it = listeners.erase(it);
                return true;
            } else {
                ++it;
            }
        }
        // listern not found
        LOG_WARN("unregistered task listener {} not found", listener->name());
        return false;
    });
}

void
CollectSplitPlanNodeIds(
    const milvus::plan::PlanNode* plan_node,
    std::unordered_set<milvus::plan::PlanNodeId>& all_ids,
    std::unordered_set<milvus::plan::PlanNodeId>& source_ids) {
    bool ok = all_ids.insert(plan_node->id()).second;
    AssertInfo(
        ok,
        fmt::format("Plan node ids must be unique, found duplicate ID: {}",
                    plan_node->id()));
    if (plan_node->sources().empty()) {
        if (plan_node->RequireSplits()) {
            source_ids.insert(plan_node->id());
        }
        return;
    }

    for (const auto& source : plan_node->sources()) {
        CollectSplitPlanNodeIds(source.get(), all_ids, source_ids);
    }
}

std::unordered_set<milvus::plan::PlanNodeId>
CollectSplitPlanNodeIds(const milvus::plan::PlanNode* plan_node) {
    std::unordered_set<milvus::plan::PlanNodeId> all_ids;
    std::unordered_set<milvus::plan::PlanNodeId> source_ids;
    CollectSplitPlanNodeIds(plan_node, all_ids, source_ids);
    return source_ids;
}

std::shared_ptr<Task>
Task::Create(const std::string& task_id,
             plan::PlanFragment plan_fragment,
             int destination,
             std::shared_ptr<QueryContext> query_context,
             Consumer consumer,
             std::function<void(std::exception_ptr)> on_error) {
    return Task::Create(task_id,
                        std::move(plan_fragment),
                        destination,
                        std::move(query_context),
                        (consumer ? [c = std::move(consumer)]() { return c; }
                                  : ConsumerSupplier{}),
                        std::move(on_error));
}

std::shared_ptr<Task>
Task::Create(const std::string& task_id,
             const plan::PlanFragment& plan_fragment,
             int destination,
             std::shared_ptr<QueryContext> query_ctx,
             ConsumerSupplier supplier,
             std::function<void(std::exception_ptr)> on_error) {
    return std::shared_ptr<Task>(new Task(task_id,
                                          std::move(plan_fragment),
                                          destination,
                                          std::move(query_ctx),
                                          std::move(supplier),
                                          std::move(on_error)));
}

void
Task::SetError(const std::exception_ptr& exception) {
    {
        std::lock_guard<std::timed_mutex> l(mutex_);
        if (!IsRunningLocked()) {
            return;
        }

        if (exception_ != nullptr) {
            return;
        }
        exception_ = exception;
    }

    Terminate(TaskState::kFailed);

    if (on_error_) {
        on_error_(exception_);
    }
}

void
Task::SetError(const std::string& message) {
    try {
        throw std::runtime_error(message);
    } catch (const std::runtime_error& e) {
        SetError(std::current_exception());
    }
}

std::vector<std::shared_ptr<Driver>>
Task::CreateDriversLocked(uint32_t split_group_id) {
    const bool is_group_execution_drivers =
        (split_group_id != kUngroupedGroupId);
    const auto num_pipelines = driver_factories_.size();

    auto self = shared_from_this();
    std::vector<std::shared_ptr<Driver>> out;
    for (auto pipeline = 0; pipeline < num_pipelines; ++pipeline) {
        auto& factory = driver_factories_[pipeline];

        if (factory->is_group_execution_ != is_group_execution_drivers) {
            continue;
        }

        const uint32_t driverid_offset =
            factory->num_drivers_ *
            (is_group_execution_drivers ? split_group_id : 0);

        for (uint32_t partition_id = 0; partition_id < factory->num_drivers_;
             ++partition_id) {
            out.emplace_back(factory->CreateDriver(
                std::make_unique<DriverContext>(self,
                                                driverid_offset + partition_id,
                                                pipeline,
                                                split_group_id,
                                                partition_id),
                [self](size_t i) {
                    return i < self->driver_factories_.size()
                               ? self->driver_factories_[i]->num_total_drivers_
                               : 0;
                }));
        }
    }
    return out;
}

ContinueFuture
Task::Terminate(TaskState state) {
    EventCompletedNotifier completed_notifier;
    std::vector<std::shared_ptr<Driver>> off_thread_drivers;
    if (!IsRunningLocked()) {
        return MakeFinishFutureLocked("Task::Terminate");
    }
    state_ = state;
    if (state_ == TaskState::kCanceled || state_ == TaskState::kAborted) {
        try {
            throw ExecTaskException(state_ == TaskState::kCanceled
                                        ? "Task Cancelled"
                                        : "Task Aborted");
        } catch (const std::exception&) {
            exception_ = std::current_exception();
        }
    }

    completed_notifier.Activate(std::move(task_completed_promises_));

    terminate_requested_ = true;

    for (auto& driver : drivers_) {
        if (driver) {
            if (EnterForTerminateLocked(driver->state()) ==
                StopReason::kTerminate) {
                off_thread_drivers.push_back(std::move(driver));
                DriverClosedLocked();
            }
        }
    }
    completed_notifier.Notify();

    for (auto& driver : off_thread_drivers) {
        driver->CloseByTask();
    }
}

RowVectorPtr
Task::Next(ContinueFuture* future) {
    // NOTE: Task::Next is single-threaded execution
    AssertInfo(plan_fragment_.execution_strategy_ ==
                   plan::ExecutionStrategy::kUngrouped,
               "Single-threaded execution supports only ungrouped execution");

    AssertInfo(state_ == TaskState::kRunning,
               "Task has already finished processing.");

    if (driver_factories_.empty()) {
        AssertInfo(consumer_supplier_ == nullptr,
                   "Single-threaded execution doesn't support delivering "
                   "results to a "
                   "callback");

        LocalPlanner::Plan(plan_fragment_,
                           nullptr,
                           &driver_factories_,
                           *query_context_->query_config(),
                           1);

        for (const auto& factory : driver_factories_) {
            assert(factory->SupportSingleThreadExecution());
            num_ungrouped_drivers_ += factory->num_drivers_;
            num_total_drivers_ += factory->num_total_drivers_;
        }

        auto self = shared_from_this();
        std::vector<std::shared_ptr<Driver>> drivers =
            CreateDriversLocked(kUngroupedGroupId);

        drivers_ = std::move(drivers);
    }

    const auto num_drivers = drivers_.size();

    std::vector<ContinueFuture> futures;
    futures.resize(num_drivers);

    for (;;) {
        int runnable_drivers = 0;
        int blocked_drivers = 0;

        for (auto i = 0; i < num_drivers; ++i) {
            if (drivers_[i] == nullptr) {
                continue;
            }

            if (!futures[i].isReady()) {
                ++blocked_drivers;
                continue;
            }

            ++runnable_drivers;

            std::shared_ptr<BlockingState> blocking_state;

            auto result = drivers_[i]->Next(blocking_state);

            if (result) {
                return result;
            }

            if (blocking_state) {
                futures[i] = blocking_state->future();
            }

            if (error()) {
                std::rethrow_exception(error());
            }
        }

        if (runnable_drivers == 0) {
            if (blocked_drivers > 0) {
                if (!future) {
                    throw ExecDriverException(
                        "Cannot make progress as all remaining drivers are "
                        "blocked and user are not expected to wait.");
                } else {
                    std::vector<ContinueFuture> not_ready_futures;
                    for (auto& continue_future : futures) {
                        if (!continue_future.isReady()) {
                            not_ready_futures.emplace_back(
                                std::move(continue_future));
                        }
                    }
                    *future =
                        folly::collectAll(std::move(not_ready_futures)).unit();
                }
            }
            return nullptr;
        }
    }
}

}  // namespace exec
}  // namespace milvus
