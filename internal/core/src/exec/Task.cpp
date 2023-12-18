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

namespace milvus {
namespace exec {

// Special group id to reflect the ungrouped execution.
constexpr uint32_t kUngroupedGroupId{std::numeric_limits<uint32_t>::max()};

std::string
MakeUuid() {
    return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
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
        std::lock_guard<std::mutex> l(mutex_);
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

void
Task::CreateDriversLocked(std::shared_ptr<Task>& self,
                          uint32_t split_group_id,
                          std::vector<std::shared_ptr<Driver>>& out) {
    const bool is_group_execution_drivers =
        (split_group_id != kUngroupedGroupId);
    const auto num_pipelines = driver_factories_.size();

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
        AssertInfo(
            consumer_supplier_ == nullptr,
            "Single-threaded execution doesn't support delivering results to a "
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
        std::vector<std::shared_ptr<Driver>> drivers;

        drivers.reserve(num_ungrouped_drivers_);
        CreateDriversLocked(self, kUngroupedGroupId, drivers);

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