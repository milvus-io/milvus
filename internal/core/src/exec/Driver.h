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

#include <functional>
#include <memory>
#include <string>
#include <sys/syscall.h>
#include <vector>

#include "common/Types.h"
#include "common/Promise.h"
#include "exec/QueryContext.h"
#include "plan/PlanNode.h"

namespace milvus {
namespace exec {

enum class StopReason {
    // Keep running.
    kNone,
    // Go off thread and do not schedule more activity.
    kPause,
    // Stop and free all. This is returned once and the thread that gets
    // this value is responsible for freeing the state associated with
    // the thread. Other threads will get kAlreadyTerminated after the
    // first thread has received kTerminate.
    kTerminate,
    kAlreadyTerminated,
    // Go off thread and then enqueue to the back of the runnable queue.
    kYield,
    // Must wait for external events.
    kBlock,
    // No more data to produce.
    kAtEnd,
    kAlreadyOnThread

};  // namespace milvus

inline std::string
StopReasonToString(StopReason reason) {
    switch (reason) {
        case StopReason::kNone:
            return "None";
        case StopReason::kPause:
            return "Pause";
        case StopReason::kTerminate:
            return "Terminate";
        case StopReason::kAlreadyTerminated:
            return "AlreadyTerminated";
        case StopReason::kYield:
            return "Yield";
        case StopReason::kBlock:
            return "Block";
        case StopReason::kAtEnd:
            return "AtEnd";
        case StopReason::kAlreadyOnThread:
            return "AlreadyOnThread";
        default:
            return "Unknown";
    }
}

enum class BlockingReason {
    kNotBlocked,
    kWaitForConsumer,
    kWaitForSplit,
    kWaitForExchange,
    kWaitForJoinBuild,
    /// For a build operator, it is blocked waiting for the probe operators to
    /// finish probing before build the next hash table from one of the previously
    /// spilled partition data.
    /// For a probe operator, it is blocked waiting for all its peer probe
    /// operators to finish probing before notifying the build operators to build
    /// the next hash table from the previously spilled data.
    kWaitForJoinProbe,
    kWaitForMemory,
    kWaitForConnector,
    /// Build operator is blocked waiting for all its peers to stop to run group
    /// spill on all of them.
    kWaitForSpill,
};

inline std::string
BlockingReasonToString(BlockingReason reason) {
    switch (reason) {
        case BlockingReason::kNotBlocked:
            return "Not Blocked";
        case BlockingReason::kWaitForConsumer:
            return "Waiting for Consumer";
        case BlockingReason::kWaitForSplit:
            return "Waiting for Split";
        case BlockingReason::kWaitForExchange:
            return "Waiting for Exchange";
        case BlockingReason::kWaitForJoinBuild:
            return "Waiting for Join Build";
        case BlockingReason::kWaitForJoinProbe:
            return "Waiting for Join Probe";
        case BlockingReason::kWaitForMemory:
            return "Waiting for Memory";
        case BlockingReason::kWaitForConnector:
            return "Waiting for Connector";
        case BlockingReason::kWaitForSpill:
            return "Waiting for Spill";
        default:
            return "Unknown Blocking Reason";
    }
}

class Driver;
class Operator;
class Task;
class BlockingState {
 public:
    BlockingState(std::shared_ptr<Driver> driver,
                  ContinueFuture&& future,
                  Operator* op,
                  BlockingReason reason)
        : driver_(std::move(driver)),
          future_(std::move(future)),
          operator_(op),
          reason_(reason) {
        num_blocked_drivers_++;
    }

    ~BlockingState() {
        num_blocked_drivers_--;
    }

    static void
    SetResume(std::shared_ptr<BlockingState> state) {
    }

    Operator*
    op() {
        return operator_;
    }

    BlockingReason
    reason() {
        return reason_;
    }

    // Moves out the blocking future stored inside. Can be called only once. Used
    // in single-threaded execution.
    ContinueFuture
    future() {
        return std::move(future_);
    }

    // Returns total number of drivers process wide that are currently in blocked
    // state.
    static uint64_t
    get_num_blocked_drivers() {
        return num_blocked_drivers_;
    }

 private:
    std::shared_ptr<Driver> driver_;
    ContinueFuture future_;
    Operator* operator_;
    BlockingReason reason_;
    static std::atomic_uint64_t num_blocked_drivers_;
};

struct DriverContext {
    int driverid_;
    int pipelineid_;
    uint32_t split_groupid_;
    uint32_t partitionid_;

    std::shared_ptr<Task> task_;
    Driver* driver_;

    explicit DriverContext(std::shared_ptr<Task> task,
                           int driverid,
                           int pipilineid,
                           uint32_t split_group_id,
                           uint32_t partition_id)
        : driverid_(driverid),
          pipelineid_(pipilineid),
          split_groupid_(split_group_id),
          partitionid_(partition_id),
          task_(task) {
    }

    std::shared_ptr<QueryConfig>
    GetQueryConfig();
};
using OperatorSupplier = std::function<std::unique_ptr<Operator>(
    int32_t operatorid, DriverContext* ctx)>;

struct DriverFactory {
    std::vector<std::shared_ptr<const plan::PlanNode>> plannodes_;
    OperatorSupplier consumer_supplier_;
    // The (local) node that will consume results supplied by this pipeline.
    // Can be null. We use that to determine the max drivers.
    std::shared_ptr<const plan::PlanNode> consumer_node_;
    uint32_t max_drivers_;
    uint32_t num_drivers_;
    uint32_t num_total_drivers_;

    bool is_group_execution_;
    bool is_input_driver_;
    bool is_output_driver_;

    std::shared_ptr<Driver>
    CreateDriver(std::unique_ptr<DriverContext> ctx,
                 // TODO: support exchange function
                 // std::shared_ptr<ExchangeClient> exchange_client,
                 std::function<int(int pipilineid)> num_driver);

    // TODO: support ditribution compute
    bool
    SupportSingleThreadExecution() const {
        return true;
    }
};

struct ThreadState {
    std::atomic<std::thread::id> thread_id_{std::thread::id()};
    std::atomic<int32_t> tid_{0};
    // Whether have enqueued to executor
    std::atomic<bool> is_enqueued_{false};
    std::atomic<bool> is_terminated_{false};
    bool has_blocking_future_{false};
    bool is_suspended{false};

    bool
    IsOnThread() const {
        return thread_id_.load() == std::this_thread::get_id();
    }

    void
    SetThread() {
        thread_id_.store(std::this_thread::get_id());
#if !defined(__APPLE__)
        tid_.store(syscall(SYS_gettid));
#endif
    }

    void
    ClearThread() {
        thread_id_.store(std::thread::id());
        tid_.store(0);
    }
};
class Driver : public std::enable_shared_from_this<Driver> {
 public:
    static void
    Enqueue(std::shared_ptr<Driver> instance);

    RowVectorPtr
    Next(std::shared_ptr<BlockingState>& blocking_state);

    DriverContext*
    driver_context() const {
        return ctx_.get();
    }

    bool
    IsClosed() const {
        return closed_;
    }

    const std::shared_ptr<Task>&
    task() const {
        return ctx_->task_;
    }

    ThreadState&
    state() {
        return state_;
    }

    BlockingReason
    blocking_reason() const {
        return blocking_reason_;
    }

    void
    Init(std::unique_ptr<DriverContext> driver_ctx,
         std::vector<std::unique_ptr<Operator>> operators);

    bool
    IsOnThread() const {
        return state_.IsOnThread();
    }

    bool
    IsTerminated() const {
        return state_.is_terminated_;
    }

    std::string
    ToString() const;

    void
    CloseOperators();

    void
    CloseByTask();

    void
    EnqueueInternal() {
        Assert(!state_.is_enqueued_);
        state_.is_enqueued_ = true;
    }

    static void
    Run(std::shared_ptr<Driver> self);

 private:
    Driver() = default;

    StopReason
    RunInternal(std::shared_ptr<Driver>& self,
                std::shared_ptr<BlockingState>& blocking_state,
                RowVectorPtr& result);

    void
    Close();

    std::unique_ptr<DriverContext> ctx_;

    std::atomic_bool closed_{false};

    std::vector<std::unique_ptr<Operator>> operators_;

    size_t current_operator_index_{0};

    BlockingReason blocking_reason_{BlockingReason::kNotBlocked};

    ThreadState state_;

    friend struct DriverFactory;
};

using Consumer = std::function<BlockingReason(RowVectorPtr, ContinueFuture*)>;
using ConsumerSupplier = std::function<Consumer()>;
class LocalPlanner {
 public:
    static void
    Plan(const plan::PlanFragment& fragment,
         ConsumerSupplier consumer_supplier,
         std::vector<std::unique_ptr<DriverFactory>>* driver_factories,
         const QueryConfig& config,
         uint32_t max_drivers);
};

}  // namespace exec
}  // namespace milvus