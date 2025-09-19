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
};

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

class Driver : public std::enable_shared_from_this<Driver> {
 public:
    static void
    Enqueue(std::shared_ptr<Driver> instance);

    RowVectorPtr
    Next(std::shared_ptr<BlockingState>& blocking_state);

    DriverContext*
    get_driver_context() const {
        return ctx_.get();
    }

    const std::shared_ptr<Task>&
    get_task() const {
        return ctx_->task_;
    }

    BlockingReason
    GetBlockingReason() const {
        return blocking_reason_;
    }

    void
    Init(std::unique_ptr<DriverContext> driver_ctx,
         std::vector<std::unique_ptr<Operator>> operators);

    void
    CloseByTask() {
        Close();
    }

 private:
    Driver() = default;

    void
    EnqueueInternal() {
    }

    /// Invoked to initialize the operators from this driver once on its first
    /// execution.
    void
    initializeOperators();

    static void
    Run(std::shared_ptr<Driver> self);

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

    bool operatorsInitialized_{false};

    BlockingReason blocking_reason_{BlockingReason::kNotBlocked};

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