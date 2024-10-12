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

#include "Driver.h"

#include <cassert>
#include <memory>

#include "common/EasyAssert.h"
#include "exec/operator/CallbackSink.h"
#include "exec/operator/CountNode.h"
#include "exec/operator/FilterBitsNode.h"
#include "exec/operator/IterativeFilterNode.h"
#include "exec/operator/MvccNode.h"
#include "exec/operator/Operator.h"
#include "exec/operator/VectorSearchNode.h"
#include "exec/operator/SearchGroupByNode.h"
#include "exec/operator/AggregationNode.h"
#include "exec/operator/ProjectNode.h"
#include "exec/Task.h"

#include "common/EasyAssert.h"

namespace milvus {
namespace exec {

std::atomic_uint64_t BlockingState::num_blocked_drivers_{0};

std::shared_ptr<QueryConfig>
DriverContext::GetQueryConfig() {
    return task_->query_context()->query_config();
}

std::shared_ptr<Driver>
DriverFactory::CreateDriver(std::unique_ptr<DriverContext> ctx,
                            std::function<int(int pipelineid)> num_drivers) {
    auto driver = std::shared_ptr<Driver>(new Driver());
    ctx->driver_ = driver.get();
    std::vector<std::unique_ptr<Operator>> operators;
    operators.reserve(plannodes_.size());

    for (size_t i = 0; i < plannodes_.size(); ++i) {
        auto id = operators.size();
        auto plannode = plannodes_[i];
        if (auto filterbitsnode =
                std::dynamic_pointer_cast<const plan::FilterBitsNode>(
                    plannode)) {
            operators.push_back(std::make_unique<PhyFilterBitsNode>(
                id, ctx.get(), filterbitsnode));
        } else if (auto filternode =
                       std::dynamic_pointer_cast<const plan::FilterNode>(
                           plannode)) {
            operators.push_back(std::make_unique<PhyIterativeFilterNode>(
                id, ctx.get(), filternode));
        } else if (auto mvccnode =
                       std::dynamic_pointer_cast<const plan::MvccNode>(
                           plannode)) {
            operators.push_back(
                std::make_unique<PhyMvccNode>(id, ctx.get(), mvccnode));
        } else if (auto countnode =
                       std::dynamic_pointer_cast<const plan::CountNode>(
                           plannode)) {
            operators.push_back(
                std::make_unique<PhyCountNode>(id, ctx.get(), countnode));
        } else if (auto vectorsearchnode =
                       std::dynamic_pointer_cast<const plan::VectorSearchNode>(
                           plannode)) {
            operators.push_back(std::make_unique<PhyVectorSearchNode>(
                id, ctx.get(), vectorsearchnode));
        } else if (auto vectorGroupByNode =
                       std::dynamic_pointer_cast<const plan::SearchGroupByNode>(
                           plannode)) {
            operators.push_back(std::make_unique<PhySearchGroupByNode>(
                id, ctx.get(), vectorGroupByNode));
        } else if (auto queryGroupByNode =
                       std::dynamic_pointer_cast<const plan::AggregationNode>(
                           plannode)) {
            operators.push_back(std::make_unique<PhyAggregationNode>(
                id, ctx.get(), queryGroupByNode));
        } else if (auto projectNode =
                       std::dynamic_pointer_cast<const plan::ProjectNode>(
                           plannode)) {
            operators.push_back(
                std::make_unique<PhyProjectNode>(id, ctx.get(), projectNode));
        }
        // TODO: add more operators
    }

    if (consumer_supplier_) {
        operators.push_back(consumer_supplier_(operators.size(), ctx.get()));
    }

    driver->Init(std::move(ctx), std::move(operators));

    return driver;
}

void
Driver::Enqueue(std::shared_ptr<Driver> driver) {
    if (driver->closed_) {
        return;
    }

    driver->get_task()->query_context()->executor()->add(
        [driver]() { Driver::Run(driver); });
}

void
Driver::Run(std::shared_ptr<Driver> self) {
    std::shared_ptr<BlockingState> blocking_state;
    RowVectorPtr result;
    auto reason = self->RunInternal(self, blocking_state, result);

    AssertInfo(result == nullptr,
               "The last operator (sink) must not produce any results.");

    if (reason == StopReason::kBlock) {
        return;
    }

    switch (reason) {
        case StopReason::kBlock:
            BlockingState::SetResume(blocking_state);
            return;
        case StopReason::kYield:
            Enqueue(self);
        case StopReason::kPause:
        case StopReason::kTerminate:
        case StopReason::kAlreadyTerminated:
        case StopReason::kAtEnd:
            return;
        default:
            AssertInfo(false, "Unhandled stop reason");
    }
}

void
Driver::initializeOperators() {
    if (operatorsInitialized_) {
        return;
    }
    operatorsInitialized_ = true;
    for (auto& op : operators_) {
        op->initialize();
    }
}

void
Driver::Init(std::unique_ptr<DriverContext> ctx,
             std::vector<std::unique_ptr<Operator>> operators) {
    assert(ctx != nullptr);
    ctx_ = std::move(ctx);
    AssertInfo(operators.size() != 0, "operators in driver must not empty");
    operators_ = std::move(operators);
    current_operator_index_ = operators_.size() - 1;
}

void
Driver::Close() {
    if (closed_) {
        return;
    }

    for (auto& op : operators_) {
        op->Close();
    }

    closed_ = true;

    Task::RemoveDriver(ctx_->task_, this);
}

RowVectorPtr
Driver::Next(std::shared_ptr<BlockingState>& blocking_state) {
    auto self = shared_from_this();

    RowVectorPtr result;
    auto stop = RunInternal(self, blocking_state, result);

    Assert(stop == StopReason::kBlock || stop == StopReason::kAtEnd ||
           stop == StopReason::kAlreadyTerminated);
    return result;
}

#define CALL_OPERATOR(call_func, operator, method_name)                        \
    try {                                                                      \
        call_func;                                                             \
    } catch (SegcoreError & e) {                                               \
        auto err_msg = fmt::format(                                            \
            "Operator::{} failed for [Operator:{}, plan node id: "             \
            "{}] : {}",                                                        \
            method_name,                                                       \
            operator->get_operator_type(),                                     \
            operator->get_plannode_id(),                                       \
            e.what());                                                         \
        LOG_ERROR(err_msg);                                                    \
        throw ExecOperatorException(err_msg);                                  \
    } catch (std::exception & e) {                                             \
        throw ExecOperatorException(                                           \
            fmt::format("Operator::{} failed for [Operator:{}, plan node id: " \
                        "{}] : {}",                                            \
                        method_name,                                           \
                        operator->get_operator_type(),                         \
                        operator->get_plannode_id(),                           \
                        e.what()));                                            \
    }

StopReason
Driver::RunInternal(std::shared_ptr<Driver>& self,
                    std::shared_ptr<BlockingState>& blocking_state,
                    RowVectorPtr& result) {
    try {
        initializeOperators();
        int num_operators = operators_.size();
        ContinueFuture future;

        for (;;) {
            for (int32_t i = num_operators - 1; i >= 0; --i) {
                auto op = operators_[i].get();
                current_operator_index_ = i;
                CALL_OPERATOR(
                    blocking_reason_ = op->IsBlocked(&future), op, "IsBlocked");
                if (blocking_reason_ != BlockingReason::kNotBlocked) {
                    blocking_state = std::make_shared<BlockingState>(
                        self, std::move(future), op, blocking_reason_);
                    return StopReason::kBlock;
                }
                Operator* next_op = nullptr;

                if (i < operators_.size() - 1) {
                    next_op = operators_[i + 1].get();
                    CALL_OPERATOR(
                        blocking_reason_ = next_op->IsBlocked(&future),
                        next_op,
                        "IsBlocked");
                    if (blocking_reason_ != BlockingReason::kNotBlocked) {
                        blocking_state = std::make_shared<BlockingState>(
                            self, std::move(future), next_op, blocking_reason_);
                        return StopReason::kBlock;
                    }

                    bool needs_input;
                    CALL_OPERATOR(needs_input = next_op->NeedInput(),
                                  next_op,
                                  "NeedInput");
                    if (needs_input) {
                        RowVectorPtr result;
                        {
                            CALL_OPERATOR(
                                result = op->GetOutput(), op, "GetOutput");
                            if (result) {
                                AssertInfo(
                                    result->size() > 0,
                                    fmt::format(
                                        "GetOutput must return nullptr or "
                                        "a non-empty vector: {}",
                                        op->get_operator_type()));
                            }
                        }
                        if (result) {
                            CALL_OPERATOR(
                                next_op->AddInput(result), next_op, "AddInput");
                            i += 2;
                            continue;
                        } else {
                            CALL_OPERATOR(
                                blocking_reason_ = op->IsBlocked(&future),
                                op,
                                "IsBlocked");
                            if (blocking_reason_ !=
                                BlockingReason::kNotBlocked) {
                                blocking_state =
                                    std::make_shared<BlockingState>(
                                        self,
                                        std::move(future),
                                        next_op,
                                        blocking_reason_);
                                return StopReason::kBlock;
                            }
                            if (op->IsFinished()) {
                                CALL_OPERATOR(next_op->NoMoreInput(),
                                              next_op,
                                              "NoMoreInput");
                                break;
                            }
                        }
                    }
                } else {
                    {
                        CALL_OPERATOR(
                            result = op->GetOutput(), op, "GetOutput");
                        if (result) {
                            AssertInfo(
                                result->size() > 0,
                                fmt::format("GetOutput must return nullptr or "
                                            "a non-empty vector: {}",
                                            op->get_operator_type()));
                            blocking_reason_ = BlockingReason::kWaitForConsumer;
                            return StopReason::kBlock;
                        }
                    }
                    if (op->IsFinished()) {
                        Close();
                        return StopReason::kAtEnd;
                    }
                    continue;
                }
            }
        }
    } catch (std::exception& e) {
        get_task()->SetError(std::current_exception());
        return StopReason::kAlreadyTerminated;
    }
}

static bool
MustStartNewPipeline(std::shared_ptr<const plan::PlanNode> plannode,
                     int source_id) {
    //TODO: support LocalMerge and other shuffle
    return source_id != 0;
}

OperatorSupplier
MakeConsumerSupplier(ConsumerSupplier supplier) {
    if (supplier) {
        return [supplier](int32_t operator_id, DriverContext* ctx) {
            return std::make_unique<CallbackSink>(operator_id, ctx, supplier());
        };
    }
    return nullptr;
}

uint32_t
MaxDrivers(const DriverFactory* factory, const QueryConfig& config) {
    return 1;
}

static void
SplitPlan(const std::shared_ptr<const plan::PlanNode>& plannode,
          std::vector<std::shared_ptr<const plan::PlanNode>>* current_plannodes,
          const std::shared_ptr<const plan::PlanNode>& consumer_node,
          OperatorSupplier operator_supplier,
          std::vector<std::unique_ptr<DriverFactory>>* driver_factories) {
    if (!current_plannodes) {
        driver_factories->push_back(std::make_unique<DriverFactory>());
        current_plannodes = &driver_factories->back()->plannodes_;
        driver_factories->back()->consumer_supplier_ = operator_supplier;
        driver_factories->back()->consumer_node_ = consumer_node;
    }

    auto sources = plannode->sources();
    if (sources.empty()) {
        driver_factories->back()->is_input_driver_ = true;
    } else {
        for (int i = 0; i < sources.size(); ++i) {
            SplitPlan(
                sources[i],
                MustStartNewPipeline(plannode, i) ? nullptr : current_plannodes,
                plannode,
                nullptr,
                driver_factories);
        }
    }
    current_plannodes->push_back(plannode);
}

void
LocalPlanner::Plan(
    const plan::PlanFragment& fragment,
    ConsumerSupplier consumer_supplier,
    std::vector<std::unique_ptr<DriverFactory>>* driver_factories,
    const QueryConfig& config,
    uint32_t max_drivers) {
    SplitPlan(fragment.plan_node_,
              nullptr,
              nullptr,
              MakeConsumerSupplier(consumer_supplier),
              driver_factories);

    (*driver_factories)[0]->is_output_driver_ = true;

    for (auto& factory : *driver_factories) {
        factory->max_drivers_ = MaxDrivers(factory.get(), config);
        factory->num_drivers_ = std::min(factory->max_drivers_, max_drivers);

        if (factory->is_group_execution_) {
            factory->num_total_drivers_ =
                factory->num_drivers_ * fragment.num_splitgroups_;
        } else {
            factory->num_total_drivers_ = factory->num_drivers_;
        }
    }
}

}  // namespace exec
}  // namespace milvus
