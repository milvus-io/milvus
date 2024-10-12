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

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/Driver.h"
#include "exec/Task.h"
#include "exec/QueryContext.h"
#include "plan/PlanNode.h"

namespace milvus {
namespace exec {

class OperatorContext {
 public:
    OperatorContext(DriverContext* driverCtx,
                    const plan::PlanNodeId& plannodeid,
                    int32_t operator_id,
                    const std::string& operator_type = "")
        : driver_context_(driverCtx),
          plannode_id_(plannodeid),
          operator_id_(operator_id),
          operator_type_(operator_type) {
    }

    ExecContext*
    get_exec_context() const {
        if (!exec_context_) {
            exec_context_ = std::make_unique<ExecContext>(
                driver_context_->task_->query_context().get());
        }
        return exec_context_.get();
    }

    const std::shared_ptr<Task>&
    get_task() const {
        return driver_context_->task_;
    }

    const std::string&
    get_task_id() const {
        return driver_context_->task_->taskid();
    }

    DriverContext*
    get_driver_context() const {
        return driver_context_;
    }

    const plan::PlanNodeId&
    get_plannode_id() const {
        return plannode_id_;
    }

    const std::string&
    get_operator_type() const {
        return operator_type_;
    }

    const int32_t
    get_operator_id() const {
        return operator_id_;
    }

 private:
    DriverContext* driver_context_;
    plan::PlanNodeId plannode_id_;
    int32_t operator_id_;
    std::string operator_type_;

    mutable std::unique_ptr<ExecContext> exec_context_;
};

class Operator {
 public:
    Operator(DriverContext* ctx,
             RowTypePtr output_type,
             int32_t operator_id,
             const std::string& plannode_id,
             const std::string& operator_type = "")
        : operator_context_(std::make_unique<OperatorContext>(
              ctx, plannode_id, operator_id, operator_type)),
          output_type_(output_type) {
    }

    virtual ~Operator() = default;

    /// Does initialization work for this operator which requires memory
    /// allocation from memory pool that can't be done under operator constructor.
    ///
    /// NOTE: the default implementation set 'initialized_' to true to ensure we
    /// never call this more than once. The overload initialize() implementation
    /// must call this base implementation first.
    virtual void
    initialize();

    virtual bool
    NeedInput() const = 0;

    virtual void
    AddInput(RowVectorPtr& input) = 0;

    virtual void
    NoMoreInput() {
        no_more_input_ = true;
    }

    virtual RowVectorPtr
    GetOutput() = 0;

    virtual bool
    IsFinished() = 0;

    virtual bool
    IsFilter() const = 0;

    virtual BlockingReason
    IsBlocked(ContinueFuture* future) = 0;

    virtual void
    Close() {
        input_ = nullptr;
        results_.clear();
    }

    virtual bool
    PreserveOrder() const {
        return false;
    }

    const std::string&
    get_operator_type() const {
        return operator_context_->get_operator_type();
    }

    const int32_t
    get_operator_id() const {
        return operator_context_->get_operator_id();
    }

    const plan::PlanNodeId&
    get_plannode_id() const {
        return operator_context_->get_plannode_id();
    }

    virtual std::string
    ToString() const {
        return "Base Operator";
    }

    virtual const RowTypePtr&
    OutputType() const {
        return output_type_;
    }

 protected:
    std::unique_ptr<OperatorContext> operator_context_;

    RowTypePtr output_type_;

    RowVectorPtr input_;

    bool no_more_input_{false};

    std::vector<VectorPtr> results_;
};

class SourceOperator : public Operator {
 public:
    SourceOperator(DriverContext* driver_ctx,
                   RowTypePtr out_type,
                   int32_t operator_id,
                   const std::string& plannode_id,
                   const std::string& operator_type)
        : Operator(
              driver_ctx, out_type, operator_id, plannode_id, operator_type) {
    }

    bool
    NeedInput() const override {
        return false;
    }

    void
    AddInput(RowVectorPtr& /* unused */) override {
        PanicInfo(NotImplemented, "SourceOperator does not support addInput()");
    }

    void
    NoMoreInput() override {
        PanicInfo(NotImplemented,
                  "SourceOperator does not support noMoreInput()");
    }

    virtual std::string
    ToString() const override {
        return "source operator";
    }
};

}  // namespace exec
}  // namespace milvus