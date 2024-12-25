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
#include "Operator.h"
#include "plan/PlanNode.h"

namespace milvus {
namespace exec {
class PhyProjectNode : public Operator {
 public:
    PhyProjectNode(int32_t operator_id,
                   DriverContext* ctx,
                   const std::shared_ptr<const plan::ProjectNode>& projectNode);

    bool
    IsFilter() const override {
        return false;
    }

    bool
    NeedInput() const override {
        return true;
    }

    void
    AddInput(RowVectorPtr& input) override;

    RowVectorPtr
    GetOutput() override;

    bool
    IsFinished() override {
        return is_finished_;
    }

    BlockingReason
    IsBlocked(ContinueFuture* /* unused */) override {
        return BlockingReason::kNotBlocked;
    }

    std::string
    ToString() const override {
        return "Project Operator";
    }

 private:
    FieldDataPtr
    projectFieldData(FieldId fieldId,
                     milvus::DataType dataType,
                     const int64_t* seg_offsets,
                     int64_t count) const;

 private:
    const segcore::SegmentInternalInterface* segment_;
    bool is_finished_{false};
    const std::vector<FieldId> fields_to_project_;
};
}  // namespace exec
}  // namespace milvus
