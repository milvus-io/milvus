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

#include "PrecomputedBitsetNode.h"

#include "common/EasyAssert.h"
#include "common/Tracer.h"
#include "exec/operator/Utils.h"

namespace milvus {
namespace exec {

PhyPrecomputedBitsetNode::PhyPrecomputedBitsetNode(
    int32_t operator_id,
    DriverContext* driverctx,
    const std::shared_ptr<const plan::PrecomputedBitsetNode>& node)
    : Operator(driverctx,
               node->output_type(),
               operator_id,
               node->id(),
               "PhyPrecomputedBitsetNode") {
    query_context_ = operator_context_->get_exec_context()->get_query_context();
}

RowVectorPtr
PhyPrecomputedBitsetNode::GetOutput() {
    milvus::exec::checkCancellation(query_context_);

    if (is_finished_) {
        return nullptr;
    }
    is_finished_ = true;

    auto bits = query_context_->get_precomputed_bitset();
    AssertInfo(bits != nullptr,
               "PhyPrecomputedBitsetNode requires precomputed bits on the query "
               "context; set_precomputed_bitset was not called");
    tracer::AddEvent("precomputed_bitset_reused");
    return bits;
}

}  // namespace exec
}  // namespace milvus
