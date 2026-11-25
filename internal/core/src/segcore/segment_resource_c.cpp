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

#include "segcore/segment_resource_c.h"

#include <limits>

#include "common/CGoCatch.h"
#include "common/EasyAssert.h"
#include "common/protobuf_utils.h"
#include "monitor/scope_metric.h"
#include "segcore/SegmentResourceEstimator.h"

CStatus
EstimateSegmentResourcesFromSerializedInfo(const uint8_t* serialized_request,
                                           uint64_t len,
                                           ProtoLayoutInterface output) {
    SCOPE_CGO_CALL_METRIC();
    try {
        AssertInfo(output != nullptr, "resource estimate output is null");
        AssertInfo(serialized_request != nullptr || len == 0,
                   "resource estimate request is null");
        AssertInfo(len <= std::numeric_limits<int>::max(),
                   "resource estimate request exceeds protobuf size limit");
        milvus::proto::cgo::BatchLoadResourceRequest request;
        AssertInfo(
            request.ParseFromArray(serialized_request, static_cast<int>(len)),
            "failed to parse resource estimate request");
        auto response = milvus::segcore::EstimateSegmentResources(request);
        AssertInfo(reinterpret_cast<milvus::ProtoLayout*>(output)
                       ->SerializeAndHoldProto(response),
                   "failed to serialize resource estimate response");
        return milvus::SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}
