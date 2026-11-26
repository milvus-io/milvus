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

#include "pb/cgo_msg.pb.h"
#include "segcore/SegmentLoadInfo.h"

namespace milvus::segcore {

// Estimates one loader batch from metadata only. Runtime limits are read once;
// all per-segment and batch arithmetic runs without holding a global lock.
proto::cgo::BatchLoadResourceResponse
EstimateSegmentResources(const proto::cgo::BatchLoadResourceRequest& request);

// Estimates only actions in diff for loading; final occupancy uses the target.
// Input must describe exactly one segment. No field/index payload is read.
proto::cgo::CoreResourceUsage
EstimateLoadDiffResources(const proto::cgo::BatchLoadResourceRequest& request,
                          const SegmentLoadInfo& target,
                          const LoadDiff& diff,
                          const SealedFieldLayoutPtr& fields);

}  // namespace milvus::segcore
