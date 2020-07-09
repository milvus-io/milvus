// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "db/merge/SSMergeTask.h"
#include "db/Utils.h"
#include "metrics/Metrics.h"
#include "segment/SegmentReader.h"
#include "segment/SegmentWriter.h"
#include "utils/Log.h"

#include <memory>
#include <string>

namespace milvus {
namespace engine {

SSMergeTask::SSMergeTask(const DBOptions& options, const std::string& collection_name,
                         const snapshot::IDS_TYPE& segments)
    : options_(options), collection_name_(collection_name), segments_(segments) {
}

Status
SSMergeTask::Execute() {
    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
