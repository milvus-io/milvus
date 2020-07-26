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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "ExecutionEngine.h"
#include "db/SnapshotVisitor.h"
#include "segment/SegmentReader.h"

namespace milvus {
namespace engine {

class ExecutionEngineImpl : public ExecutionEngine {
 public:
    ExecutionEngineImpl(const std::string& dir_root, const SegmentVisitorPtr& segment_visitor);

    Status
    Load(ExecutionEngineContext& context) override;

    Status
    CopyToGpu(uint64_t device_id) override;

    Status
    Search(ExecutionEngineContext& context) override;

    Status
    BuildIndex() override;

 private:
    knowhere::VecIndexPtr
    CreatetVecIndex(const std::string& index_name);

    Status
    LoadForSearch(const query::QueryPtr& query_ptr);

    Status
    LoadForIndex();

    Status
    Load(const std::vector<std::string>& field_names);

 private:
    std::string root_path_;
    SegmentVisitorPtr segment_visitor_;
    segment::SegmentReaderPtr segment_reader_;

    int64_t gpu_num_ = 0;
    bool gpu_enable_ = false;
};

}  // namespace engine
}  // namespace milvus
