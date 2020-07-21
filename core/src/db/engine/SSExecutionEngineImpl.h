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

#include "SSExecutionEngine.h"
#include "db/SnapshotVisitor.h"
#include "segment/SSSegmentReader.h"

namespace milvus {
namespace engine {

class SSExecutionEngineImpl : public SSExecutionEngine {
 public:
    SSExecutionEngineImpl(const std::string& dir_root, const SegmentVisitorPtr& segment_visitor);

    Status
    Load(const query::QueryPtr& query_ptr) override;

    Status
    CopyToGpu(uint64_t device_id) override;

    Status
    Search(const query::QueryPtr& query_ptr, QueryResult& result) override;

    Status
    BuildIndex(const std::string& field_name, const CollectionIndex& index) override;

 private:
    SegmentVisitorPtr segment_visitor_;
    segment::SSSegmentReaderPtr segment_reader_;
};

}  // namespace engine
}  // namespace milvus
