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

#include "db/engine/SSExecutionEngineImpl.h"

#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

#include "segment/SSSegmentReader.h"
#include "segment/SSSegmentWriter.h"
#include "utils/CommonUtil.h"
#include "utils/Error.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/Status.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace engine {

namespace {
Status
GetRequiredIndexFields(const query::QueryPtr& query_ptr, std::vector<std::string>& field_names) {
    return Status::OK();
}

}  // namespace

SSExecutionEngineImpl::SSExecutionEngineImpl(const std::string& dir_root, const SegmentVisitorPtr& segment_visitor)
    : segment_visitor_(segment_visitor) {
    segment_reader_ = std::make_shared<segment::SSSegmentReader>(dir_root, segment_visitor);
}

Status
SSExecutionEngineImpl::SSExecutionEngineImpl::Load(const query::QueryPtr& query_ptr) {
    SegmentPtr segment_ptr;
    segment_reader_->GetSegment(segment_ptr);

    std::vector<std::string> field_names;
    GetRequiredIndexFields(query_ptr, field_names);
    for (auto& name : field_names) {
        FIELD_TYPE field_type = FIELD_TYPE::NONE;
        segment_ptr->GetFieldType(name, field_type);
        if (field_type == FIELD_TYPE::VECTOR || field_type == FIELD_TYPE::VECTOR_FLOAT ||
            field_type == FIELD_TYPE::VECTOR_BINARY) {
            knowhere::VecIndexPtr index_ptr;
            segment_reader_->LoadVectorIndex(name, index_ptr);
        } else {
            knowhere::IndexPtr index_ptr;
            segment_reader_->LoadStructuredIndex(name, index_ptr);
        }
    }

    return Status::OK();
}

Status
SSExecutionEngineImpl::CopyToGpu(uint64_t device_id) {
    SegmentPtr segment_ptr;
    segment_reader_->GetSegment(segment_ptr);

    return Status::OK();
}

Status
SSExecutionEngineImpl::Search(const query::QueryPtr& query_ptr, QueryResult& result) {
    return Status::OK();
}

Status
SSExecutionEngineImpl::BuildIndex(const std::string& field_name, const CollectionIndex& index) {
    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
