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

#include <iostream>
#include <string>
#include <vector>
#include "db/snapshot/Resources.h"

namespace milvus {
namespace engine {
namespace snapshot {

struct SegmentFileContext {
    std::string field_name;
    std::string field_element_name;
    ID_TYPE segment_id;
    ID_TYPE partition_id;
};

struct LoadOperationContext {
    ID_TYPE id = 0;
    State status = INVALID;
    std::string name;
};

struct OperationContext {
    SegmentPtr new_segment = nullptr;
    SegmentCommitPtr new_segment_commit = nullptr;
    PartitionCommitPtr new_partition_commit = nullptr;
    SchemaCommitPtr new_schema_commit = nullptr;

    SegmentFilePtr stale_segment_file = nullptr;
    std::vector<SegmentPtr> stale_segments;

    FieldPtr prev_field = nullptr;
    FieldElementPtr prev_field_element = nullptr;

    SegmentPtr prev_segment = nullptr;
    SegmentCommitPtr prev_segment_commit = nullptr;
    PartitionPtr prev_partition = nullptr;
    PartitionCommitPtr prev_partition_commit = nullptr;
    CollectionCommitPtr prev_collection_commit = nullptr;

    SegmentFile::VecT new_segment_files;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
