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
#include <map>
#include <set>
#include <vector>
#include "ResourceTypes.h"
#include "Resources.h"
#include "ScopedResource.h"

namespace milvus {
namespace engine {
namespace snapshot {

using CollectionScopedT = ScopedResource<Collection>;
using CollectionCommitScopedT = ScopedResource<CollectionCommit>;

using SchemaCommitScopedT = ScopedResource<SchemaCommit>;
using SchemaCommitsT = std::map<ID_TYPE, SchemaCommitScopedT>;

using FieldScopedT = ScopedResource<Field>;
using FieldsT = std::map<ID_TYPE, FieldScopedT>;
using FieldCommitScopedT = ScopedResource<FieldCommit>;
using FieldCommitsT = std::map<ID_TYPE, FieldCommitScopedT>;
using FieldElementScopedT = ScopedResource<FieldElement>;
using FieldElementsT = std::map<ID_TYPE, FieldElementScopedT>;

using PartitionScopedT = ScopedResource<Partition>;
using PartitionCommitScopedT = ScopedResource<PartitionCommit>;
using PartitionsT = std::map<ID_TYPE, PartitionScopedT>;
using PartitionCommitsT = std::map<ID_TYPE, PartitionCommitScopedT>;

using SegmentScopedT = ScopedResource<Segment>;
using SegmentCommitScopedT = ScopedResource<SegmentCommit>;
using SegmentFileScopedT = ScopedResource<SegmentFile>;
using SegmentsT = std::map<ID_TYPE, SegmentScopedT>;
using SegmentCommitsT = std::map<ID_TYPE, SegmentCommitScopedT>;
using SegmentFilesT = std::map<ID_TYPE, SegmentFileScopedT>;

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
