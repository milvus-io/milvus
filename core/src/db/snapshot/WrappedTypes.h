#pragma once
#include "Resources.h"
#include "ScopedResource.h"
#include "ResourceTypes.h"
#include <map>
#include <vector>
#include <set>

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
