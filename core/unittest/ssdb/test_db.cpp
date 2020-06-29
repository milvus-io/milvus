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

#include <fiu-control.h>
#include <fiu-local.h>
#include <gtest/gtest.h>

#include <string>
#include <set>
#include <algorithm>

#include "ssdb/utils.h"
#include "db/snapshot/CompoundOperations.h"
#include "db/snapshot/Context.h"
#include "db/snapshot/EventExecutor.h"
#include "db/snapshot/OperationExecutor.h"
#include "db/snapshot/ReferenceProxy.h"
#include "db/snapshot/ResourceHolders.h"
#include "db/snapshot/ScopedResource.h"
#include "db/snapshot/Snapshots.h"
#include "db/snapshot/Store.h"
#include "db/snapshot/WrappedTypes.h"

using ID_TYPE = milvus::engine::snapshot::ID_TYPE;
using IDS_TYPE = milvus::engine::snapshot::IDS_TYPE;
using LSN_TYPE = milvus::engine::snapshot::LSN_TYPE;
using MappingT = milvus::engine::snapshot::MappingT;
using LoadOperationContext = milvus::engine::snapshot::LoadOperationContext;
using CreateCollectionContext = milvus::engine::snapshot::CreateCollectionContext;
using SegmentFileContext = milvus::engine::snapshot::SegmentFileContext;
using OperationContext = milvus::engine::snapshot::OperationContext;
using PartitionContext = milvus::engine::snapshot::PartitionContext;
using BuildOperation = milvus::engine::snapshot::BuildOperation;
using MergeOperation = milvus::engine::snapshot::MergeOperation;
using CreateCollectionOperation = milvus::engine::snapshot::CreateCollectionOperation;
using NewSegmentOperation = milvus::engine::snapshot::NewSegmentOperation;
using DropPartitionOperation = milvus::engine::snapshot::DropPartitionOperation;
using CreatePartitionOperation = milvus::engine::snapshot::CreatePartitionOperation;
using DropCollectionOperation = milvus::engine::snapshot::DropCollectionOperation;
using CollectionCommitsHolder = milvus::engine::snapshot::CollectionCommitsHolder;
using CollectionsHolder = milvus::engine::snapshot::CollectionsHolder;
using CollectionScopedT = milvus::engine::snapshot::CollectionScopedT;
using Collection = milvus::engine::snapshot::Collection;
using CollectionPtr = milvus::engine::snapshot::CollectionPtr;
using Partition = milvus::engine::snapshot::Partition;
using PartitionPtr = milvus::engine::snapshot::PartitionPtr;
using Segment = milvus::engine::snapshot::Segment;
using SegmentPtr = milvus::engine::snapshot::SegmentPtr;
using SegmentFile = milvus::engine::snapshot::SegmentFile;
using SegmentFilePtr = milvus::engine::snapshot::SegmentFilePtr;
using Field = milvus::engine::snapshot::Field;
using FieldElement = milvus::engine::snapshot::FieldElement;
using Snapshots = milvus::engine::snapshot::Snapshots;
using ScopedSnapshotT = milvus::engine::snapshot::ScopedSnapshotT;
using ReferenceProxy = milvus::engine::snapshot::ReferenceProxy;
using Queue = milvus::BlockingQueue<ID_TYPE>;
using TQueue = milvus::BlockingQueue<std::tuple<ID_TYPE, ID_TYPE>>;
using SoftDeleteCollectionOperation = milvus::engine::snapshot::SoftDeleteOperation<Collection>;
using ParamsField = milvus::engine::snapshot::ParamsField;
using IteratePartitionHandler = milvus::engine::snapshot::IterateHandler<Partition>;

TEST_F(SSDBTest, CreateCollection) {
}
