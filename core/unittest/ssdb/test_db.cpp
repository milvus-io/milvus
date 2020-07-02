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
using SSDBImpl = milvus::engine::SSDBImpl;

milvus::Status
CreateCollection(std::shared_ptr<SSDBImpl> db, const std::string& collection_name, const LSN_TYPE& lsn) {
    CreateCollectionContext context;
    context.lsn = lsn;
    auto collection_schema = std::make_shared<Collection>(collection_name);
    context.collection = collection_schema;
    auto vector_field = std::make_shared<Field>("vector", 0,
            milvus::engine::snapshot::FieldType::VECTOR);
    auto vector_field_element = std::make_shared<FieldElement>(0, 0, "ivfsq8",
            milvus::engine::snapshot::FieldElementType::IVFSQ8);
    auto int_field = std::make_shared<Field>("int", 0,
            milvus::engine::snapshot::FieldType::INT32);
    context.fields_schema[vector_field] = {vector_field_element};
    context.fields_schema[int_field] = {};

    return db->CreateCollection(context);
}

TEST_F(SSDBTest, CollectionTest) {
    LSN_TYPE lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn) {
        return ++lsn;
    };
    std::string c1 = "c1";
    auto status = CreateCollection(db_, c1, next_lsn());
    ASSERT_TRUE(status.ok());

    ScopedSnapshotT ss;
    status = Snapshots::GetInstance().GetSnapshot(ss, c1);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(ss);
    ASSERT_EQ(ss->GetName(), c1);

    bool has;
    status = db_->HasCollection(c1, has);
    ASSERT_TRUE(has);
    ASSERT_TRUE(status.ok());

    std::vector<std::string> names;
    status = db_->AllCollections(names);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(names.size(), 1);
    ASSERT_EQ(names[0], c1);

    std::string c1_1 = "c1";
    status = CreateCollection(db_, c1_1, next_lsn());
    ASSERT_FALSE(status.ok());

    std::string c2 = "c2";
    status = CreateCollection(db_, c2, next_lsn());
    ASSERT_TRUE(status.ok());

    status = db_->AllCollections(names);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(names.size(), 2);

    status = db_->DropCollection(c1);
    ASSERT_TRUE(status.ok());

    status = db_->AllCollections(names);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(names.size(), 1);
    ASSERT_EQ(names[0], c2);

    status = db_->DropCollection(c1);
    ASSERT_FALSE(status.ok());
}

TEST_F(SSDBTest, PartitionTest) {
    LSN_TYPE lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn) {
        return ++lsn;
    };
    std::string c1 = "c1";
    auto status = CreateCollection(db_, c1, next_lsn());
    ASSERT_TRUE(status.ok());

    std::vector<std::string> partition_names;
    status = db_->ShowPartitions(c1, partition_names);
    ASSERT_EQ(partition_names.size(), 1);
    ASSERT_EQ(partition_names[0], "_default");

    std::string p1 = "p1";
    std::string c2 = "c2";
    status = db_->CreatePartition(c2, p1);
    ASSERT_FALSE(status.ok());

    status = db_->CreatePartition(c1, p1);
    ASSERT_TRUE(status.ok());

    status = db_->ShowPartitions(c1, partition_names);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(partition_names.size(), 2);

    status = db_->CreatePartition(c1, p1);
    ASSERT_FALSE(status.ok());

    status = db_->DropPartition(c1, "p3");
    ASSERT_FALSE(status.ok());

    status = db_->DropPartition(c1, p1);
    ASSERT_TRUE(status.ok());
    status = db_->ShowPartitions(c1, partition_names);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(partition_names.size(), 1);
}
