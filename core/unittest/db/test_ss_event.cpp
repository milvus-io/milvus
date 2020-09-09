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

#include <gtest/gtest.h>

#include "db/snapshot/InActiveResourcesGCEvent.h"
#include "db/utils.h"

using CollectionCommit = milvus::engine::snapshot::CollectionCommit;
using CollectionCommitPtr = milvus::engine::snapshot::CollectionCommitPtr;
using PartitionCommit = milvus::engine::snapshot::PartitionCommit;
using PartitionCommitPtr = milvus::engine::snapshot::PartitionCommitPtr;
using SegmentCommit = milvus::engine::snapshot::SegmentCommit;
using SegmentCommitPtr = milvus::engine::snapshot::SegmentCommitPtr;
using SchemaCommit = milvus::engine::snapshot::SchemaCommit;
using FieldCommit = milvus::engine::snapshot::FieldCommit;

using FType = milvus::engine::DataType;
using FEType = milvus::engine::FieldElementType;

using InActiveResourcesGCEvent = milvus::engine::snapshot::InActiveResourcesGCEvent;
template<typename T>
using ResourceGCEvent = milvus::engine::snapshot::ResourceGCEvent<T>;
using EventExecutor = milvus::engine::snapshot::EventExecutor;

TEST_F(EventTest, TestInActiveResGcEvent) {
    CollectionPtr collection;
    auto status = store_->CreateResource(Collection("test_gc_c1"), collection);
    ASSERT_TRUE(status.ok()) << status.ToString();

    CollectionPtr inactive_collection;
    auto c = Collection("test_gc_c2");
    c.Deactivate();
    status = store_->CreateResource(std::move(c), inactive_collection);
    ASSERT_TRUE(status.ok()) << status.ToString();

    CollectionPtr active_collection;
    auto c_2 = Collection("test_gc_c3");
    c_2.Activate();
    status = store_->CreateResource(std::move(c_2), active_collection);
    ASSERT_TRUE(status.ok()) << status.ToString();

    PartitionPtr partition;
    status = store_->CreateResource<Partition>(Partition("test_gc_c1_p1", collection->GetID()), partition);
    ASSERT_TRUE(status.ok()) << status.ToString();

    PartitionPtr inactive_partition;
    auto p = Partition("test_gc_c1_p2", collection->GetID());
    p.Deactivate();
    status = store_->CreateResource<Partition>(std::move(p), inactive_partition);
    ASSERT_TRUE(status.ok()) << status.ToString();

    PartitionCommitPtr partition_commit;
    status = store_->CreateResource<PartitionCommit>(PartitionCommit(collection->GetID(), partition->GetID()),
        partition_commit);
    ASSERT_TRUE(status.ok()) << status.ToString();

    CollectionCommitPtr collection_commit;
    status = store_->CreateResource<CollectionCommit>(CollectionCommit(0, 0), collection_commit);
    ASSERT_TRUE(status.ok()) << status.ToString();

    SegmentPtr s;
    status = store_->CreateResource<Segment>(Segment(collection->GetID(), partition->GetCollectionId()), s);
    ASSERT_TRUE(status.ok()) << status.ToString();

    Field::Ptr field;
    status = store_->CreateResource<Field>(Field("f_0", 0, FType::INT64), field);
    ASSERT_TRUE(status.ok()) << status.ToString();

    FieldElementPtr field_element;
    status = store_->CreateResource<FieldElement>(
        FieldElement(collection->GetID(), field->GetID(), "fe_0", FEType::FET_INDEX), field_element);
    ASSERT_TRUE(status.ok()) << status.ToString();

    FieldCommit::Ptr field_commit;
    status = store_->CreateResource<FieldCommit>(FieldCommit(collection->GetID(), field->GetID()), field_commit);
    ASSERT_TRUE(status.ok()) << status.ToString();

    SchemaCommit::Ptr schema;
    status = store_->CreateResource<SchemaCommit>(SchemaCommit(collection->GetID(), {}), schema);
    ASSERT_TRUE(status.ok()) << status.ToString();

    SegmentFilePtr seg_file;
    status = store_->CreateResource<SegmentFile>(
        SegmentFile(collection->GetID(), partition->GetID(), s->GetID(), field_element->GetID(), field_element->GetFEtype()), seg_file);
    ASSERT_TRUE(status.ok()) << status.ToString();

    SegmentCommitPtr sc;
    status = store_->CreateResource<SegmentCommit>(SegmentCommit(schema->GetID(), partition->GetID(), s->GetID()), sc);
    ASSERT_TRUE(status.ok()) << status.ToString();

    CollectionCommitPtr inactive_collection_commit;
    auto cc = CollectionCommit(collection->GetID(), schema->GetID());
    cc.Deactivate();
    status = store_->CreateResource<CollectionCommit>(std::move(cc), inactive_collection_commit);
    ASSERT_TRUE(status.ok()) << status.ToString();

    // TODO(yhz): Check if disk file has been deleted

    auto event = std::make_shared<InActiveResourcesGCEvent>();
    status = event->Process(store_);
    ASSERT_TRUE(status.ok()) << status.ToString();

    std::vector<FieldElementPtr> field_elements;
    ASSERT_TRUE(store_->GetInActiveResources<FieldElement>(field_elements).ok());
    ASSERT_TRUE(field_elements.empty());

    std::vector<Field::Ptr> fields;
    ASSERT_TRUE(store_->GetInActiveResources<Field>(fields).ok());
    ASSERT_TRUE(fields.empty());

    std::vector<FieldCommit::Ptr> field_commits;
    ASSERT_TRUE(store_->GetInActiveResources<FieldCommit>(field_commits).ok());
    ASSERT_TRUE(field_commits.empty());

    std::vector<SegmentFilePtr> seg_files;
    ASSERT_TRUE(store_->GetInActiveResources<SegmentFile>(seg_files).ok());
    ASSERT_TRUE(seg_files.empty());

    std::vector<SegmentCommitPtr> seg_commits;
    ASSERT_TRUE(store_->GetInActiveResources<SegmentCommit>(seg_commits).ok());
    ASSERT_TRUE(seg_commits.empty());

    std::vector<SegmentPtr> segs;
    ASSERT_TRUE(store_->GetInActiveResources<Segment>(segs).ok());
    ASSERT_TRUE(segs.empty());

    std::vector<SchemaCommit::Ptr> schemas;
    ASSERT_TRUE(store_->GetInActiveResources<SchemaCommit>(schemas).ok());
    ASSERT_TRUE(schemas.empty());

    std::vector<PartitionCommitPtr> partition_commits;
    ASSERT_TRUE(store_->GetInActiveResources<PartitionCommit>(partition_commits).ok());
    ASSERT_TRUE(partition_commits.empty());

    std::vector<PartitionPtr> partitions;
    ASSERT_TRUE(store_->GetInActiveResources<Partition>(partitions).ok());
    ASSERT_TRUE(partitions.empty());

    std::vector<CollectionPtr> collections;
    ASSERT_TRUE(store_->GetInActiveResources<Collection>(collections).ok());
    ASSERT_TRUE(collections.empty());

    std::vector<CollectionCommitPtr> collection_commits;
    ASSERT_TRUE(store_->GetInActiveResources<CollectionCommit>(collection_commits).ok());
    ASSERT_TRUE(collection_commits.empty());
}

TEST_F(EventTest, GcTest) {
    std::string collection_name = "event_test_gc_test";

    CollectionPtr collection;
    auto cc = Collection(collection_name);
    cc.Activate();
    auto status = store_->CreateResource(std::move(cc), collection);
    ASSERT_TRUE(status.ok()) << status.ToString();

    CollectionPtr collection2;
    status = store_->GetResource<Collection>(collection->GetID(), collection2);
    ASSERT_TRUE(status.ok()) << status.ToString();

    auto event = std::make_shared<ResourceGCEvent<Collection>>(collection);
    status = event->Process(store_);
    ASSERT_TRUE(status.ok()) << status.ToString();

    CollectionPtr rcollection;
    status = store_->GetResource<Collection>(collection->GetID(),rcollection);
    ASSERT_FALSE(status.ok());
}

TEST_F(EventTest, GcBlockingTest) {
    size_t max_count = 1000;

    std::vector<CollectionPtr> collections;
    for (size_t i = 0; i < max_count; i++) {
        CollectionPtr collection;
        std::string name = "test_gc_c_" + std::to_string(i);
        auto status = store_->CreateResource(Collection(name), collection);
        ASSERT_TRUE(status.ok()) << status.ToString();
        collections.push_back(collection);
    }

    auto start = std::chrono::system_clock::now();
    for (auto& collection : collections) {
        auto gc_event = std::make_shared<ResourceGCEvent<Collection>>(collection);
        EventExecutor::GetInstance().Submit(gc_event);
    }
    auto end = std::chrono::system_clock::now();
    auto count = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    ASSERT_LT(count, 1000);
}
