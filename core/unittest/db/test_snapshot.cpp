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

#include <random>
#include <string>
#include <set>

#include "db/utils.h"
#include "db/snapshot/ReferenceProxy.h"
#include "db/snapshot/ScopedResource.h"
#include "db/snapshot/WrappedTypes.h"
#include "db/snapshot/ResourceHolders.h"
#include "db/snapshot/OperationExecutor.h"
#include "db/snapshot/Store.h"
#include "db/snapshot/Context.h"
#include "db/snapshot/CompoundOperations.h"
#include "db/snapshot/Snapshots.h"


TEST_F(SnapshotTest, ReferenceProxyTest) {
    std::string status("raw");
    const std::string CALLED = "CALLED";
    auto callback = [&]() {
        status = CALLED;
    };

    auto proxy = milvus::engine::snapshot::ReferenceProxy();
    ASSERT_EQ(proxy.RefCnt(), 0);

    int refcnt = 3;
    for (auto i = 0; i < refcnt; ++i) {
        proxy.Ref();
    }
    ASSERT_EQ(proxy.RefCnt(), refcnt);

    proxy.RegisterOnNoRefCB(callback);

    for (auto i = 0; i < refcnt; ++i) {
        proxy.UnRef();
    }
    ASSERT_EQ(proxy.RefCnt(), 0);
    ASSERT_EQ(status, CALLED);
}

TEST_F(SnapshotTest, ScopedResourceTest) {
    auto inner = std::make_shared<milvus::engine::snapshot::Collection>("c1");
    ASSERT_EQ(inner->RefCnt(), 0);

    {
        auto not_scoped = milvus::engine::snapshot::CollectionScopedT(inner, false);
        ASSERT_EQ(not_scoped->RefCnt(), 0);
        not_scoped->Ref();
        ASSERT_EQ(not_scoped->RefCnt(), 1);
        ASSERT_EQ(inner->RefCnt(), 1);

        auto not_scoped_2 = not_scoped;
        ASSERT_EQ(not_scoped_2->RefCnt(), 1);
        ASSERT_EQ(not_scoped->RefCnt(), 1);
        ASSERT_EQ(inner->RefCnt(), 1);
    }
    ASSERT_EQ(inner->RefCnt(), 1);

    inner->UnRef();
    ASSERT_EQ(inner->RefCnt(), 0);

    {
        // Test scoped construct
        auto scoped = milvus::engine::snapshot::CollectionScopedT(inner);
        ASSERT_EQ(scoped->RefCnt(), 1);
        ASSERT_EQ(inner->RefCnt(), 1);

        {
            // Test bool operator
            decltype(scoped) other_scoped;
            ASSERT_EQ(other_scoped, false);
            // Test operator=
            other_scoped = scoped;
            ASSERT_EQ(other_scoped->RefCnt(), 2);
            ASSERT_EQ(scoped->RefCnt(), 2);
            ASSERT_EQ(inner->RefCnt(), 2);
        }
        ASSERT_EQ(scoped->RefCnt(), 1);
        ASSERT_EQ(inner->RefCnt(), 1);

        {
            // Test copy
            auto other_scoped(scoped);
            ASSERT_EQ(other_scoped->RefCnt(), 2);
            ASSERT_EQ(scoped->RefCnt(), 2);
            ASSERT_EQ(inner->RefCnt(), 2);
        }
        ASSERT_EQ(scoped->RefCnt(), 1);
        ASSERT_EQ(inner->RefCnt(), 1);
    }
    ASSERT_EQ(inner->RefCnt(), 0);
}

TEST_F(SnapshotTest, ResourceHoldersTest) {
    milvus::engine::snapshot::ID_TYPE collection_id = 1;
    auto collection = milvus::engine::snapshot::CollectionsHolder::GetInstance().GetResource(collection_id, false);
    auto prev_cnt = collection->RefCnt();
    {
        auto collection_2 = milvus::engine::snapshot::CollectionsHolder::GetInstance().GetResource(
                collection_id, false);
        ASSERT_EQ(collection->GetID(), collection_id);
        ASSERT_EQ(collection->RefCnt(), prev_cnt);
    }

    {
        auto collection = milvus::engine::snapshot::CollectionsHolder::GetInstance().GetResource(collection_id, true);
        ASSERT_EQ(collection->GetID(), collection_id);
        ASSERT_EQ(collection->RefCnt(), 1+prev_cnt);
    }

    if (prev_cnt == 0) {
        auto collection = milvus::engine::snapshot::CollectionsHolder::GetInstance().GetResource(collection_id, false);
        ASSERT_TRUE(!collection);
    }
}

milvus::engine::snapshot::ScopedSnapshotT
CreateCollection(const std::string& collection_name) {
    milvus::engine::snapshot::CreateCollectionContext context;
    auto collection_schema = std::make_shared<milvus::engine::snapshot::Collection>(collection_name);
    context.collection = collection_schema;
    auto vector_field = std::make_shared<milvus::engine::snapshot::Field>("vector", 0);
    auto vector_field_element = std::make_shared<milvus::engine::snapshot::FieldElement>(0, 0, "ivfsq8",
            milvus::engine::snapshot::FieldElementType::IVFSQ8);
    auto int_field = std::make_shared<milvus::engine::snapshot::Field>("int", 0);
    context.fields_schema[vector_field] = {vector_field_element};
    context.fields_schema[int_field] = {};

    auto op = std::make_shared<milvus::engine::snapshot::CreateCollectionOperation>(context);
    op->Push();
    milvus::engine::snapshot::ScopedSnapshotT ss;
    auto status = op->GetSnapshot(ss);
    return ss;
}

TEST_F(SnapshotTest, CreateCollectionOperationTest) {
    milvus::engine::snapshot::Store::GetInstance().DoReset();
    milvus::engine::snapshot::ScopedSnapshotT expect_null;
    auto status = milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(expect_null, 100000);
    ASSERT_TRUE(!expect_null);

    std::string collection_name = "test_c1";
    auto ss = CreateCollection(collection_name);
    ASSERT_TRUE(ss);

    milvus::engine::snapshot::ScopedSnapshotT latest_ss;
    status = milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(latest_ss, "xxxx");
    ASSERT_TRUE(!status.ok());

    status = milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
    ASSERT_TRUE(latest_ss);
    ASSERT_TRUE(latest_ss->GetName() == collection_name);

    milvus::engine::snapshot::IDS_TYPE ids;
    status = milvus::engine::snapshot::Snapshots::GetInstance().GetCollectionIds(ids);
    ASSERT_EQ(ids.size(), 1);
    ASSERT_EQ(ids[0], latest_ss->GetCollectionId());

    milvus::engine::snapshot::OperationContext sd_op_ctx;
    sd_op_ctx.collection = latest_ss->GetCollection();
    ASSERT_TRUE(sd_op_ctx.collection->IsActive());
    auto sd_op = std::make_shared<milvus::engine::snapshot::SoftDeleteCollectionOperation>(sd_op_ctx);
    status = sd_op->Push();
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(sd_op->GetStatus().ok());
    ASSERT_TRUE(!sd_op_ctx.collection->IsActive());
    ASSERT_TRUE(!latest_ss->GetCollection()->IsActive());

    milvus::engine::snapshot::Snapshots::GetInstance().Reset();
}

TEST_F(SnapshotTest, DropCollectionTest) {
    milvus::engine::snapshot::Store::GetInstance().DoReset();
    std::string collection_name = "test_c1";
    auto ss = CreateCollection(collection_name);
    ASSERT_TRUE(ss);
    milvus::engine::snapshot::ScopedSnapshotT lss;
    auto status = milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(lss, collection_name);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(lss);
    ASSERT_EQ(ss->GetID(), lss->GetID());
    auto prev_ss_id = ss->GetID();
    auto prev_c_id = ss->GetCollection()->GetID();
    status = milvus::engine::snapshot::Snapshots::GetInstance().DropCollection(collection_name);
    ASSERT_TRUE(status.ok());
    status = milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(lss, collection_name);
    ASSERT_TRUE(!status.ok());

    auto ss_2 = CreateCollection(collection_name);
    status = milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(lss, collection_name);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(ss_2->GetID(), lss->GetID());
    ASSERT_TRUE(prev_ss_id != ss_2->GetID());
    ASSERT_TRUE(prev_c_id != ss_2->GetCollection()->GetID());
    status = milvus::engine::snapshot::Snapshots::GetInstance().DropCollection(collection_name);
    ASSERT_TRUE(status.ok());
    status = milvus::engine::snapshot::Snapshots::GetInstance().DropCollection(collection_name);
    ASSERT_TRUE(!status.ok());
}

TEST_F(SnapshotTest, ConCurrentCollectionOperation) {
    milvus::engine::snapshot::Store::GetInstance().DoReset();
    std::string collection_name("c1");

    milvus::engine::snapshot::ID_TYPE stale_ss_id;
    auto worker1 = [&]() {
        milvus::Status status;
        auto ss = CreateCollection(collection_name);
        ASSERT_TRUE(ss);
        ASSERT_EQ(ss->GetName(), collection_name);
        stale_ss_id = ss->GetID();
        decltype(ss) a_ss;
        status = milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(a_ss, collection_name);
        ASSERT_TRUE(status.ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        ASSERT_TRUE(!ss->GetCollection()->IsActive());
        status = milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(a_ss, collection_name);
        ASSERT_TRUE(!status.ok());

        auto c_c = milvus::engine::snapshot::CollectionCommitsHolder::GetInstance().GetResource(stale_ss_id, false);
        ASSERT_TRUE(c_c);
        ASSERT_EQ(c_c->GetID(), stale_ss_id);
    };
    auto worker2 = [&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        auto status = milvus::engine::snapshot::Snapshots::GetInstance().DropCollection(collection_name);
        ASSERT_TRUE(status.ok());
        milvus::engine::snapshot::ScopedSnapshotT a_ss;
        status = milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(a_ss, collection_name);
        ASSERT_TRUE(!status.ok());
    };
    auto worker3 = [&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        auto ss = CreateCollection(collection_name);
        ASSERT_TRUE(!ss);
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        ss = CreateCollection(collection_name);
        ASSERT_TRUE(ss);
        ASSERT_EQ(ss->GetName(), collection_name);
    };
    std::thread t1 = std::thread(worker1);
    std::thread t2 = std::thread(worker2);
    std::thread t3 = std::thread(worker3);
    t1.join();
    t2.join();
    t3.join();

    auto c_c = milvus::engine::snapshot::CollectionCommitsHolder::GetInstance().GetResource(stale_ss_id, false);
    ASSERT_TRUE(!c_c);
}

TEST_F(SnapshotTest, PartitionTest) {
    milvus::engine::snapshot::Store::GetInstance().DoReset();
    std::string collection_name("c1");
    auto ss = CreateCollection(collection_name);
    ASSERT_TRUE(ss);
    ASSERT_EQ(ss->GetName(), collection_name);
    ASSERT_EQ(ss->NumberOfPartitions(), 1);

    milvus::engine::snapshot::OperationContext context;
    auto op = std::make_shared<milvus::engine::snapshot::CreatePartitionOperation>(context, ss);

    std::string partition_name("p1");
    milvus::engine::snapshot::PartitionContext p_ctx;
    p_ctx.name = partition_name;
    milvus::engine::snapshot::PartitionPtr partition;
    auto status = op->CommitNewPartition(p_ctx, partition);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(partition);
    ASSERT_EQ(partition->GetName(), partition_name);
    ASSERT_TRUE(!partition->IsActive());
    ASSERT_TRUE(partition->HasAssigned());

    status = op->Push();
    ASSERT_TRUE(status.ok());
    decltype(ss) curr_ss;
    status = op->GetSnapshot(curr_ss);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(curr_ss);
    ASSERT_EQ(curr_ss->GetName(), ss->GetName());
    ASSERT_TRUE(curr_ss->GetID() > ss->GetID());
    ASSERT_EQ(curr_ss->NumberOfPartitions(), 2);

    auto drop_op = std::make_shared<milvus::engine::snapshot::DropPartitionOperation>(p_ctx, curr_ss);
    status = drop_op->Push();
    ASSERT_TRUE(status.ok());

    decltype(ss) latest_ss;
    status = drop_op->GetSnapshot(latest_ss);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(latest_ss);
    ASSERT_EQ(latest_ss->GetName(), ss->GetName());
    ASSERT_TRUE(latest_ss->GetID() > curr_ss->GetID());
    ASSERT_EQ(latest_ss->NumberOfPartitions(), 1);

    drop_op = std::make_shared<milvus::engine::snapshot::DropPartitionOperation>(p_ctx, latest_ss);
    status = drop_op->Push();
    ASSERT_TRUE(!status.ok());
    std::cout << status.ToString() << std::endl;
}

TEST_F(SnapshotTest, OperationTest) {
    milvus::Status status;
    std::string to_string;
    milvus::engine::snapshot::SegmentFileContext sf_context;
    sf_context.field_name = "f_1_1";
    sf_context.field_element_name = "fe_1_1";
    sf_context.segment_id = 1;
    sf_context.partition_id = 1;

    milvus::engine::snapshot::ScopedSnapshotT ss;
    status = milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(ss, 1);
    auto ss_id = ss->GetID();
    ASSERT_TRUE(status.ok());

    // Check snapshot
    {
        auto collection_commit = milvus::engine::snapshot::CollectionCommitsHolder::GetInstance()
            .GetResource(ss_id, false);
        /* snapshot::SegmentCommitsHolder::GetInstance().GetResource(prev_segment_commit->GetID()); */
        ASSERT_TRUE(collection_commit);
        to_string = collection_commit->ToString();
        ASSERT_EQ(to_string, "");
    }

    milvus::engine::snapshot::OperationContext merge_ctx;
    std::set<milvus::engine::snapshot::ID_TYPE> stale_segment_commit_ids;

    decltype(sf_context.segment_id) new_seg_id;
    decltype(ss) new_ss;
    // Check build operation correctness
    {
        milvus::engine::snapshot::OperationContext context;
        auto build_op = std::make_shared<milvus::engine::snapshot::BuildOperation>(context, ss);
        milvus::engine::snapshot::SegmentFilePtr seg_file;
        status = build_op->CommitNewSegmentFile(sf_context, seg_file);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(seg_file);
        auto prev_segment_commit = ss->GetSegmentCommit(seg_file->GetSegmentId());
        auto prev_segment_commit_mappings = prev_segment_commit->GetMappings();
        ASSERT_NE(prev_segment_commit->ToString(), "");

        build_op->Push();
        status = build_op->GetSnapshot(ss);
        ASSERT_TRUE(ss->GetID() > ss_id);

        auto segment_commit = ss->GetSegmentCommit(seg_file->GetSegmentId());
        auto segment_commit_mappings = segment_commit->GetMappings();
        milvus::engine::snapshot::MappingT expected_mappings = prev_segment_commit_mappings;
        expected_mappings.insert(seg_file->GetID());
        ASSERT_EQ(expected_mappings, segment_commit_mappings);

        auto seg = ss->GetResource<milvus::engine::snapshot::Segment>(seg_file->GetSegmentId());
        ASSERT_TRUE(seg);
        merge_ctx.stale_segments.push_back(seg);
        stale_segment_commit_ids.insert(segment_commit->GetID());
    }

    // Check stale snapshot has been deleted from store
    {
        auto collection_commit = milvus::engine::snapshot::CollectionCommitsHolder::GetInstance()
            .GetResource(ss_id, false);
        ASSERT_TRUE(!collection_commit);
    }

    ss_id = ss->GetID();
    milvus::engine::snapshot::ID_TYPE partition_id;
    {
        milvus::engine::snapshot::OperationContext context;
        context.prev_partition = ss->GetResource<milvus::engine::snapshot::Partition>(1);
        auto op = std::make_shared<milvus::engine::snapshot::NewSegmentOperation>(context, ss);
        milvus::engine::snapshot::SegmentPtr new_seg;
        status = op->CommitNewSegment(new_seg);
        ASSERT_TRUE(status.ok());
        ASSERT_NE(new_seg->ToString(), "");
        milvus::engine::snapshot::SegmentFilePtr seg_file;
        status = op->CommitNewSegmentFile(sf_context, seg_file);
        ASSERT_TRUE(status.ok());
        status = op->Push();
        ASSERT_TRUE(status.ok());

        status = op->GetSnapshot(ss);
        ASSERT_TRUE(ss->GetID() > ss_id);
        ASSERT_TRUE(status.ok());

        auto segment_commit = ss->GetSegmentCommit(seg_file->GetSegmentId());
        auto segment_commit_mappings = segment_commit->GetMappings();
        milvus::engine::snapshot::MappingT expected_segment_mappings;
        expected_segment_mappings.insert(seg_file->GetID());
        ASSERT_EQ(expected_segment_mappings, segment_commit_mappings);
        merge_ctx.stale_segments.push_back(new_seg);
        partition_id = segment_commit->GetPartitionId();
        stale_segment_commit_ids.insert(segment_commit->GetID());
        auto partition = ss->GetResource<milvus::engine::snapshot::Partition>(partition_id);
        merge_ctx.prev_partition = partition;
        new_seg_id = seg_file->GetSegmentId();
        new_ss = ss;
    }

    milvus::engine::snapshot::SegmentPtr merge_seg;
    ss_id = ss->GetID();
    {
        auto prev_partition_commit = ss->GetPartitionCommitByPartitionId(partition_id);
        auto expect_null = ss->GetPartitionCommitByPartitionId(11111111);
        ASSERT_TRUE(!expect_null);
        ASSERT_NE(prev_partition_commit->ToString(), "");
        auto op = std::make_shared<milvus::engine::snapshot::MergeOperation>(merge_ctx, ss);
        milvus::engine::snapshot::SegmentPtr new_seg;
        status = op->CommitNewSegment(new_seg);
        sf_context.segment_id = new_seg->GetID();
        milvus::engine::snapshot::SegmentFilePtr seg_file;
        status = op->CommitNewSegmentFile(sf_context, seg_file);
        ASSERT_TRUE(status.ok());
        status = op->Push();
        ASSERT_TRUE(status.ok());
        std::cout << op->ToString() << std::endl;
        status = op->GetSnapshot(ss);
        ASSERT_TRUE(ss->GetID() > ss_id);
        ASSERT_TRUE(status.ok());

        auto segment_commit = ss->GetSegmentCommit(new_seg->GetID());
        auto new_partition_commit = ss->GetPartitionCommitByPartitionId(partition_id);
        auto new_mappings = new_partition_commit->GetMappings();
        auto prev_mappings = prev_partition_commit->GetMappings();
        auto expected_mappings = prev_mappings;
        for (auto id : stale_segment_commit_ids) {
            expected_mappings.erase(id);
        }
        expected_mappings.insert(segment_commit->GetID());
        ASSERT_EQ(expected_mappings, new_mappings);

        milvus::engine::snapshot::CollectionCommitsHolder::GetInstance().Dump();
        merge_seg = new_seg;
    }

    // 1. New seg1, seg2
    // 2. Build seg1 start
    // 3. Merge seg1, seg2 to seg3
    // 4. Commit new seg file of build operation -> Stale Segment Found Here!
    {
        milvus::engine::snapshot::OperationContext context;
        auto build_op = std::make_shared<milvus::engine::snapshot::BuildOperation>(context, new_ss);
        milvus::engine::snapshot::SegmentFilePtr seg_file;
        auto new_sf_context = sf_context;
        new_sf_context.segment_id = new_seg_id;
        status = build_op->CommitNewSegmentFile(new_sf_context, seg_file);
        ASSERT_TRUE(!status.ok());
    }

    // 1. Build start
    // 2. Commit new seg file of build operation
    // 3. Drop collection
    // 4. Commit build operation -> Stale Segment Found Here!
    {
        milvus::engine::snapshot::OperationContext context;
        auto build_op = std::make_shared<milvus::engine::snapshot::BuildOperation>(context, ss);
        milvus::engine::snapshot::SegmentFilePtr seg_file;
        auto new_sf_context = sf_context;
        new_sf_context.segment_id = merge_seg->GetID();
        status = build_op->CommitNewSegmentFile(new_sf_context, seg_file);
        ASSERT_TRUE(status.ok());
        std::cout << build_op->ToString() << std::endl;

        auto status = milvus::engine::snapshot::Snapshots::GetInstance().DropCollection(ss->GetName());
        ASSERT_TRUE(status.ok());
        status = build_op->Push();
        ASSERT_TRUE(!status.ok());
        ASSERT_TRUE(!(build_op->GetStatus()).ok());
        std::cout << build_op->ToString() << std::endl;
    }
    milvus::engine::snapshot::Snapshots::GetInstance().Reset();
}
