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
#include <algorithm>

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

using ID_TYPE = milvus::engine::snapshot::ID_TYPE;
using IDS_TYPE = milvus::engine::snapshot::IDS_TYPE;
using LSN_TYPE = milvus::engine::snapshot::LSN_TYPE;
using MappingT = milvus::engine::snapshot::MappingT;
using CreateCollectionContext = milvus::engine::snapshot::CreateCollectionContext;
using SegmentFileContext = milvus::engine::snapshot::SegmentFileContext;
using OperationContext =  milvus::engine::snapshot::OperationContext;
using PartitionContext =  milvus::engine::snapshot::PartitionContext;
using BuildOperation =  milvus::engine::snapshot::BuildOperation;
using MergeOperation =  milvus::engine::snapshot::MergeOperation;
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
using Queue = milvus::server::BlockingQueue<ID_TYPE>;
using TQueue = milvus::server::BlockingQueue<std::tuple<ID_TYPE, ID_TYPE>>;

int RandomInt(int start, int end) {
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(start, end);
    return dist(rng);
}

TEST_F(SnapshotTest, ReferenceProxyTest) {
    std::string status("raw");
    const std::string CALLED = "CALLED";
    auto callback = [&]() {
        status = CALLED;
    };

    auto proxy = ReferenceProxy();
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
    auto inner = std::make_shared<Collection>("c1");
    ASSERT_EQ(inner->RefCnt(), 0);

    {
        auto not_scoped = CollectionScopedT(inner, false);
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
        auto scoped = CollectionScopedT(inner);
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
    ID_TYPE collection_id = 1;
    auto collection = CollectionsHolder::GetInstance().GetResource(collection_id, false);
    auto prev_cnt = collection->RefCnt();
    {
        auto collection_2 = CollectionsHolder::GetInstance().GetResource(
                collection_id, false);
        ASSERT_EQ(collection->GetID(), collection_id);
        ASSERT_EQ(collection->RefCnt(), prev_cnt);
    }

    {
        auto collection = CollectionsHolder::GetInstance().GetResource(collection_id, true);
        ASSERT_EQ(collection->GetID(), collection_id);
        ASSERT_EQ(collection->RefCnt(), 1+prev_cnt);
    }

    if (prev_cnt == 0) {
        auto collection = CollectionsHolder::GetInstance().GetResource(collection_id, false);
        ASSERT_TRUE(!collection);
    }
}

ScopedSnapshotT
CreateCollection(const std::string& collection_name, const LSN_TYPE& lsn) {
    CreateCollectionContext context;
    context.lsn = lsn;
    auto collection_schema = std::make_shared<Collection>(collection_name);
    context.collection = collection_schema;
    auto vector_field = std::make_shared<Field>("vector", 0);
    auto vector_field_element = std::make_shared<FieldElement>(0, 0, "ivfsq8",
            milvus::engine::snapshot::FieldElementType::IVFSQ8);
    auto int_field = std::make_shared<Field>("int", 0);
    context.fields_schema[vector_field] = {vector_field_element};
    context.fields_schema[int_field] = {};

    auto op = std::make_shared<CreateCollectionOperation>(context);
    op->Push();
    ScopedSnapshotT ss;
    auto status = op->GetSnapshot(ss);
    return ss;
}

TEST_F(SnapshotTest, CreateCollectionOperationTest) {
    ScopedSnapshotT expect_null;
    auto status = Snapshots::GetInstance().GetSnapshot(expect_null, 100000);
    ASSERT_TRUE(!expect_null);

    std::string collection_name = "test_c1";
    LSN_TYPE lsn = 1;
    auto ss = CreateCollection(collection_name, lsn);
    ASSERT_TRUE(ss);

    ScopedSnapshotT latest_ss;
    status = Snapshots::GetInstance().GetSnapshot(latest_ss, "xxxx");
    ASSERT_TRUE(!status.ok());

    status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
    ASSERT_TRUE(latest_ss);
    ASSERT_TRUE(latest_ss->GetName() == collection_name);

    IDS_TYPE ids;
    status = Snapshots::GetInstance().GetCollectionIds(ids);
    ASSERT_EQ(ids.size(), 6);
    ASSERT_EQ(ids[5], latest_ss->GetCollectionId());

    OperationContext sd_op_ctx;
    sd_op_ctx.collection = latest_ss->GetCollection();
    sd_op_ctx.lsn = latest_ss->GetMaxLsn() + 1;
    ASSERT_TRUE(sd_op_ctx.collection->IsActive());
    auto sd_op = std::make_shared<DropCollectionOperation>(sd_op_ctx, latest_ss);
    status = sd_op->Push();
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(sd_op->GetStatus().ok());
    ASSERT_TRUE(!sd_op_ctx.collection->IsActive());
    ASSERT_TRUE(!latest_ss->GetCollection()->IsActive());

    Snapshots::GetInstance().Reset();
}

TEST_F(SnapshotTest, DropCollectionTest) {
    std::string collection_name = "test_c1";
    LSN_TYPE lsn = 1;
    auto ss = CreateCollection(collection_name, lsn);
    ASSERT_TRUE(ss);
    ScopedSnapshotT lss;
    auto status = Snapshots::GetInstance().GetSnapshot(lss, collection_name);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(lss);
    ASSERT_EQ(ss->GetID(), lss->GetID());
    auto prev_ss_id = ss->GetID();
    auto prev_c_id = ss->GetCollection()->GetID();
    lsn = ss->GetMaxLsn() + 1;
    status = Snapshots::GetInstance().DropCollection(collection_name, lsn);
    ASSERT_TRUE(status.ok());
    status = Snapshots::GetInstance().GetSnapshot(lss, collection_name);
    ASSERT_TRUE(!status.ok());

    auto ss_2 = CreateCollection(collection_name, ++lsn);
    status = Snapshots::GetInstance().GetSnapshot(lss, collection_name);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(ss_2->GetID(), lss->GetID());
    ASSERT_TRUE(prev_ss_id != ss_2->GetID());
    ASSERT_TRUE(prev_c_id != ss_2->GetCollection()->GetID());
    status = Snapshots::GetInstance().DropCollection(collection_name, ++lsn);
    ASSERT_TRUE(status.ok());
    status = Snapshots::GetInstance().DropCollection(collection_name, ++lsn);
    ASSERT_TRUE(!status.ok());
}

TEST_F(SnapshotTest, ConCurrentCollectionOperation) {
    std::string collection_name("c1");
    LSN_TYPE lsn = 1;

    ID_TYPE stale_ss_id;
    auto worker1 = [&]() {
        milvus::Status status;
        auto ss = CreateCollection(collection_name, ++lsn);
        ASSERT_TRUE(ss);
        ASSERT_EQ(ss->GetName(), collection_name);
        stale_ss_id = ss->GetID();
        decltype(ss) a_ss;
        status = Snapshots::GetInstance().GetSnapshot(a_ss, collection_name);
        ASSERT_TRUE(status.ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        ASSERT_TRUE(!ss->GetCollection()->IsActive());
        status = Snapshots::GetInstance().GetSnapshot(a_ss, collection_name);
        ASSERT_TRUE(!status.ok());

        auto c_c = CollectionCommitsHolder::GetInstance().GetResource(stale_ss_id, false);
        ASSERT_TRUE(c_c);
        ASSERT_EQ(c_c->GetID(), stale_ss_id);
    };
    auto worker2 = [&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        auto status = Snapshots::GetInstance().DropCollection(collection_name, ++lsn);
        ASSERT_TRUE(status.ok());
        ScopedSnapshotT a_ss;
        status = Snapshots::GetInstance().GetSnapshot(a_ss, collection_name);
        ASSERT_TRUE(!status.ok());
    };
    auto worker3 = [&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        auto ss = CreateCollection(collection_name, ++lsn);
        ASSERT_TRUE(!ss);
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        ss = CreateCollection(collection_name, ++lsn);
        ASSERT_TRUE(ss);
        ASSERT_EQ(ss->GetName(), collection_name);
    };
    std::thread t1 = std::thread(worker1);
    std::thread t2 = std::thread(worker2);
    std::thread t3 = std::thread(worker3);
    t1.join();
    t2.join();
    t3.join();

    auto c_c = CollectionCommitsHolder::GetInstance().GetResource(stale_ss_id, false);
    ASSERT_TRUE(!c_c);
}

ScopedSnapshotT
CreatePartition(const std::string& collection_name, const PartitionContext& p_context,
        const LSN_TYPE& lsn) {
    ScopedSnapshotT curr_ss;
    ScopedSnapshotT ss;
    auto status = Snapshots::GetInstance().GetSnapshot(ss, collection_name);
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
        return curr_ss;
    }

    OperationContext context;
    context.lsn = lsn;
    auto op = std::make_shared<CreatePartitionOperation>(context, ss);

    PartitionPtr partition;
    status = op->CommitNewPartition(p_context, partition);
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
        return curr_ss;
    }

    status = op->Push();
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
        return curr_ss;
    }

    status = op->GetSnapshot(curr_ss);
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
        return curr_ss;
    }
    return curr_ss;
}

TEST_F(SnapshotTest, PartitionTest) {
    std::string collection_name("c1");
    LSN_TYPE lsn = 1;
    auto ss = CreateCollection(collection_name, ++lsn);
    ASSERT_TRUE(ss);
    ASSERT_EQ(ss->GetName(), collection_name);
    ASSERT_EQ(ss->NumberOfPartitions(), 1);

    OperationContext context;
    context.lsn = ++lsn;
    auto op = std::make_shared<CreatePartitionOperation>(context, ss);

    std::string partition_name("p1");
    PartitionContext p_ctx;
    p_ctx.name = partition_name;
    PartitionPtr partition;
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

    p_ctx.lsn = ++lsn;
    auto drop_op = std::make_shared<DropPartitionOperation>(p_ctx, curr_ss);
    status = drop_op->Push();
    ASSERT_TRUE(status.ok());

    decltype(ss) latest_ss;
    status = drop_op->GetSnapshot(latest_ss);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(latest_ss);
    ASSERT_EQ(latest_ss->GetName(), ss->GetName());
    ASSERT_TRUE(latest_ss->GetID() > curr_ss->GetID());
    ASSERT_EQ(latest_ss->NumberOfPartitions(), 1);

    p_ctx.lsn = ++lsn;
    drop_op = std::make_shared<DropPartitionOperation>(p_ctx, latest_ss);
    status = drop_op->Push();
    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(!status.ok());

    // TODO: Keep LSN in order
    PartitionContext pp_ctx;
    /* pp_ctx.name = "p2"; */
    /* curr_ss = CreatePartition(collection_name, pp_ctx, lsn-1); */
    /* ASSERT_TRUE(curr_ss); */

    std::stringstream p_name_stream;

    auto num = RandomInt(20, 30);
    for (auto i = 0; i < num; ++i) {
        p_name_stream.str("");
        p_name_stream << "partition_" << i;
        pp_ctx.name = p_name_stream.str();
        curr_ss = CreatePartition(collection_name, pp_ctx, ++lsn);
        ASSERT_TRUE(curr_ss);
        ASSERT_EQ(curr_ss->NumberOfPartitions(), 2 + i);
    }

    auto total_partition_num = curr_ss->NumberOfPartitions();

    ID_TYPE partition_id;
    for (auto i = 0; i < num; ++i) {
        p_name_stream.str("");
        p_name_stream << "partition_" << i;

        status = curr_ss->GetPartitionId(p_name_stream.str(), partition_id);
        ASSERT_TRUE(status.ok());
        status = Snapshots::GetInstance().DropPartition(
                curr_ss->GetCollectionId(), partition_id, ++lsn);
        ASSERT_TRUE(status.ok());
        status = Snapshots::GetInstance().GetSnapshot(
                curr_ss, curr_ss->GetCollectionId());
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(curr_ss->NumberOfPartitions(), total_partition_num - i -1);
    }
}

// TODO: Open this test later
/* TEST_F(SnapshotTest, PartitionTest2) { */
/*     std::string collection_name("c1"); */
/*     LSN_TYPE lsn = 1; */
/*     milvus::Status status; */

/*     auto ss = CreateCollection(collection_name, ++lsn); */
/*     ASSERT_TRUE(ss); */
/*     ASSERT_EQ(lsn, ss->GetMaxLsn()); */

/*     OperationContext context; */
/*     context.lsn = lsn; */
/*     auto cp_op = std::make_shared<CreatePartitionOperation>(context, ss); */
/*     std::string partition_name("p1"); */
/*     PartitionContext p_ctx; */
/*     p_ctx.name = partition_name; */
/*     PartitionPtr partition; */
/*     status = cp_op->CommitNewPartition(p_ctx, partition); */
/*     ASSERT_TRUE(status.ok()); */
/*     ASSERT_TRUE(partition); */
/*     ASSERT_EQ(partition->GetName(), partition_name); */
/*     ASSERT_TRUE(!partition->IsActive()); */
/*     ASSERT_TRUE(partition->HasAssigned()); */

/*     status = cp_op->Push(); */
/*     ASSERT_TRUE(!status.ok()); */
/* } */

TEST_F(SnapshotTest, OperationTest) {
    milvus::Status status;
    std::string to_string;
    LSN_TYPE lsn;
    SegmentFileContext sf_context;
    sf_context.field_name = "f_1_1";
    sf_context.field_element_name = "fe_1_1";
    sf_context.segment_id = 1;
    sf_context.partition_id = 1;

    ScopedSnapshotT ss;
    status = Snapshots::GetInstance().GetSnapshot(ss, 1);
    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
    auto ss_id = ss->GetID();
    lsn = ss->GetMaxLsn() + 1;
    ASSERT_TRUE(status.ok());

    // Check snapshot
    {
        auto collection_commit = CollectionCommitsHolder::GetInstance()
            .GetResource(ss_id, false);
        /* snapshot::SegmentCommitsHolder::GetInstance().GetResource(prev_segment_commit->GetID()); */
        ASSERT_TRUE(collection_commit);
        to_string = collection_commit->ToString();
        ASSERT_EQ(to_string, "");
    }

    OperationContext merge_ctx;
    std::set<ID_TYPE> stale_segment_commit_ids;

    decltype(sf_context.segment_id) new_seg_id;
    decltype(ss) new_ss;
    // Check build operation correctness
    {
        OperationContext context;
        context.lsn = ++lsn;
        auto build_op = std::make_shared<BuildOperation>(context, ss);
        SegmentFilePtr seg_file;
        status = build_op->CommitNewSegmentFile(sf_context, seg_file);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(seg_file);
        auto prev_segment_commit = ss->GetSegmentCommitBySegmentId(seg_file->GetSegmentId());
        auto prev_segment_commit_mappings = prev_segment_commit->GetMappings();
        ASSERT_NE(prev_segment_commit->ToString(), "");

        build_op->Push();
        status = build_op->GetSnapshot(ss);
        ASSERT_TRUE(ss->GetID() > ss_id);

        auto segment_commit = ss->GetSegmentCommitBySegmentId(seg_file->GetSegmentId());
        auto segment_commit_mappings = segment_commit->GetMappings();
        MappingT expected_mappings = prev_segment_commit_mappings;
        expected_mappings.insert(seg_file->GetID());
        ASSERT_EQ(expected_mappings, segment_commit_mappings);

        auto seg = ss->GetResource<Segment>(seg_file->GetSegmentId());
        ASSERT_TRUE(seg);
        merge_ctx.stale_segments.push_back(seg);
        stale_segment_commit_ids.insert(segment_commit->GetID());
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    // Check stale snapshot has been deleted from store
    {
        auto collection_commit = CollectionCommitsHolder::GetInstance()
            .GetResource(ss_id, false);
        ASSERT_TRUE(!collection_commit);
    }

    ss_id = ss->GetID();
    ID_TYPE partition_id;
    {
        OperationContext context;
        context.lsn = ++lsn;
        context.prev_partition = ss->GetResource<Partition>(1);
        auto op = std::make_shared<NewSegmentOperation>(context, ss);
        SegmentPtr new_seg;
        status = op->CommitNewSegment(new_seg);
        ASSERT_TRUE(status.ok());
        ASSERT_NE(new_seg->ToString(), "");
        SegmentFilePtr seg_file;
        status = op->CommitNewSegmentFile(sf_context, seg_file);
        ASSERT_TRUE(status.ok());
        status = op->Push();
        ASSERT_TRUE(status.ok());

        status = op->GetSnapshot(ss);
        ASSERT_TRUE(ss->GetID() > ss_id);
        ASSERT_TRUE(status.ok());

        auto segment_commit = ss->GetSegmentCommitBySegmentId(seg_file->GetSegmentId());
        auto segment_commit_mappings = segment_commit->GetMappings();
        MappingT expected_segment_mappings;
        expected_segment_mappings.insert(seg_file->GetID());
        ASSERT_EQ(expected_segment_mappings, segment_commit_mappings);
        merge_ctx.stale_segments.push_back(new_seg);
        partition_id = segment_commit->GetPartitionId();
        stale_segment_commit_ids.insert(segment_commit->GetID());
        auto partition = ss->GetResource<Partition>(partition_id);
        merge_ctx.prev_partition = partition;
        new_seg_id = seg_file->GetSegmentId();
        new_ss = ss;
    }

    SegmentPtr merge_seg;
    ss_id = ss->GetID();
    {
        auto prev_partition_commit = ss->GetPartitionCommitByPartitionId(partition_id);
        auto expect_null = ss->GetPartitionCommitByPartitionId(11111111);
        ASSERT_TRUE(!expect_null);
        ASSERT_NE(prev_partition_commit->ToString(), "");
        merge_ctx.lsn = ++lsn;
        auto op = std::make_shared<MergeOperation>(merge_ctx, ss);
        SegmentPtr new_seg;
        status = op->CommitNewSegment(new_seg);
        sf_context.segment_id = new_seg->GetID();
        SegmentFilePtr seg_file;
        status = op->CommitNewSegmentFile(sf_context, seg_file);
        ASSERT_TRUE(status.ok());
        status = op->Push();
        ASSERT_TRUE(status.ok());
        std::cout << op->ToString() << std::endl;
        status = op->GetSnapshot(ss);
        ASSERT_TRUE(ss->GetID() > ss_id);
        ASSERT_TRUE(status.ok());

        auto segment_commit = ss->GetSegmentCommitBySegmentId(new_seg->GetID());
        auto new_partition_commit = ss->GetPartitionCommitByPartitionId(partition_id);
        auto new_mappings = new_partition_commit->GetMappings();
        auto prev_mappings = prev_partition_commit->GetMappings();
        auto expected_mappings = prev_mappings;
        for (auto id : stale_segment_commit_ids) {
            expected_mappings.erase(id);
        }
        expected_mappings.insert(segment_commit->GetID());
        ASSERT_EQ(expected_mappings, new_mappings);

        CollectionCommitsHolder::GetInstance().Dump();
        merge_seg = new_seg;
    }

    // 1. New seg1, seg2
    // 2. Build seg1 start
    // 3. Merge seg1, seg2 to seg3
    // 4. Commit new seg file of build operation -> Stale Segment Found Here!
    {
        OperationContext context;
        context.lsn = ++lsn;
        auto build_op = std::make_shared<BuildOperation>(context, new_ss);
        SegmentFilePtr seg_file;
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
        OperationContext context;
        context.lsn = ++lsn;
        auto build_op = std::make_shared<BuildOperation>(context, ss);
        SegmentFilePtr seg_file;
        auto new_sf_context = sf_context;
        new_sf_context.segment_id = merge_seg->GetID();
        status = build_op->CommitNewSegmentFile(new_sf_context, seg_file);
        ASSERT_TRUE(status.ok());
        std::cout << build_op->ToString() << std::endl;

        auto status = Snapshots::GetInstance().DropCollection(ss->GetName(),
                ++lsn);
        ASSERT_TRUE(status.ok());
        status = build_op->Push();
        std::cout << status.ToString() << std::endl;
        ASSERT_TRUE(!status.ok());
        ASSERT_TRUE(!(build_op->GetStatus()).ok());
        std::cout << build_op->ToString() << std::endl;
    }
    Snapshots::GetInstance().Reset();
}

struct WaitableObj {
    bool notified_ = false;
    std::mutex mutex_;
    std::condition_variable cv_;

    void
    Wait() {
        std::unique_lock<std::mutex> lck(mutex_);
        if (!notified_) {
            cv_.wait(lck);
        }
        notified_ = false;
    }

    void
    Notify() {
        std::unique_lock<std::mutex> lck(mutex_);
        notified_ = true;
        lck.unlock();
        cv_.notify_one();
    }
};


TEST_F(SnapshotTest, CompoundTest1) {
    milvus::Status status;
    std::atomic<LSN_TYPE> lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn) {
        return ++lsn;
    };
    LSN_TYPE pid = 0;
    auto next_pid = [&]() -> decltype(pid) {
        return ++pid;
    };
    std::string collection_name("c1");
    auto ss = CreateCollection(collection_name, next_lsn());
    ASSERT_TRUE(ss);
    ASSERT_EQ(lsn, ss->GetMaxLsn());

    Queue merge_queue;
    Queue build_queue;

    std::set<ID_TYPE> all_segments;
    std::set<ID_TYPE> segment_in_building;
    std::map<ID_TYPE, std::set<ID_TYPE>> merged_segs_history;
    std::set<ID_TYPE> merged_segs;
    std::set<ID_TYPE> built_segs;
    std::set<ID_TYPE> build_stale_segs;

    std::mutex all_mtx;
    std::mutex merge_mtx;
    std::mutex built_mtx;
    std::mutex partition_mtx;

    WaitableObj merge_waiter;
    WaitableObj build_waiter;

    SegmentFileContext sf_context;
    sf_context.field_name = "vector";
    sf_context.field_element_name = "ivfsq8";
    sf_context.segment_id = 1;
    sf_context.partition_id = 1;

    IDS_TYPE partitions = {ss->GetResources<Partition>().begin()->second->GetID()};

    auto do_build = [&] (const ID_TYPE& seg_id) {
        decltype(ss) latest_ss;
        auto status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
        ASSERT_TRUE(status.ok());

        auto build_sf_context = sf_context;

        OperationContext context;
        context.lsn = next_lsn();
        auto build_op = std::make_shared<BuildOperation>(context, latest_ss);
        SegmentFilePtr seg_file;
        build_sf_context.segment_id = seg_id;
        status = build_op->CommitNewSegmentFile(build_sf_context, seg_file);
        if (!status.ok()) {
            std::cout << status.ToString() << std::endl;
            std::unique_lock<std::mutex> lock(merge_mtx);
            auto it = merged_segs.find(seg_id);
            ASSERT_NE(it, merged_segs.end());
            return;
        }
        std::unique_lock<std::mutex> lock(built_mtx);
        status = build_op->Push();
        if (!status.ok()) {
            std::cout << status.ToString() << std::endl;
            std::unique_lock<std::mutex> lock(merge_mtx);
            auto it = merged_segs.find(seg_id);
            ASSERT_NE(it, merged_segs.end());
            return;
        }
        ASSERT_TRUE(status.ok());
        status = build_op->GetSnapshot(latest_ss);
        ASSERT_TRUE(status.ok());

        built_segs.insert(seg_id);
    };

    auto do_merge = [&] (std::set<ID_TYPE>& seg_ids, ID_TYPE& new_seg_id) {
        if (seg_ids.size() == 0) {
            return;
        }
        decltype(ss) latest_ss;
        auto status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
        ASSERT_TRUE(status.ok());

        PartitionPtr partition;
        OperationContext context;
        for (auto& id : seg_ids) {
            auto seg = latest_ss->GetResource<Segment>(id);
            if (!seg) {
                std::cout << "Error seg=" << id << std::endl;
                ASSERT_TRUE(seg);
            }
            if (!partition) {
                partition = latest_ss->GetResource<Partition>(seg->GetPartitionId());
                ASSERT_TRUE(partition);
            } else {
                ASSERT_EQ(seg->GetPartitionId(), partition->GetID());
            }
            context.stale_segments.push_back(seg);
            if (!context.prev_partition) {
                context.prev_partition = latest_ss->GetResource<Partition>(
                        seg->GetPartitionId());
            }
        }

        context.lsn = next_lsn();
        auto op = std::make_shared<MergeOperation>(context, latest_ss);
        SegmentPtr new_seg;
        status = op->CommitNewSegment(new_seg);
        ASSERT_TRUE(status.ok());
        sf_context.segment_id = new_seg->GetID();
        SegmentFilePtr seg_file;
        status = op->CommitNewSegmentFile(sf_context, seg_file);
        ASSERT_TRUE(status.ok());
        std::unique_lock<std::mutex> lock(merge_mtx);
        status = op->Push();
        if (!status.ok()) {
            lock.unlock();
            std::unique_lock<std::mutex> blk(built_mtx);
            std::cout << status.ToString() << std::endl;
            /* for (auto id : built_segs) { */
            /*     std::cout << "builted " << id << std::endl; */
            /* } */
            /* for (auto id : seg_ids) { */
            /*     std::cout << "to_merge " << id << std::endl; */
            /* } */
            bool stale_found = false;
            for (auto& seg_id : seg_ids) {
                auto it = built_segs.find(seg_id);
                if (it != built_segs.end()) {
                    stale_found = true;
                    break;
                }
            }
            ASSERT_TRUE(stale_found);
            return;
        }
        ID_TYPE ss_id = latest_ss->GetID();
        status = op->GetSnapshot(latest_ss);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(latest_ss->GetID() > ss_id);

        merged_segs_history[new_seg->GetID()] = seg_ids;
        for (auto& seg_id : seg_ids) {
            merged_segs.insert(seg_id);
        }
        new_seg_id = new_seg->GetID();
        ASSERT_EQ(new_seg->GetPartitionId(), partition->GetID());
    };

    // TODO: If any Compound Operation find larger Snapshot. This Operation should be rollback to latest
    auto handler_worker = [&] {
        auto loop_cnt = RandomInt(10, 20);
        decltype(ss) latest_ss;

        auto create_new_segment = [&]() {
            ID_TYPE partition_id;
            {
                std::unique_lock<std::mutex> lock(partition_mtx);
                auto idx = RandomInt(0, partitions.size() - 1);
                partition_id = partitions[idx];
            }
            Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
            OperationContext context;
            context.prev_partition = latest_ss->GetResource<Partition>(partition_id);
            context.lsn = next_lsn();
            auto op = std::make_shared<NewSegmentOperation>(context, latest_ss);
            SegmentPtr new_seg;
            status = op->CommitNewSegment(new_seg);
            if (!status.ok()) {
                std::cout << status.ToString() << std::endl;
            }
            ASSERT_TRUE(status.ok());
            SegmentFilePtr seg_file;
            sf_context.segment_id = new_seg->GetID();
            op->CommitNewSegmentFile(sf_context, seg_file);
            op->Push();
            status = op->GetSnapshot(latest_ss);
            ASSERT_TRUE(status.ok());

            {
                std::unique_lock<std::mutex> lock(all_mtx);
                all_segments.insert(new_seg->GetID());
            }
            if (RandomInt(0, 10) >= 7) {
                build_queue.Put(new_seg->GetID());
            }
            merge_queue.Put(new_seg->GetID());
        };

        auto create_partition = [&]() {
            std::stringstream ss;
            ss << "fake_partition_" << next_pid();
            PartitionContext context;
            context.name = ss.str();
            std::unique_lock<std::mutex> lock(partition_mtx);
            auto latest_ss = CreatePartition(collection_name, context, next_lsn());
            ASSERT_TRUE(latest_ss);
            auto partition = latest_ss->GetPartition(ss.str());
            partitions.push_back(partition->GetID());
            if (latest_ss->NumberOfPartitions() != partitions.size()) {
                for (auto& pid : partitions) {
                    std::cout << "PartitionId=" << pid << std::endl;
                }
            }
            ASSERT_EQ(latest_ss->NumberOfPartitions(), partitions.size());
        };

        for (auto i = 0; i < loop_cnt; ++i) {
            if (RandomInt(0, 10) > 7) {
                create_partition();
            }
            create_new_segment();
        }
    };

    std::map<ID_TYPE, std::set<ID_TYPE>> merge_segs;
    auto merge_worker = [&] {
        while (true) {
            auto seg_id = merge_queue.Take();
            if (seg_id == 0) {
                std::cout << "Exiting Merge Worker" << std::endl;
                break;
            }
            decltype(ss) latest_ss;
            auto status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
            ASSERT_TRUE(status.ok());
            auto seg = latest_ss->GetResource<Segment>(seg_id);
            if (!seg) {
                std::cout << "SegID=" << seg_id << std::endl;
                std::cout << latest_ss->ToString() << std::endl;
                ASSERT_TRUE(seg);
            }

            auto it_segs = merge_segs.find(seg->GetPartitionId());
            if (it_segs == merge_segs.end()) {
                merge_segs[seg->GetPartitionId()] = {seg->GetID()};
            } else {
                merge_segs[seg->GetPartitionId()].insert(seg->GetID());
            }
            auto& segs = merge_segs[seg->GetPartitionId()];
            if ((segs.size() >= 2) && (RandomInt(0, 10) >= 2)) {
                std::cout << "Merging partition " << seg->GetPartitionId() << " segs (";
                for (auto seg : segs) {
                    std::cout << seg << ",";
                }
                std::cout << ")" << std::endl;
                ID_TYPE new_seg_id = 0;
                do_merge(segs, new_seg_id);
                segs.clear();
                if (new_seg_id == 0) {
                    continue;
                }
                if (RandomInt(0, 10) >= 6) {
                    build_queue.Put(new_seg_id);
                }

            } else {
                continue;
            }
        }
        merge_waiter.Notify();
        build_queue.Put(0);
        build_waiter.Wait();
    };

    auto build_worker = [&] {
        while (true) {
            auto seg_id = build_queue.Take();
            if (seg_id == 0) {
                std::cout << "Exiting Build Worker" << std::endl;
                break;
            }

            std::cout << "Building " << seg_id << std::endl;
            do_build(seg_id);
        }
        build_waiter.Notify();
    };
    std::vector<std::thread> handlers;
    auto num_handlers = RandomInt(8, 9);
    for (auto i = 0; i < num_handlers; ++i) {
        handlers.emplace_back(handler_worker);
    }
    std::thread t3 = std::thread(merge_worker);
    std::thread t4 = std::thread(build_worker);

    for (auto& handler : handlers) {
        handler.join();
    }

    merge_queue.Put(0);
    t3.join();
    t4.join();

    /* for (auto& kv : merged_segs_history) { */
    /*     std::cout << "merged: ("; */
    /*     for (auto i : kv.second) { */
    /*         std::cout << i << ","; */
    /*     } */
    /*     std::cout << ") -> " << kv.first << std::endl; */
    /* } */

    /* for (auto& id : built_segs) { */
    /*     std::cout << "built: " << id << std::endl; */
    /* } */

    merge_waiter.Wait();

    decltype(ss) latest_ss;
    status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
    ASSERT_TRUE(status.ok());
    auto expect_segments = all_segments;
    for (auto& kv : merged_segs_history) {
        expect_segments.insert(kv.first);
        for (auto& id : kv.second) {
            expect_segments.erase(id);
        }
    }
    decltype(expect_segments) final_segments;
    auto segments = latest_ss->GetResources<Segment>();
    for (auto& kv : segments) {
        final_segments.insert(kv.first);
    }
    ASSERT_EQ(final_segments, expect_segments);

    auto final_segment_file_cnt = latest_ss->GetResources<SegmentFile>().size();

    decltype(final_segment_file_cnt) expect_segment_file_cnt;
    expect_segment_file_cnt = expect_segments.size();
    expect_segment_file_cnt += built_segs.size();
    std::cout << latest_ss->ToString() << std::endl;
    std::vector<int> common_ids;
    std::set_intersection(merged_segs.begin(), merged_segs.end(), built_segs.begin(), built_segs.end(),
            std::back_inserter(common_ids));
    expect_segment_file_cnt -= common_ids.size();
    ASSERT_EQ(expect_segment_file_cnt, final_segment_file_cnt);
}


TEST_F(SnapshotTest, CompoundTest2) {
    milvus::Status status;
    LSN_TYPE lsn = 0;
    auto next_lsn = [&]() -> decltype(lsn)& {
        return ++lsn;
    };
    LSN_TYPE pid = 0;
    auto next_pid = [&]() -> decltype(pid) {
        return ++pid;
    };
    std::string collection_name("c1");
    auto ss = CreateCollection(collection_name, next_lsn());
    ASSERT_TRUE(ss);
    ASSERT_EQ(lsn, ss->GetMaxLsn());

    TQueue merge_queue;
    TQueue build_queue;

    std::set<ID_TYPE> all_segments;
    std::set<ID_TYPE> segment_in_building;
    std::map<ID_TYPE, std::set<ID_TYPE>> merged_segs_history;
    std::set<ID_TYPE> merged_segs;
    std::set<ID_TYPE> built_segs;
    std::set<ID_TYPE> build_stale_segs;

    std::mutex all_mtx;
    std::mutex merge_mtx;
    std::mutex built_mtx;
    std::mutex partition_mtx;
    std::mutex seg_p_mtx;

    WaitableObj merge_waiter;
    WaitableObj build_waiter;

    SegmentFileContext sf_context;
    sf_context.field_name = "vector";
    sf_context.field_element_name = "ivfsq8";
    sf_context.segment_id = 1;
    sf_context.partition_id = 1;

    IDS_TYPE partitions = {ss->GetResources<Partition>().begin()->second->GetID()};
    std::set<ID_TYPE> stale_partitions;
    std::map<ID_TYPE, ID_TYPE> seg_p_map;

    auto do_build = [&] (const ID_TYPE& seg_id, const ID_TYPE& p_id) {
        decltype(ss) latest_ss;
        auto status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
        ASSERT_TRUE(status.ok());

        auto build_sf_context = sf_context;

        OperationContext context;
        context.lsn = next_lsn();
        auto build_op = std::make_shared<BuildOperation>(context, latest_ss);
        SegmentFilePtr seg_file;
        build_sf_context.segment_id = seg_id;
        status = build_op->CommitNewSegmentFile(build_sf_context, seg_file);
        if (!status.ok()) {
            std::cout << status.ToString() << std::endl;
            {
                std::unique_lock<std::mutex> lock(partition_mtx);
                auto it = stale_partitions.find(p_id);
                if (it != stale_partitions.end()) {
                    std::cout << "Segment " << seg_id << " host partition " << p_id << " is stale" << std::endl;
                    return;
                }
            }
            std::unique_lock<std::mutex> lock(merge_mtx);
            auto it = merged_segs.find(seg_id);
            ASSERT_NE(it, merged_segs.end());
            return;
        }
        std::unique_lock<std::mutex> lock(built_mtx);
        status = build_op->Push();
        if (!status.ok()) {
            lock.unlock();
            std::cout << status.ToString() << std::endl;
            {
                std::unique_lock<std::mutex> lock(partition_mtx);
                auto it = stale_partitions.find(p_id);
                if (it != stale_partitions.end()) {
                    std::cout << "Segment " << seg_id << " host partition " << p_id << " is stale" << std::endl;
                    return;
                }
            }
            std::unique_lock<std::mutex> lock(merge_mtx);
            auto it = merged_segs.find(seg_id);
            ASSERT_NE(it, merged_segs.end());
            return;
        }
        ASSERT_TRUE(status.ok());
        status = build_op->GetSnapshot(latest_ss);
        ASSERT_TRUE(status.ok());

        built_segs.insert(seg_id);
    };

    auto do_merge = [&] (std::set<ID_TYPE>& seg_ids, ID_TYPE& new_seg_id, const ID_TYPE& p_id) {
        if (seg_ids.size() == 0) {
            return;
        }
        decltype(ss) latest_ss;
        auto status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
        ASSERT_TRUE(status.ok());

        PartitionPtr partition;
        OperationContext context;
        for (auto& id : seg_ids) {
            auto seg = latest_ss->GetResource<Segment>(id);
            if (!seg) {
                std::cout << "Error seg=" << id << std::endl;
                ASSERT_TRUE(seg);
            }
            if (!partition) {
                partition = latest_ss->GetResource<Partition>(seg->GetPartitionId());
                ASSERT_TRUE(partition);
            } else {
                ASSERT_EQ(seg->GetPartitionId(), partition->GetID());
            }
            context.stale_segments.push_back(seg);
            if (!context.prev_partition) {
                context.prev_partition = latest_ss->GetResource<Partition>(
                        seg->GetPartitionId());
            }
        }

        context.lsn = next_lsn();
        auto op = std::make_shared<MergeOperation>(context, latest_ss);
        SegmentPtr new_seg;
        status = op->CommitNewSegment(new_seg);
        ASSERT_TRUE(status.ok());
        sf_context.segment_id = new_seg->GetID();
        SegmentFilePtr seg_file;
        status = op->CommitNewSegmentFile(sf_context, seg_file);
        ASSERT_TRUE(status.ok());
        std::unique_lock<std::mutex> lock(merge_mtx);
        status = op->Push();
        if (!status.ok()) {
            lock.unlock();
            bool stale_found = false;
            {
                std::unique_lock<std::mutex> blk(built_mtx);
                std::cout << status.ToString() << std::endl;
                /* for (auto id : built_segs) { */
                /*     std::cout << "builted " << id << std::endl; */
                /* } */
                /* for (auto id : seg_ids) { */
                /*     std::cout << "to_merge " << id << std::endl; */
                /* } */
                for (auto& seg_id : seg_ids) {
                    auto it = built_segs.find(seg_id);
                    if (it != built_segs.end()) {
                        stale_found = true;
                        break;
                    }
                }
            }
            if (!stale_found) {
                std::unique_lock<std::mutex> lock(partition_mtx);
                auto it = stale_partitions.find(p_id);
                if (it != stale_partitions.end()) {
                    /* std::cout << "Segment " << seg_id << " host partition " << p_id << " is stale" << std::endl; */
                    stale_found = true;
                }
            }

            ASSERT_TRUE(stale_found);
            return;
        }
        ID_TYPE ss_id = latest_ss->GetID();
        status = op->GetSnapshot(latest_ss);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(latest_ss->GetID() > ss_id);

        merged_segs_history[new_seg->GetID()] = seg_ids;
        for (auto& seg_id : seg_ids) {
            merged_segs.insert(seg_id);
        }
        new_seg_id = new_seg->GetID();
        seg_p_map[new_seg_id] = new_seg->GetPartitionId();
        ASSERT_EQ(new_seg->GetPartitionId(), partition->GetID());
    };

    // TODO: If any Compound Operation find larger Snapshot. This Operation should be rollback to latest
    auto handler_worker = [&] {
        auto loop_cnt = RandomInt(30, 35);
        decltype(ss) latest_ss;

        auto create_new_segment = [&]() {
            ID_TYPE partition_id;
            {
                std::unique_lock<std::mutex> lock(partition_mtx);
                auto idx = RandomInt(0, partitions.size() - 1);
                partition_id = partitions[idx];
            }
            Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
            OperationContext context;
            context.prev_partition = latest_ss->GetResource<Partition>(partition_id);
            context.lsn = next_lsn();
            auto op = std::make_shared<NewSegmentOperation>(context, latest_ss);
            SegmentPtr new_seg;
            status = op->CommitNewSegment(new_seg);
            if (!status.ok()) {
                std::cout << status.ToString() << std::endl;
                std::unique_lock<std::mutex> lock(partition_mtx);
                ASSERT_TRUE(stale_partitions.find(partition_id) != stale_partitions.end());
                return;
            }
            ASSERT_TRUE(status.ok());
            SegmentFilePtr seg_file;
            sf_context.segment_id = new_seg->GetID();
            status = op->CommitNewSegmentFile(sf_context, seg_file);
            if (!status.ok()) {
                std::cout << status.ToString() << std::endl;
                ASSERT_TRUE(status.ok());
            }
            status = op->Push();
            if (!status.ok()) {
                std::cout << status.ToString() << std::endl;
                std::unique_lock<std::mutex> lock(partition_mtx);
                ASSERT_TRUE(stale_partitions.find(partition_id) != stale_partitions.end());
                return;
            }
            status = op->GetSnapshot(latest_ss);
            if (!status.ok()) {
                std::cout << status.ToString() << std::endl;
                std::unique_lock<std::mutex> lock(partition_mtx);
                ASSERT_TRUE(stale_partitions.find(partition_id) != stale_partitions.end());
                return;
            }

            {
                std::unique_lock<std::mutex> lock(all_mtx);
                all_segments.insert(new_seg->GetID());
            }
            if (RandomInt(0, 10) >= 7) {
                build_queue.Put({new_seg->GetID(), new_seg->GetPartitionId()});
            }
            seg_p_map[new_seg->GetID()] = new_seg->GetPartitionId();
            merge_queue.Put({new_seg->GetID(), new_seg->GetPartitionId()});
        };

        auto create_partition = [&]() {
            std::stringstream ss;
            ss << "fake_partition_" << next_pid();
            PartitionContext context;
            context.name = ss.str();
            std::unique_lock<std::mutex> lock(partition_mtx);
            auto latest_ss = CreatePartition(collection_name, context, next_lsn());
            ASSERT_TRUE(latest_ss);
            auto partition = latest_ss->GetPartition(ss.str());
            partitions.push_back(partition->GetID());
            if (latest_ss->NumberOfPartitions() != partitions.size()) {
                for (auto& pid : partitions) {
                    std::cout << "PartitionId=" << pid << std::endl;
                }
            }
            ASSERT_EQ(latest_ss->NumberOfPartitions(), partitions.size());
        };

        auto drop_partition = [&]() {
            std::unique_lock<std::mutex> lock(partition_mtx);
            if (partitions.size() <= 2) {
                return;
            }
            decltype(ss) latest_ss;
            Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
            auto index = RandomInt(0, partitions.size() - 1);
            auto pid = partitions[index];
            std::cout << "Dropping partition " << pid << std::endl;
            status = Snapshots::GetInstance().DropPartition(
                    latest_ss->GetCollectionId(), pid, next_lsn());
            ASSERT_TRUE(status.ok());
            stale_partitions.insert(pid);
            partitions.erase(partitions.begin() + index);
        };

        for (auto i = 0; i < loop_cnt; ++i) {
            if (RandomInt(0, 10) > 6) {
                create_partition();
            }
            if (RandomInt(0, 10) > 8) {
                drop_partition();
            }
            create_new_segment();
        }
    };

    std::map<ID_TYPE, std::set<ID_TYPE>> merge_segs;
    auto merge_worker = [&] {
        while (true) {
            auto ids = merge_queue.Take();
            auto seg_id = std::get<0>(ids);
            auto p_id = std::get<1>(ids);
            if (seg_id == 0) {
                std::cout << "Exiting Merge Worker" << std::endl;
                break;
            }
            decltype(ss) latest_ss;
            auto status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
            ASSERT_TRUE(status.ok());
            auto seg = latest_ss->GetResource<Segment>(seg_id);
            if (!seg) {
                std::cout << "SegID=" << seg_id << std::endl;
                std::cout << latest_ss->ToString() << std::endl;
                std::unique_lock<std::mutex> lock(partition_mtx);
                auto it = stale_partitions.find(p_id);
                if (it != stale_partitions.end()) {
                    std::cout << "Segment " << seg_id << " host partition " << p_id << " is stale" << std::endl;
                    continue;
                }
                ASSERT_TRUE(seg);
            }

            auto it_segs = merge_segs.find(seg->GetPartitionId());
            if (it_segs == merge_segs.end()) {
                merge_segs[seg->GetPartitionId()] = {seg->GetID()};
            } else {
                merge_segs[seg->GetPartitionId()].insert(seg->GetID());
            }
            auto& segs = merge_segs[seg->GetPartitionId()];
            if ((segs.size() >= 2) && (RandomInt(0, 10) >= 2)) {
                std::cout << "Merging partition " << seg->GetPartitionId() << " segs (";
                for (auto seg : segs) {
                    std::cout << seg << ",";
                }
                std::cout << ")" << std::endl;
                ID_TYPE new_seg_id = 0;
                ID_TYPE partition_id = 0;
                do_merge(segs, new_seg_id, p_id);
                segs.clear();
                if (new_seg_id == 0) {
                    continue;
                }
                if (RandomInt(0, 10) >= 6) {
                    build_queue.Put({new_seg_id, p_id});
                }

            } else {
                continue;
            }
        }
        merge_waiter.Notify();
        build_queue.Put({0, 0});
        build_waiter.Wait();
    };

    auto build_worker = [&] {
        while (true) {
            auto ids = build_queue.Take();
            auto seg_id = std::get<0>(ids);
            auto p_id = std::get<1>(ids);
            if (seg_id == 0) {
                std::cout << "Exiting Build Worker" << std::endl;
                break;
            }

            std::cout << "Building " << seg_id << std::endl;
            do_build(seg_id, p_id);
        }
        build_waiter.Notify();
    };
    std::vector<std::thread> handlers;
    auto num_handlers = RandomInt(6, 8);
    for (auto i = 0; i < num_handlers; ++i) {
        handlers.emplace_back(handler_worker);
    }
    std::thread t3 = std::thread(merge_worker);
    std::thread t4 = std::thread(build_worker);

    for (auto& handler : handlers) {
        handler.join();
    }

    merge_queue.Put({0, 0});
    t3.join();
    t4.join();

    /* for (auto& kv : merged_segs_history) { */
    /*     std::cout << "merged: ("; */
    /*     for (auto i : kv.second) { */
    /*         std::cout << i << ","; */
    /*     } */
    /*     std::cout << ") -> " << kv.first << std::endl; */
    /* } */

    /* for (auto& id : built_segs) { */
    /*     std::cout << "built: " << id << std::endl; */
    /* } */

    merge_waiter.Wait();

    decltype(ss) latest_ss;
    status = Snapshots::GetInstance().GetSnapshot(latest_ss, collection_name);
    ASSERT_TRUE(status.ok());

    auto expect_segments = all_segments;
    for (auto& kv : merged_segs_history) {
        expect_segments.insert(kv.first);
        for (auto& id : kv.second) {
            expect_segments.erase(id);
        }
    }

    for (auto& seg_p : seg_p_map) {
        auto it = stale_partitions.find(seg_p.second);
        if (it == stale_partitions.end()) {
            continue;
        }
        /* std::cout << "stale Segment " << seg_p.first << std::endl; */
        expect_segments.erase(seg_p.first);
    }

    decltype(expect_segments) final_segments;
    auto segments = latest_ss->GetResources<Segment>();
    for (auto& kv : segments) {
        final_segments.insert(kv.first);
    }
    ASSERT_EQ(final_segments, expect_segments);
    // TODO: Check Total Segment Files Cnt
}
