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

TEST_F(SnapshotTest, CreateCollectionOperationTest) {
    milvus::engine::snapshot::Store::GetInstance().DoReset();
    auto expect_null = milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(100000);
    ASSERT_TRUE(!expect_null);

    std::string collection_name = "test_c1";
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
    auto ss = op->GetSnapshot();

    auto latest_ss = milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot("xxxx");
    ASSERT_TRUE(!latest_ss);

    latest_ss = milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(collection_name);
    ASSERT_TRUE(latest_ss);
    ASSERT_TRUE(latest_ss->GetName() == collection_name);

    auto ids = milvus::engine::snapshot::Snapshots::GetInstance().GetCollectionIds();
    ASSERT_EQ(ids.size(), 1);
    ASSERT_EQ(ids[0], latest_ss->GetCollectionId());

    milvus::engine::snapshot::OperationContext sd_op_ctx;
    sd_op_ctx.collection = latest_ss->GetCollection();
    ASSERT_TRUE(sd_op_ctx.collection->IsActive());
    auto sd_op = std::make_shared<milvus::engine::snapshot::SoftDeleteCollectionOperation>(sd_op_ctx);
    sd_op->Push();
    ASSERT_TRUE(sd_op->GetStatus().ok());
    ASSERT_TRUE(!sd_op_ctx.collection->IsActive());
    ASSERT_TRUE(!latest_ss->GetCollection()->IsActive());

    milvus::engine::snapshot::Snapshots::GetInstance().Reset();
}

TEST_F(SnapshotTest, OperationTest) {
    {
        std::string to_string;
        milvus::engine::snapshot::SegmentFileContext sf_context;
        sf_context.field_name = "f_1_1";
        sf_context.field_element_name = "fe_1_1";
        sf_context.segment_id = 1;
        sf_context.partition_id = 1;

        auto ss = milvus::engine::snapshot::Snapshots::GetInstance().GetSnapshot(1);
        auto ss_id = ss->GetID();

        // Check snapshot
        {
            auto collection_commit = milvus::engine::snapshot::CollectionCommitsHolder::GetInstance()
                .GetResource(ss_id, false);
            /* snapshot::SegmentCommitsHolder::GetInstance().GetResource(prev_segment_commit->GetID()); */
            ASSERT_TRUE(collection_commit);
            to_string = collection_commit->ToString();
        }

        milvus::engine::snapshot::OperationContext merge_ctx;
        std::set<milvus::engine::snapshot::ID_TYPE> stale_segment_commit_ids;

        // Check build operation correctness
        {
            milvus::engine::snapshot::OperationContext context;
            auto build_op = std::make_shared<milvus::engine::snapshot::BuildOperation>(context, ss);
            auto seg_file = build_op->CommitNewSegmentFile(sf_context);
            ASSERT_TRUE(seg_file);
            auto prev_segment_commit = ss->GetSegmentCommit(seg_file->GetSegmentId());
            auto prev_segment_commit_mappings = prev_segment_commit->GetMappings();
            ASSERT_TRUE(prev_segment_commit->ToString() != "");

            build_op->Push();
            ss = build_op->GetSnapshot();
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
            auto new_seg = op->CommitNewSegment();
            ASSERT_TRUE(new_seg->ToString() != "");
            auto seg_file = op->CommitNewSegmentFile(sf_context);
            op->Push();

            ss = op->GetSnapshot();
            ASSERT_TRUE(ss->GetID() > ss_id);

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
        }

        ss_id = ss->GetID();
        {
            auto prev_partition_commit = ss->GetPartitionCommitByPartitionId(partition_id);
            auto expect_null = ss->GetPartitionCommitByPartitionId(11111111);
            ASSERT_TRUE(!expect_null);
            ASSERT_TRUE(prev_partition_commit->ToString() != "");
            auto op = std::make_shared<milvus::engine::snapshot::MergeOperation>(merge_ctx, ss);
            auto new_seg = op->CommitNewSegment();
            sf_context.segment_id = new_seg->GetID();
            auto seg_file = op->CommitNewSegmentFile(sf_context);
            op->Push();
            ss = op->GetSnapshot();
            ASSERT_TRUE(ss->GetID() > ss_id);

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
        }
    }
    milvus::engine::snapshot::Snapshots::GetInstance().Reset();
}
