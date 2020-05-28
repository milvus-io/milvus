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


#if 1
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

TEST_F(SnapshotTest, OperationTest) {
    {
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
        }
    }
}
#endif
