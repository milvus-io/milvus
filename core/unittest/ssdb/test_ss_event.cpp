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
#include "ssdb/utils.h"

using InActiveResourcesGCEvent = milvus::engine::snapshot::InActiveResourcesGCEvent;

TEST_F(SSEventTest, TestInActiveResGcEvent) {
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


    CollectionCommitPtr collection_commit;
    status = store_->CreateResource<CollectionCommit>(CollectionCommit(0, 0), collection_commit);
    ASSERT_TRUE(status.ok()) << status.ToString();

    CollectionCommitPtr inactive_collection_commit;
    auto cc = CollectionCommit(0, 0);
    cc.Deactivate();
    status = store_->CreateResource<CollectionCommit>(std::move(cc), inactive_collection_commit);
    ASSERT_TRUE(status.ok()) << status.ToString();

    // TODO(yhz): Check if disk file has been deleted

    auto event = std::make_shared<InActiveResourcesGCEvent>();
    status = event->Process(store_);
    ASSERT_TRUE(status.ok()) << status.ToString();

    CollectionPtr collection2;
    status = store_->GetResource<Collection>(collection->GetID(), collection2);
    ASSERT_FALSE(status.ok());

    CollectionPtr inactive_collection2;
    status = store_->GetResource<Collection>(inactive_collection->GetID(), inactive_collection2);
    ASSERT_FALSE(status.ok());

    PartitionPtr partition2;
    status = store_->GetResource<Partition>(partition->GetID(), partition2);
    ASSERT_FALSE(status.ok());
}
