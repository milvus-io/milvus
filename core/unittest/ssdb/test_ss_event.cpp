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

    PartitionPtr partition;
    status = store_->CreateResource<Partition>(Partition("test_gc_c1_p1", collection->GetID()), partition);
    ASSERT_TRUE(status.ok()) << status.ToString();

    // TODO(yhz): Create a temp file under "test_gc_c1" to check if disk file has been deleted

    auto event = std::make_shared<InActiveResourcesGCEvent>();
    status = event->Process(store_);
    ASSERT_TRUE(status.ok()) << status.ToString();

    CollectionPtr collection2;
    status = store_->GetResource<Collection>(collection->GetID(), collection2);
    ASSERT_FALSE(status.ok());

    PartitionPtr partition2;
    status = store_->GetResource<Partition>(partition->GetID(), partition2);
    ASSERT_FALSE(status.ok());
}
