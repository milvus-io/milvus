// Copyright (C) 2019-2026 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "common/ClusteringRowRanges.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentSealed.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"
#include "gtest/gtest.h"
using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

TEST(ClusteringRanges, SealedPhysicalRowsAndFallback) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto data = DataGen(schema, 100);
    auto segment = CreateSealedSegment(schema, empty_index_meta, 9182);
    LoadGeneratedDataIntoSegment(data, segment.get(), false, {});
    ScopedSchemaHandle handle(*schema);
    auto serialized = handle.ParseSearch("", "vec", 5, "L2", "{}", 3);
    auto plan =
        CreateSearchPlanByExpr(schema, serialized.data(), serialized.size());
    auto raw = CreatePlaceholderGroup(2, 4, 42);
    auto queries = ParsePlaceholderGroup(plan.get(), raw.SerializeAsString());
    auto search = [&](ClusteringRowRangesPtr ranges) {
        return segment->Search(plan.get(),
                               queries.get(),
                               1000000,
                               folly::CancellationToken(),
                               0,
                               0,
                               0,
                               false,
                               false,
                               nullptr,
                               ranges);
    };
    auto unrestricted = search(nullptr);
    auto ranges = std::make_shared<ClusteringRowRanges>();
    ranges->expected_rows = 100;
    ranges->ranges = {{20, 4}, {70, 6}};
    auto selected = search(ranges);
    ASSERT_EQ(selected->seg_offsets_.size(), 10);
    for (auto offset : selected->seg_offsets_) {
        ASSERT_TRUE((offset >= 20 && offset < 24) ||
                    (offset >= 70 && offset < 76));
    }
    // The same cached plan has no request-specific state.
    EXPECT_EQ(search(nullptr)->seg_offsets_, unrestricted->seg_offsets_);
    ranges->ranges.clear();
    for (auto offset : search(ranges)->seg_offsets_) EXPECT_EQ(offset, -1);
    ranges->ranges = {{98, 3}};
    EXPECT_EQ(search(ranges)->seg_offsets_, unrestricted->seg_offsets_);
    ranges->ranges = {{20, 4}};
    ranges->expected_rows = 101;
    EXPECT_EQ(search(ranges)->seg_offsets_, unrestricted->seg_offsets_);
}

TEST(ClusteringRanges, MaskIntersectionAndValidation) {
    BitsetType mask(10, false);
    mask.set(3, true);
    ClusteringRowRanges ranges{10, {{2, 3}, {8, 2}}};
    ASSERT_TRUE(ranges.Apply(mask, 10));
    for (int i = 0; i < 10; ++i)
        EXPECT_EQ(mask[i], i == 3 || !(i >= 2 && i < 5 || i >= 8));
    std::vector<bool> original(10);
    for (int i = 0; i < 10; ++i) original[i] = mask[i];
    ranges.ranges = {{1, 5}, {4, 2}};
    EXPECT_FALSE(ranges.Apply(mask, 10));
    for (int i = 0; i < 10; ++i) EXPECT_EQ(mask[i], original[i]);
    ranges.ranges = {{9, INT64_MAX}};
    EXPECT_FALSE(ranges.Apply(mask, 10));
}
