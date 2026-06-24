// Copyright (C) 2019-2026 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>

#include <initializer_list>
#include <memory>

#include "common/OffsetMapping.h"

namespace milvus {
namespace {

struct ValidData {
    std::unique_ptr<bool[]> data;
    int64_t count;
};

ValidData
MakeValid(std::initializer_list<int> bits) {
    auto valid = std::make_unique<bool[]>(bits.size());
    int64_t offset = 0;
    for (const auto bit : bits) {
        valid[offset++] = bit != 0;
    }
    return {std::move(valid), offset};
}

}  // namespace

TEST(OffsetMapping, DefaultIsDisabledAndPassThrough) {
    OffsetMapping mapping;
    auto snapshot = mapping.GetSnapshot();
    EXPECT_FALSE(snapshot.IsEnabled());
    EXPECT_EQ(snapshot.GetValidCount(), 0);
    EXPECT_EQ(snapshot.GetTotalCount(), 0);
    EXPECT_EQ(snapshot.GetPhysicalOffset(42), 42);
    EXPECT_EQ(snapshot.GetLogicalOffset(42), 42);
    EXPECT_EQ(snapshot.GetLogicalOffsets(0, 1), nullptr);
}

TEST(OffsetMapping, AppendBuildsBidirectionalMapping) {
    OffsetMapping mapping;
    auto valid = MakeValid({1, 0, 1, 1});
    auto append_result = mapping.Append(valid.data.get(), valid.count, 0);

    EXPECT_EQ(append_result.physical_offset, 0);
    EXPECT_EQ(append_result.valid_count, 3);

    auto snapshot = mapping.GetSnapshot();
    EXPECT_TRUE(snapshot.IsEnabled());
    EXPECT_EQ(snapshot.GetTotalCount(), 4);
    EXPECT_EQ(snapshot.GetValidCount(), 3);
    EXPECT_EQ(snapshot.GetPhysicalOffset(0), 0);
    EXPECT_EQ(snapshot.GetPhysicalOffset(1), -1);
    EXPECT_EQ(snapshot.GetPhysicalOffset(2), 1);
    EXPECT_EQ(snapshot.GetPhysicalOffset(3), 2);
    EXPECT_EQ(snapshot.GetLogicalOffset(0), 0);
    EXPECT_EQ(snapshot.GetLogicalOffset(1), 2);
    EXPECT_EQ(snapshot.GetLogicalOffset(2), 3);
    EXPECT_EQ(snapshot.GetVisiblePhysicalCount(4), 3);
    auto ids = snapshot.GetLogicalOffsets(0, 3);
    ASSERT_NE(ids, nullptr);
    EXPECT_EQ(ids[0], 0);
    EXPECT_EQ(ids[1], 2);
    EXPECT_EQ(ids[2], 3);
}

TEST(OffsetMapping, AppendMultipleBatchesPreservesPhysicalVector) {
    OffsetMapping mapping;
    auto batch1 = MakeValid({1, 0, 1});
    auto append_result = mapping.Append(batch1.data.get(), batch1.count, 0);
    EXPECT_EQ(append_result.physical_offset, 0);
    EXPECT_EQ(append_result.valid_count, 2);

    auto batch2 = MakeValid({0, 1, 1});
    append_result = mapping.Append(batch2.data.get(), batch2.count);
    EXPECT_EQ(append_result.physical_offset, 2);
    EXPECT_EQ(append_result.valid_count, 2);

    auto snapshot = mapping.GetSnapshot();
    EXPECT_EQ(snapshot.GetValidCount(), 4);
    EXPECT_EQ(snapshot.GetTotalCount(), 6);
    EXPECT_EQ(snapshot.GetPhysicalOffset(0), 0);
    EXPECT_EQ(snapshot.GetPhysicalOffset(2), 1);
    EXPECT_EQ(snapshot.GetPhysicalOffset(4), 2);
    EXPECT_EQ(snapshot.GetPhysicalOffset(5), 3);
    EXPECT_EQ(snapshot.GetLogicalOffset(0), 0);
    EXPECT_EQ(snapshot.GetLogicalOffset(1), 2);
    EXPECT_EQ(snapshot.GetLogicalOffset(2), 4);
    EXPECT_EQ(snapshot.GetLogicalOffset(3), 5);
    EXPECT_EQ(snapshot.GetVisiblePhysicalCount(4), 2);
    EXPECT_EQ(snapshot.GetVisiblePhysicalCount(6), 4);
    auto ids = snapshot.GetLogicalOffsets(0, 4);
    ASSERT_NE(ids, nullptr);
    EXPECT_EQ(ids[0], 0);
    EXPECT_EQ(ids[1], 2);
    EXPECT_EQ(ids[2], 4);
    EXPECT_EQ(ids[3], 5);
}

TEST(OffsetMapping, AppendAllNullStillEnablesMapping) {
    OffsetMapping mapping;
    auto valid = MakeValid({0, 0, 0, 0});
    auto append_result = mapping.Append(valid.data.get(), valid.count, 0);
    EXPECT_EQ(append_result.physical_offset, 0);
    EXPECT_EQ(append_result.valid_count, 0);

    auto snapshot = mapping.GetSnapshot();
    EXPECT_TRUE(snapshot.IsEnabled());
    EXPECT_EQ(snapshot.GetTotalCount(), 4);
    EXPECT_EQ(snapshot.GetValidCount(), 0);
    EXPECT_EQ(snapshot.GetPhysicalOffset(0), -1);
    EXPECT_EQ(snapshot.GetVisiblePhysicalCount(4), 0);
    EXPECT_EQ(snapshot.GetLogicalOffsets(0, 1), nullptr);
}

TEST(OffsetMapping, AppendNoopOnNullOrZero) {
    OffsetMapping mapping;
    auto append_result = mapping.Append(nullptr, 3, 0);
    EXPECT_EQ(append_result.physical_offset, 0);
    EXPECT_EQ(append_result.valid_count, 0);
    EXPECT_FALSE(mapping.GetSnapshot().IsEnabled());

    auto valid = MakeValid({1});
    append_result = mapping.Append(valid.data.get(), 0, 0);
    EXPECT_EQ(append_result.physical_offset, 0);
    EXPECT_EQ(append_result.valid_count, 0);
    EXPECT_FALSE(mapping.GetSnapshot().IsEnabled());
}

}  // namespace milvus
