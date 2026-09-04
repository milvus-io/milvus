// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/FieldMeta.h"
#include "common/GroupChunk.h"
#include "common/Types.h"
#include "index/SkipIndex.h"
#include "index/skipindex_stats/SkipIndexStats.h"
#include "mmap/ChunkedColumnGroup.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus {

namespace {

// Build a ProxyChunkColumn whose skip metrics are owned by the group meta, and
// whose single cell (chunk 0) carries the given FieldChunkMetrics.
std::shared_ptr<ChunkedColumnInterface>
MakeColumnWithMetrics(FieldId field_id,
                      std::shared_ptr<index::FieldChunkMetrics> metrics) {
    segcore::storagev2translator::SkipMetricsByField metrics_by_field;
    metrics_by_field[field_id.get()].push_back(std::move(metrics));

    std::vector<std::unique_ptr<GroupChunk>> group_chunks(1);
    auto translator =
        std::make_unique<TestGroupChunkTranslator>(1,
                                                   std::vector<int64_t>{1},
                                                   "skip_index_test",
                                                   std::move(group_chunks),
                                                   std::move(metrics_by_field));
    auto column_group =
        std::make_shared<ChunkedColumnGroup>(std::move(translator));
    FieldMeta field_meta(
        FieldName("i64"), field_id, DataType::INT64, false, std::nullopt);
    return std::make_shared<ProxyChunkColumn>(
        column_group, field_id, field_meta);
}

std::shared_ptr<index::FieldChunkMetrics>
MakeIntMetrics(int64_t min, int64_t max) {
    return std::make_shared<index::IntFieldChunkMetrics<int64_t>>(
        min, max, nullptr);
}

std::shared_ptr<index::FieldChunkMetrics>
MakeAllNullMetrics() {
    auto metrics = std::make_shared<index::NoneFieldChunkMetrics>();
    metrics->SetNullState(index::FieldChunkMetrics::NullState::AllNulls);
    return metrics;
}

}  // namespace

class SkipIndexTest : public ::testing::Test {
 protected:
    FieldId field_id_{1};
};

TEST_F(SkipIndexTest, FailOpenWhenNoSource) {
    SkipIndex skip_index;
    // No field registered: the skip filter must fail open (never skip).
    EXPECT_FALSE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 5));
    EXPECT_FALSE(skip_index.CanSkipBinaryRange<int64_t>(
        field_id_, 0, 0, 10, true, true));
    EXPECT_FALSE(skip_index.CanSkipInQuery<int64_t>(
        field_id_, 0, std::vector<int64_t>{1, 2, 3}));
}

TEST_F(SkipIndexTest, FailOpenWhenMissingMetrics) {
    SkipIndex skip_index;
    skip_index.LoadSkipSource(field_id_,
                              MakeColumnWithMetrics(field_id_, nullptr));
    // The column exposes no metrics for chunk 0: still fail open.
    EXPECT_FALSE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 5));
}

TEST_F(SkipIndexTest, UnaryRange) {
    SkipIndex skip_index;
    skip_index.LoadSkipSource(
        field_id_, MakeColumnWithMetrics(field_id_, MakeIntMetrics(0, 10)));

    // Equal inside [0, 10] cannot be skipped; outside can.
    EXPECT_FALSE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 5));
    EXPECT_TRUE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));

    // GreaterThan above the max skips; GreaterThan within the range does not.
    EXPECT_TRUE(skip_index.CanSkipUnaryRange<int64_t>(
        field_id_, 0, OpType::GreaterThan, 10));
    EXPECT_FALSE(skip_index.CanSkipUnaryRange<int64_t>(
        field_id_, 0, OpType::GreaterThan, 5));

    // LessThan below the min skips.
    EXPECT_TRUE(skip_index.CanSkipUnaryRange<int64_t>(
        field_id_, 0, OpType::LessThan, 0));
}

TEST_F(SkipIndexTest, BinaryRange) {
    SkipIndex skip_index;
    skip_index.LoadSkipSource(
        field_id_, MakeColumnWithMetrics(field_id_, MakeIntMetrics(0, 10)));

    // Query [20, 30] is disjoint from [0, 10]: skip.
    EXPECT_TRUE(skip_index.CanSkipBinaryRange<int64_t>(
        field_id_, 0, 20, 30, true, true));
    // Query [5, 15] overlaps [0, 10]: do not skip.
    EXPECT_FALSE(skip_index.CanSkipBinaryRange<int64_t>(
        field_id_, 0, 5, 15, true, true));
}

TEST_F(SkipIndexTest, InQuery) {
    SkipIndex skip_index;
    skip_index.LoadSkipSource(
        field_id_, MakeColumnWithMetrics(field_id_, MakeIntMetrics(0, 10)));

    // IN list entirely outside [0, 10]: skip.
    EXPECT_TRUE(skip_index.CanSkipInQuery<int64_t>(
        field_id_, 0, std::vector<int64_t>{100, 200}));
    // IN list overlapping [0, 10]: do not skip.
    EXPECT_FALSE(skip_index.CanSkipInQuery<int64_t>(
        field_id_, 0, std::vector<int64_t>{5, 200}));
}

TEST_F(SkipIndexTest, AllNullChunkIsAlwaysSkippable) {
    SkipIndex skip_index;
    skip_index.LoadSkipSource(
        field_id_, MakeColumnWithMetrics(field_id_, MakeAllNullMetrics()));

    // An all-null chunk has no matching non-null value: the null state forces
    // the skip decision to true even though the bounds are absent.
    EXPECT_TRUE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 5));
}

TEST_F(SkipIndexTest, RebindingSourceRetiresPreviousMetrics) {
    SkipIndex skip_index;
    skip_index.LoadSkipSource(
        field_id_, MakeColumnWithMetrics(field_id_, MakeIntMetrics(0, 10)));
    ASSERT_TRUE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));

    // Replacing the column must retire the old generation's bounds, not keep
    // them alongside a column whose chunk layout they no longer describe. A
    // replacement that carries no metrics (a Storage V1 column, or a V3 proxy
    // column) therefore has to fail open.
    skip_index.LoadSkipSource(field_id_,
                              MakeColumnWithMetrics(field_id_, nullptr));
    EXPECT_FALSE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));

    // ... and a replacement with different bounds answers from those bounds.
    skip_index.LoadSkipSource(
        field_id_, MakeColumnWithMetrics(field_id_, MakeIntMetrics(15, 25)));
    EXPECT_FALSE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));
    EXPECT_TRUE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 5));
}

TEST_F(SkipIndexTest, CloneAndErase) {
    SkipIndex skip_index;
    skip_index.LoadSkipSource(
        field_id_, MakeColumnWithMetrics(field_id_, MakeIntMetrics(0, 10)));
    ASSERT_TRUE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));

    auto cloned = skip_index.Clone();
    EXPECT_TRUE(
        cloned->CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));

    cloned->Erase(field_id_);
    EXPECT_FALSE(
        cloned->CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));
    // Erasing the clone must not affect the original.
    EXPECT_TRUE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));
}

}  // namespace milvus
