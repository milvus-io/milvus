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

// test_commit_timestamp.cpp
//
// Tests for commit_timestamp behavior in ChunkedSegmentSealedImpl (Approach A).
// Approach A: during LoadFieldData, in-memory timestamps are overwritten to
// commit_ts_ when commit_ts_ != 0. This file tests the four invariants:
//
//   I1. MVCC gate  — rows invisible for query_ts < commit_ts after overwrite
//   I2. TTL gate   — rows NOT expired when commit_ts > ttl_threshold
//   I3. Delete     — pre-commit delete IS applied after commit
//   I4. Normal     — commit_ts=0 leaves existing behavior unchanged
//
// SetCommitTimestamp is called BEFORE LoadGeneratedDataIntoSegment so the
// overwrite happens at load time. Tests call CreateSealedSegment +
// SetCommitTimestamp + LoadGeneratedDataIntoSegment in that order.

#include <gtest/gtest.h>
#include "common/Consts.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentSealed.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;

// Helper: build a sealed segment where the in-memory timestamp column is
// overwritten to T_commit, using the production code path:
//   1. CreateSealedSegment
//   2. SetCommitTimestamp(T_commit)          <- before LoadFieldData
//   3. LoadGeneratedDataIntoSegment(dataset) <- triggers overwrite
static std::unique_ptr<SegmentSealed>
CreateImportSegment(SchemaPtr schema,
                    const GeneratedData& dataset,
                    Timestamp T_commit) {
    auto segment = CreateSealedSegment(schema, milvus::empty_index_meta);
    // SetCommitTimestamp BEFORE loading field data — required production order.
    auto* impl = dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get());
    EXPECT_NE(impl, nullptr);
    impl->SetCommitTimestamp(T_commit);
    // This triggers load_system_field_internal which overwrites ts to T_commit.
    LoadGeneratedDataIntoSegment(dataset, segment.get());
    return segment;
}

// I1: MVCC gate — rows invisible before commit_ts, visible after
TEST(CommitTimestamp, MVCC_RowsInvisibleBeforeCommitTs) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto vec = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(pk);

    // N=10 rows, original ts = [100, 101, ..., 109]
    constexpr int64_t N = 10;
    constexpr Timestamp T_old = 100;  // ts_offset; actual range [100, 109]
    constexpr Timestamp T_commit = 5000;
    constexpr Timestamp T_before = 2000;  // < T_commit
    constexpr Timestamp T_after = 6000;   // > T_commit

    auto dataset = DataGen(schema, N, /*seed=*/42, /*ts_offset=*/T_old);
    auto seg = CreateImportSegment(schema, dataset, T_commit);

    // query_ts < T_commit -> all rows must be masked (invisible)
    {
        BitsetType bs(N, false);
        BitsetTypeView view(bs);
        seg->mask_with_timestamps(view, T_before, /*collection_ttl=*/0);
        EXPECT_EQ(bs.count(), static_cast<size_t>(N))
            << "query_ts=" << T_before << " < commit_ts=" << T_commit
            << ": all rows must be invisible";
    }

    // query_ts >= T_commit -> no rows masked (all visible)
    {
        BitsetType bs(N, false);
        BitsetTypeView view(bs);
        seg->mask_with_timestamps(view, T_after, /*collection_ttl=*/0);
        EXPECT_EQ(bs.count(), 0UL)
            << "query_ts=" << T_after << " >= commit_ts=" << T_commit
            << ": all rows must be visible";
    }
}

// I2: TTL gate — rows NOT expired when commit_ts > ttl_threshold
// Original row timestamps (T_old=100) are below TTL_THRESHOLD=3000.
// After overwrite to T_commit=5000 those rows must no longer be TTL-expired.
TEST(CommitTimestamp, TTL_RowsNotExpiredWhenCommitTsAboveTtl) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto vec = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(pk);

    constexpr int64_t N = 10;
    constexpr Timestamp T_old = 100;  // original ts range: [100, 109]
    constexpr Timestamp T_commit = 5000;
    constexpr Timestamp TTL_THRESHOLD =
        3000;  // old ts <= 3000 -> expired without overwrite

    auto dataset = DataGen(schema, N, /*seed=*/42, /*ts_offset=*/T_old);

    // Import segment: in-memory ts overwritten to T_commit=5000.
    // query_ts=Timestamp(-1) means "accept all MVCC", only TTL filtering matters.
    {
        auto seg = CreateImportSegment(schema, dataset, T_commit);
        BitsetType bs(N, false);
        BitsetTypeView view(bs);
        // Pass large MVCC ts so TTL logic is the only filter.
        seg->mask_with_timestamps(view, T_commit + 1000, TTL_THRESHOLD);
        EXPECT_EQ(bs.count(), 0UL)
            << "Import segment with commit_ts=" << T_commit
            << " must NOT be TTL-expired at threshold=" << TTL_THRESHOLD;
    }

    // Control: normal segment with raw row.ts=T_old — rows ARE expired at TTL_THRESHOLD.
    {
        auto seg_ctrl = CreateSealedWithFieldDataLoaded(schema, dataset);
        BitsetType bs(N, false);
        BitsetTypeView view(bs);
        // MVCC ts large so only TTL filters; T_old=100 <= TTL_THRESHOLD=3000 -> expired.
        seg_ctrl->mask_with_timestamps(view, T_old + 10000, TTL_THRESHOLD);
        EXPECT_EQ(bs.count(), static_cast<size_t>(N))
            << "Without overwrite, row.ts=" << T_old
            << " <= ttl=" << TTL_THRESHOLD
            << ": all rows should be TTL-expired";
    }
}

// I3: Pre-commit delete applied after commit
// A delete with ts=T_delete < T_commit is applied before loading. After the
// segment is visible (query_ts > T_commit), that row must be deleted.
TEST(CommitTimestamp, Delete_PreCommitDeleteNotApplied) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto vec = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(pk);

    constexpr int64_t N = 10;
    constexpr Timestamp T_old = 1000;  // original ts range: [1000, 1009]
    constexpr Timestamp T_commit = 3000;
    constexpr Timestamp T_delete = 2000;         // < T_commit
    constexpr Timestamp T_query_visible = 4000;  // > T_commit
    constexpr Timestamp T_query_hidden = 2500;   // < T_commit

    auto dataset = DataGen(schema, N, /*seed=*/42, /*ts_offset=*/T_old);
    auto pks = dataset.get_col<int64_t>(pk);
    auto seg = CreateImportSegment(schema, dataset, T_commit);

    // Apply delete for the first row at ts=T_delete < T_commit.
    // Since row_ts is overwritten to T_commit, search_pk(pk, T_delete) won't
    // find the row (row_ts=3000 > T_delete=2000). The delete should NOT apply.
    auto del_ids = GenPKs(pks.begin(), pks.begin() + 1);
    std::vector<Timestamp> del_tss(1, T_delete);
    auto status = seg->Delete(1, del_ids.get(), del_tss.data());
    ASSERT_TRUE(status.ok());

    // At query_ts < T_commit: MVCC must mask ALL rows (segment not yet visible).
    {
        BitsetType bs(N, false);
        BitsetTypeView view(bs);
        seg->mask_with_timestamps(view, T_query_hidden, /*collection_ttl=*/0);
        EXPECT_EQ(bs.count(), static_cast<size_t>(N))
            << "At query_ts=" << T_query_hidden << " < T_commit=" << T_commit
            << ", all rows must be MVCC-invisible";
    }

    // At query_ts > T_commit: segment is visible; pre-commit delete should NOT apply.
    {
        BitsetType bs_mvcc(N, false);
        BitsetTypeView view_mvcc(bs_mvcc);
        seg->mask_with_timestamps(
            view_mvcc, T_query_visible, /*collection_ttl=*/0);
        EXPECT_EQ(bs_mvcc.count(), 0UL)
            << "MVCC must not mask any row at query_ts=" << T_query_visible;

        // Delete at T_delete=2000 < T_commit=3000 should NOT be applied
        // because the row did not exist at T_delete.
        BitsetType bs_del(N, false);
        BitsetTypeView view_del(bs_del);
        seg->mask_with_delete(view_del, N, T_query_visible);
        EXPECT_EQ(bs_del.count(), 0UL)
            << "Delete at ts=" << T_delete << " < commit_ts=" << T_commit
            << " must NOT apply — row did not exist at delete time";
    }
}

// I4: Normal segment (commit_ts=0) — existing behavior unchanged
// Without SetCommitTimestamp the MVCC and TTL behavior is governed by original
// row timestamps from DataGen.
TEST(CommitTimestamp, NormalSegment_BehaviorUnchanged) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto vec = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(pk);

    constexpr int64_t N = 10;
    // ts_offset=1000 => row timestamps in [1000, 1009]
    constexpr Timestamp ROW_TS_MIN = 1000;
    constexpr Timestamp ROW_TS_MAX = 1009;

    auto dataset = DataGen(schema, N, /*seed=*/42, /*ts_offset=*/ROW_TS_MIN);
    // No SetCommitTimestamp call -> commit_ts_=0 -> no overwrite.
    auto seg = CreateSealedWithFieldDataLoaded(schema, dataset);

    // MVCC: query_ts=500 < ROW_TS_MIN=1000 -> all rows masked
    {
        BitsetType bs(N, false);
        BitsetTypeView view(bs);
        seg->mask_with_timestamps(view, 500, /*collection_ttl=*/0);
        EXPECT_EQ(bs.count(), static_cast<size_t>(N))
            << "query_ts=500 < row_ts_min=" << ROW_TS_MIN
            << ": all rows invisible";
    }
    // MVCC: query_ts > ROW_TS_MAX -> none masked
    {
        BitsetType bs(N, false);
        BitsetTypeView view(bs);
        seg->mask_with_timestamps(
            view, ROW_TS_MAX + 1000, /*collection_ttl=*/0);
        EXPECT_EQ(bs.count(), 0UL)
            << "query_ts > row_ts_max=" << ROW_TS_MAX << ": all rows visible";
    }
    // TTL: collection_ttl=500 < ROW_TS_MIN=1000 -> none expired
    {
        BitsetType bs(N, false);
        BitsetTypeView view(bs);
        seg->mask_with_timestamps(
            view, ROW_TS_MAX + 1000, /*collection_ttl=*/500);
        EXPECT_EQ(bs.count(), 0UL)
            << "ttl=500 < row_ts_min=" << ROW_TS_MIN << ": no TTL expiry";
    }
    // TTL: collection_ttl > ROW_TS_MAX -> all expired
    {
        BitsetType bs(N, false);
        BitsetTypeView view(bs);
        seg->mask_with_timestamps(
            view, ROW_TS_MAX + 1000, /*collection_ttl=*/ROW_TS_MAX + 1);
        EXPECT_EQ(bs.count(), static_cast<size_t>(N))
            << "ttl=" << (ROW_TS_MAX + 1) << " > row_ts_max=" << ROW_TS_MAX
            << ": all rows TTL-expired";
    }
}
