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
#include <cstring>
#include "cachinglayer/Manager.h"
#include "common/Chunk.h"
#include "common/Consts.h"
#include "common/FieldMeta.h"
#include "mmap/ChunkedColumn.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentSealed.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
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

// Boundary: commit_ts == max(row_ts). Overwrite is a no-op semantically; MVCC
// and TTL behavior is identical to a normal segment with that row_ts.
// This documents the boundary contract enforced by the Go-side
// UpdateCommitTimestamp operator (commit_ts >= max(binlog.TimestampTo)).
TEST(CommitTimestamp, Boundary_CommitEqualsMaxRowTs) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto vec = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(pk);

    constexpr int64_t N = 10;
    constexpr Timestamp T_old = 1000;     // row ts range: [1000, 1009]
    constexpr Timestamp T_commit = 1009;  // == max(row_ts)

    auto dataset = DataGen(schema, N, /*seed=*/42, /*ts_offset=*/T_old);
    auto seg = CreateImportSegment(schema, dataset, T_commit);

    // query_ts < T_commit -> all rows masked (invisible).
    {
        BitsetType bs(N, false);
        BitsetTypeView view(bs);
        seg->mask_with_timestamps(view, T_commit - 1, /*collection_ttl=*/0);
        EXPECT_EQ(bs.count(), static_cast<size_t>(N))
            << "query_ts=" << (T_commit - 1) << " < commit_ts=" << T_commit
            << ": all rows must be invisible";
    }

    // query_ts == T_commit -> all rows visible (MVCC is inclusive on ==).
    {
        BitsetType bs(N, false);
        BitsetTypeView view(bs);
        seg->mask_with_timestamps(view, T_commit, /*collection_ttl=*/0);
        EXPECT_EQ(bs.count(), 0UL)
            << "query_ts=" << T_commit << " == commit_ts: all rows visible";
    }
}

// ===========================================================================
// V2/V3 column-group fixture
//
// On v2/v3 import segments the raw timestamp column (original row_ts) is
// emplaced into fields_ by load_field_data_common, while the timestamp index
// is built from commit_ts via init_storage_v1_timestamp_index. The V1 fixture
// above (CreateImportSegment / LoadGeneratedDataIntoSegment) never reproduces
// this state because the V1 path stores timestamps in insert_record_ instead
// of fields_. CommitTimestampV2TestAccess pokes the private fields_ map to
// simulate exactly the v2/v3 shape, so we can verify that EffectiveCommitTs()
// short-circuits every timestamp reader regardless of which storage path
// populated fields_.
// ===========================================================================

namespace milvus::segcore {

class CommitTimestampV2TestAccess {
 public:
    // Emplace a ChunkedColumn at TimestampFieldID that points at `buf`,
    // mirroring load_field_data_common on the v2/v3 column-group path. Caller
    // must have already run CreateImportSegment so insert_record_.timestamps_
    // and the timestamp index already carry commit_ts — this method only
    // injects the *raw* column whose presence used to bypass the overwrite.
    //
    // `buf` must outlive `segment`: the FixedWidthChunk stores a raw pointer
    // into it and ChunkMmapGuard does not manage heap allocations.
    static void
    InjectRawTimestampColumn(ChunkedSegmentSealedImpl* segment,
                             char* buf,
                             int32_t n_rows) {
        static const FieldMeta ts_field_meta(FieldName("Timestamp"),
                                             TimestampFieldID,
                                             DataType::INT64,
                                             /*nullable=*/false,
                                             /*default_value=*/std::nullopt);

        const auto buf_size = static_cast<uint64_t>(n_rows) * sizeof(Timestamp);
        auto mmap_guard = std::make_shared<ChunkMmapGuard>(nullptr, 0, "");

        std::vector<std::unique_ptr<Chunk>> chunks;
        chunks.emplace_back(
            std::make_unique<FixedWidthChunk>(n_rows,
                                              /*dim=*/1,
                                              buf,
                                              buf_size,
                                              sizeof(Timestamp),
                                              /*nullable=*/false,
                                              mmap_guard));

        auto translator = std::make_unique<TestChunkTranslator>(
            std::vector<int64_t>{n_rows}, "ts_v2_test", std::move(chunks));
        auto slot =
            cachinglayer::Manager::GetInstance().CreateCacheSlot<milvus::Chunk>(
                std::move(translator), nullptr);
        auto column =
            std::make_shared<ChunkedColumn>(std::move(slot), ts_field_meta);
        segment->fields_.wlock()->insert_or_assign(TimestampFieldID, column);
    }
};

}  // namespace milvus::segcore

using milvus::segcore::CommitTimestampV2TestAccess;

// Holds a sealed segment in v2/v3-shape together with the raw timestamp buffer
// it points at. The chunk inside `segment` references `ts_buf.data()`, so
// `segment` must be destroyed before `ts_buf`. Members are destroyed in reverse
// declaration order, so `ts_buf` is declared first (destroyed last).
struct ImportSegmentV2Handle {
    std::vector<char> ts_buf;
    std::unique_ptr<SegmentSealed> segment;
};

static ImportSegmentV2Handle
CreateImportSegmentV2(SchemaPtr schema,
                      const GeneratedData& dataset,
                      Timestamp T_commit,
                      Timestamp T_old) {
    ImportSegmentV2Handle handle;
    handle.segment = CreateImportSegment(schema, dataset, T_commit);
    auto* impl = dynamic_cast<ChunkedSegmentSealedImpl*>(handle.segment.get());
    EXPECT_NE(impl, nullptr);

    // DataGen emits ts_offset, ts_offset+1, ... — reproduce that raw shape.
    const auto n = static_cast<int32_t>(dataset.row_ids_.size());
    handle.ts_buf.resize(static_cast<size_t>(n) * sizeof(Timestamp));
    auto* ts_view = reinterpret_cast<Timestamp*>(handle.ts_buf.data());
    for (int32_t i = 0; i < n; ++i) {
        ts_view[i] = T_old + static_cast<Timestamp>(i);
    }
    CommitTimestampV2TestAccess::InjectRawTimestampColumn(
        impl, handle.ts_buf.data(), n);
    return handle;
}

// V2.1: MVCC — query_ts < commit_ts must mask every row, even though the
// raw timestamp column in fields_ would say the rows are old enough to be
// visible. This was the latent gap: mask_with_timestamps::do_scan used to
// scan the raw column inside the index-narrowed range.
TEST(CommitTimestamp, V2_MVCC_RowsInvisibleBeforeCommitTs) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(pk);

    constexpr int64_t N = 10;
    constexpr Timestamp T_old = 100;
    constexpr Timestamp T_commit = 5000;
    constexpr Timestamp T_before = 2000;
    constexpr Timestamp T_after = 6000;

    auto dataset = DataGen(schema, N, /*seed=*/42, /*ts_offset=*/T_old);
    auto handle = CreateImportSegmentV2(schema, dataset, T_commit, T_old);
    auto& seg = handle.segment;

    {
        BitsetType bs(N, false);
        BitsetTypeView view(bs);
        seg->mask_with_timestamps(view, T_before, /*collection_ttl=*/0);
        EXPECT_EQ(bs.count(), static_cast<size_t>(N))
            << "v2: query_ts=" << T_before << " < commit_ts=" << T_commit
            << ": all rows must be invisible";
    }
    {
        BitsetType bs(N, false);
        BitsetTypeView view(bs);
        seg->mask_with_timestamps(view, T_after, /*collection_ttl=*/0);
        EXPECT_EQ(bs.count(), 0UL)
            << "v2: query_ts=" << T_after << " >= commit_ts=" << T_commit
            << ": all rows must be visible";
    }
}

// V2.2: TTL — raw row_ts (T_old) is below TTL_THRESHOLD, but commit_ts is
// above it, so no row may be TTL-expired. This is the case the reviewer
// explicitly called out: do_scan would read the raw column and erroneously
// mark rows as expired.
TEST(CommitTimestamp, V2_TTL_RowsNotExpiredWhenCommitTsAboveTtl) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(pk);

    constexpr int64_t N = 10;
    constexpr Timestamp T_old = 100;
    constexpr Timestamp T_commit = 5000;
    constexpr Timestamp TTL_THRESHOLD = 3000;

    auto dataset = DataGen(schema, N, /*seed=*/42, /*ts_offset=*/T_old);
    auto handle = CreateImportSegmentV2(schema, dataset, T_commit, T_old);
    auto& seg = handle.segment;

    BitsetType bs(N, false);
    BitsetTypeView view(bs);
    seg->mask_with_timestamps(view, T_commit + 1000, TTL_THRESHOLD);
    EXPECT_EQ(bs.count(), 0UL)
        << "v2: import segment with commit_ts=" << T_commit
        << " must NOT be TTL-expired at threshold=" << TTL_THRESHOLD
        << " even with raw row_ts=" << T_old << " in fields_";
}

// V2.3: Pre-commit delete must not apply on v2 import segments. The bug was
// that search_batch_pks::read_ts read from the raw column and saw row_ts
// (less than del_ts), letting the delete callback fire.
TEST(CommitTimestamp, V2_Delete_PreCommitDeleteNotApplied) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(pk);

    constexpr int64_t N = 10;
    constexpr Timestamp T_old = 1000;
    constexpr Timestamp T_commit = 3000;
    constexpr Timestamp T_delete = 2000;
    constexpr Timestamp T_query_visible = 4000;

    auto dataset = DataGen(schema, N, /*seed=*/42, /*ts_offset=*/T_old);
    auto pks = dataset.get_col<int64_t>(pk);
    auto handle = CreateImportSegmentV2(schema, dataset, T_commit, T_old);
    auto& seg = handle.segment;

    auto del_ids = GenPKs(pks.begin(), pks.begin() + 1);
    std::vector<Timestamp> del_tss(1, T_delete);
    auto status = seg->Delete(1, del_ids.get(), del_tss.data());
    ASSERT_TRUE(status.ok());

    BitsetType bs_del(N, false);
    BitsetTypeView view_del(bs_del);
    seg->mask_with_delete(view_del, N, T_query_visible);
    EXPECT_EQ(bs_del.count(), 0UL)
        << "v2: delete at ts=" << T_delete << " < commit_ts=" << T_commit
        << " must NOT apply on a column-group import segment";
}

// V2.4: bulk_subscript(Timestamp) must return commit_ts for every requested
// offset, not the raw row_ts that lives in the column. Used by retrieval
// and any caller that materialises the system timestamp field.
TEST(CommitTimestamp, V2_BulkSubscript_ReturnsCommitTs) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(pk);

    constexpr int64_t N = 10;
    constexpr Timestamp T_old = 100;
    constexpr Timestamp T_commit = 5000;

    auto dataset = DataGen(schema, N, /*seed=*/42, /*ts_offset=*/T_old);
    auto handle = CreateImportSegmentV2(schema, dataset, T_commit, T_old);
    auto& seg = handle.segment;

    std::vector<int64_t> offsets = {0, 1, 3, 5, 9};
    std::vector<Timestamp> out(offsets.size(), 0);
    seg->bulk_subscript(/*op_ctx=*/nullptr,
                        SystemFieldType::Timestamp,
                        offsets.data(),
                        static_cast<int64_t>(offsets.size()),
                        out.data());
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_EQ(out[i], T_commit)
            << "v2: bulk_subscript at offset " << offsets[i]
            << " must return commit_ts=" << T_commit << ", got " << out[i];
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
