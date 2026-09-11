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

#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "query/SharedFilterBitsetResult.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentInterface.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

namespace {

constexpr int kDim = 16;
constexpr int kNumRows = 500;
constexpr int kTopK = 10;

struct SharedFilterFixture {
    SchemaPtr schema;
    FieldId vec_a;
    FieldId vec_b;
    FieldId int64_fid;
    std::unique_ptr<SegmentSealed> segment;
    std::unique_ptr<ScopedSchemaHandle> handle;
};

// Two float vector fields over the same rows stand in for the dense and
// BM25/sparse paths of a hybrid search: different ANN targets, one predicate.
SharedFilterFixture
MakeFixture(int64_t segment_id = 0) {
    SharedFilterFixture f;
    f.schema = std::make_shared<Schema>();
    f.vec_a = f.schema->AddDebugField(
        "vec_a", DataType::VECTOR_FLOAT, kDim, knowhere::metric::L2);
    f.vec_b = f.schema->AddDebugField(
        "vec_b", DataType::VECTOR_FLOAT, kDim, knowhere::metric::L2);
    f.int64_fid = f.schema->AddDebugField("int64", DataType::INT64);
    auto pk = f.schema->AddDebugField("pk", DataType::INT64);
    f.schema->set_primary_field_id(pk);

    auto raw = DataGen(f.schema, kNumRows, 42);
    // An explicit id matters for the cross-segment check below: the helper's
    // default is 0 for every segment it makes.
    auto segment = CreateSealedSegment(f.schema, empty_index_meta, segment_id);
    LoadGeneratedDataIntoSegment(raw, segment.get());
    f.segment = std::move(segment);
    f.handle = std::make_unique<ScopedSchemaHandle>(*f.schema);
    return f;
}

template <typename Fixture>
PlanPtr
MakePlan(const Fixture& f,
         const std::string& expr,
         const std::string& vector_field) {
    auto plan_str = f.handle->ParseSearch(expr, vector_field, kTopK, "L2");
    return CreateSearchPlanByExpr(f.schema, plan_str.data(), plan_str.size());
}

// Same seed => same query vectors, so a plain search and a shared-bitset
// search are comparable result for result.
std::unique_ptr<milvus::query::PlaceholderGroup>
MakePlaceholder(const Plan* plan, int seed) {
    auto raw = CreatePlaceholderGroup(1, kDim, seed);
    return ParsePlaceholderGroup(plan, raw.SerializeAsString());
}

// `allow_empty` is only for the deliberately-empty predicate case: everywhere
// else two empty results would compare equal and prove nothing, so the
// non-empty check is what keeps the differential honest.
void
ExpectSameSearchResult(const SearchResult& expected,
                       const SearchResult& actual,
                       const std::string& what,
                       bool allow_empty = false) {
    if (!allow_empty) {
        ASSERT_FALSE(expected.seg_offsets_.empty())
            << what << ": fixture produced no hits, the comparison would be "
                       "vacuous";
    }
    ASSERT_EQ(expected.total_nq_, actual.total_nq_) << what;
    ASSERT_EQ(expected.unity_topK_, actual.unity_topK_) << what;
    ASSERT_EQ(expected.seg_offsets_.size(), actual.seg_offsets_.size()) << what;
    ASSERT_EQ(expected.distances_.size(), actual.distances_.size()) << what;
    for (size_t i = 0; i < expected.seg_offsets_.size(); ++i) {
        EXPECT_EQ(expected.seg_offsets_[i], actual.seg_offsets_[i])
            << what << " offset " << i;
        EXPECT_FLOAT_EQ(expected.distances_[i], actual.distances_[i])
            << what << " distance " << i;
    }
}


struct GrowingFixture {
    SchemaPtr schema;
    FieldId vec_a;
    FieldId vec_b;
    FieldId int64_fid;
    SegmentGrowingPtr segment;
    SegmentGrowingImpl* impl{nullptr};
    std::unique_ptr<ScopedSchemaHandle> handle;
};

// The same two-vector shape on a growing segment. Growing is where the
// active_count contract actually has teeth: the row set is bounded by the
// query timestamp on every call instead of being fixed when the segment loads.
GrowingFixture
MakeGrowingFixture() {
    GrowingFixture f;
    f.schema = std::make_shared<Schema>();
    f.vec_a = f.schema->AddDebugField(
        "vec_a", DataType::VECTOR_FLOAT, kDim, knowhere::metric::L2);
    f.vec_b = f.schema->AddDebugField(
        "vec_b", DataType::VECTOR_FLOAT, kDim, knowhere::metric::L2);
    f.int64_fid = f.schema->AddDebugField("int64", DataType::INT64);
    auto pk = f.schema->AddDebugField("pk", DataType::INT64);
    f.schema->set_primary_field_id(pk);

    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(128);
    // Brute force only: an interim index would make the two sides of the
    // differential incomparable for reasons unrelated to the shared bitset.
    config.set_enable_interim_segment_index(false);
    f.segment = CreateGrowingSegment(f.schema, nullptr, /*segment_id=*/0, config);
    f.impl = dynamic_cast<SegmentGrowingImpl*>(f.segment.get());
    f.handle = std::make_unique<ScopedSchemaHandle>(*f.schema);
    return f;
}

// Stamp a whole batch at one timestamp so a query timestamp either includes
// all of it or none of it.
void
InsertBatch(GrowingFixture& f, int64_t count, Timestamp ts, uint64_t seed) {
    auto data = DataGen(f.schema, count, seed);
    std::vector<Timestamp> tss(count, ts);
    auto offset = f.impl->PreInsert(count);
    f.impl->Insert(
        offset, count, data.row_ids_.data(), tss.data(), data.raw_);
}

}  // namespace

// The property the whole design rests on: reusing a shared bitset must give
// byte-for-byte what an independent search gives. Anything else is a silent
// wrong-results bug, so this is an equality assertion, not a similarity check.
TEST(SharedFilterBitset, MatchesIndependentSearch) {
    auto f = MakeFixture();
    const std::vector<std::string> exprs = {
        "int64 >= 0",         // matches everything: all_rows_visible fast path
        "int64 > 100",        // ordinary partial match
        "int64 < 0",          // matches nothing
    };

    for (const auto& expr : exprs) {
        SCOPED_TRACE("expr: " + expr);
        auto plan_a = MakePlan(f, expr, "vec_a");
        auto plan_b = MakePlan(f, expr, "vec_b");
        auto ph_a = MakePlaceholder(plan_a.get(), 1024);
        auto ph_b = MakePlaceholder(plan_b.get(), 2048);

        auto want_a = f.segment->Search(plan_a.get(), ph_a.get(), MAX_TIMESTAMP);
        auto want_b = f.segment->Search(plan_b.get(), ph_b.get(), MAX_TIMESTAMP);

        // One filter evaluation, then both branches against it.
        auto bitset = f.segment->ComputeFilterBitset(plan_a.get(),
                                                     MAX_TIMESTAMP,
                                                     folly::CancellationToken(),
                                                     0,
                                                     0);
        ASSERT_NE(bitset, nullptr);
        EXPECT_EQ(bitset->segment_id, f.segment->get_segment_id());

        auto got_a = f.segment->SearchWithBitset(plan_a.get(),
                                                 ph_a.get(),
                                                 bitset.get(),
                                                 MAX_TIMESTAMP,
                                                 folly::CancellationToken(),
                                                 0,
                                                 0);
        auto got_b = f.segment->SearchWithBitset(plan_b.get(),
                                                 ph_b.get(),
                                                 bitset.get(),
                                                 MAX_TIMESTAMP,
                                                 folly::CancellationToken(),
                                                 0,
                                                 0);
        const bool empty_by_design = expr == "int64 < 0";
        ExpectSameSearchResult(*want_a, *got_a, "branch a", empty_by_design);
        ExpectSameSearchResult(*want_b, *got_b, "branch b", empty_by_design);
    }
}

// The bitset is handed to every branch unchanged, so a branch must not be able
// to disturb what its siblings see. Running the same branch twice off one
// bitset would diverge if phase 2 mutated it.
TEST(SharedFilterBitset, BitsetIsReusableAcrossBranches) {
    auto f = MakeFixture();
    auto plan = MakePlan(f, "int64 > 100", "vec_a");
    auto ph = MakePlaceholder(plan.get(), 1024);

    auto bitset = f.segment->ComputeFilterBitset(
        plan.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0);
    ASSERT_NE(bitset, nullptr);

    auto first = f.segment->SearchWithBitset(plan.get(),
                                             ph.get(),
                                             bitset.get(),
                                             MAX_TIMESTAMP,
                                             folly::CancellationToken(),
                                             0,
                                             0);
    auto second = f.segment->SearchWithBitset(plan.get(),
                                              ph.get(),
                                              bitset.get(),
                                              MAX_TIMESTAMP,
                                              folly::CancellationToken(),
                                              0,
                                              0);
    ExpectSameSearchResult(*first, *second, "reused bitset");
}

// The derived query state travels with the bitset, and getting that wrong is
// the failure mode this design is most exposed to. all_rows_visible is the
// observable half: MvccNode only sets it when it is the source node -- that is,
// when the plan carries no predicate at all -- so it distinguishes the two
// shapes cleanly.
TEST(SharedFilterBitset, CarriesDerivedQueryState) {
    auto f = MakeFixture();

    auto filtered = MakePlan(f, "int64 >= 0", "vec_a");
    auto with_filter = f.segment->ComputeFilterBitset(
        filtered.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0);
    ASSERT_NE(with_filter, nullptr);
    EXPECT_EQ(with_filter->active_count, kNumRows);
    EXPECT_EQ(with_filter->segment_id, f.segment->get_segment_id());
    EXPECT_FALSE(with_filter->bitset_is_element_level);
    // FilterBitsNode is the source here, so MvccNode cannot take its fast path
    // even though this predicate matches every row.
    EXPECT_FALSE(with_filter->all_rows_visible);

    auto unfiltered = MakePlan(f, "", "vec_a");
    auto without_filter = f.segment->ComputeFilterBitset(
        unfiltered.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0);
    ASSERT_NE(without_filter, nullptr);
    EXPECT_TRUE(without_filter->all_rows_visible)
        << "with no predicate MvccNode is the source and must report "
           "all_rows_visible, which the branch contexts rely on to skip the "
           "BitsetView entirely";
}

// Handing a branch a bitset computed on another segment is an internal
// invariant violation, and one of the two O(1) checks phase 2 keeps.
TEST(SharedFilterBitset, RejectsBitsetFromAnotherSegment) {
    auto f = MakeFixture(/*segment_id=*/1);
    auto other = MakeFixture(/*segment_id=*/2);
    auto plan = MakePlan(f, "int64 > 100", "vec_a");
    auto ph = MakePlaceholder(plan.get(), 1024);

    auto foreign = other.segment->ComputeFilterBitset(
        plan.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0);
    ASSERT_NE(foreign, nullptr);

    EXPECT_ANY_THROW(f.segment->SearchWithBitset(plan.get(),
                                                 ph.get(),
                                                 foreign.get(),
                                                 MAX_TIMESTAMP,
                                                 folly::CancellationToken(),
                                                 0,
                                                 0));
}

// Sealed segments fix their row set at load, so the sealed cases above never
// exercise a timestamp-bounded active_count. Growing does.
TEST(SharedFilterBitset, GrowingSegmentMatchesIndependentSearch) {
    auto f = MakeGrowingFixture();
    InsertBatch(f, kNumRows, /*ts=*/100, /*seed=*/42);
    const Timestamp query_ts = 1000;

    auto plan_a = MakePlan(f, "int64 > 100", "vec_a");
    auto plan_b = MakePlan(f, "int64 > 100", "vec_b");
    auto ph_a = MakePlaceholder(plan_a.get(), 1024);
    auto ph_b = MakePlaceholder(plan_b.get(), 2048);

    auto want_a = f.segment->Search(plan_a.get(), ph_a.get(), query_ts);
    auto want_b = f.segment->Search(plan_b.get(), ph_b.get(), query_ts);

    auto bitset = f.segment->ComputeFilterBitset(
        plan_a.get(), query_ts, folly::CancellationToken(), 0, 0);
    ASSERT_NE(bitset, nullptr);
    EXPECT_EQ(bitset->active_count, kNumRows);

    auto got_a = f.segment->SearchWithBitset(plan_a.get(),
                                             ph_a.get(),
                                             bitset.get(),
                                             query_ts,
                                             folly::CancellationToken(),
                                             0,
                                             0);
    auto got_b = f.segment->SearchWithBitset(plan_b.get(),
                                             ph_b.get(),
                                             bitset.get(),
                                             query_ts,
                                             folly::CancellationToken(),
                                             0,
                                             0);
    ExpectSameSearchResult(*want_a, *got_a, "growing branch a");
    ExpectSameSearchResult(*want_b, *got_b, "growing branch b");
}

// Raised in review: a growing segment can take writes between the two phases,
// so can phase 2 end up searching rows the bitset does not cover?
//
// It cannot, and this pins down why. Both phases resolve active_count from the
// same MVCC timestamp, and get_active_count is an upper_bound over the
// timestamp vector, so a batch stamped later is outside the snapshot for both.
// The branch therefore reuses the bitset -- the count guard does not fire --
// and lands on exactly what an ordinary search at that timestamp returns.
TEST(SharedFilterBitset, GrowingInsertsBetweenPhasesStayInvisible) {
    auto f = MakeGrowingFixture();
    InsertBatch(f, kNumRows, /*ts=*/100, /*seed=*/42);
    const Timestamp query_ts = 1000;

    auto plan = MakePlan(f, "int64 > 100", "vec_a");
    auto ph = MakePlaceholder(plan.get(), 1024);

    // Phase 1 runs while the segment holds only the first batch.
    auto bitset = f.segment->ComputeFilterBitset(
        plan.get(), query_ts, folly::CancellationToken(), 0, 0);
    ASSERT_NE(bitset, nullptr);
    ASSERT_EQ(bitset->active_count, kNumRows);

    // A concurrent writer lands rows stamped after the query timestamp.
    InsertBatch(f, 200, /*ts=*/2000, /*seed=*/7);
    EXPECT_EQ(f.segment->get_row_count(), kNumRows + 200);
    EXPECT_EQ(f.segment->get_active_count(query_ts), kNumRows)
        << "rows newer than the pinned timestamp must stay outside the "
           "snapshot both phases see";

    // Phase 2 still matches a plain search at the same timestamp.
    auto want = f.segment->Search(plan.get(), ph.get(), query_ts);
    auto got = f.segment->SearchWithBitset(plan.get(),
                                           ph.get(),
                                           bitset.get(),
                                           query_ts,
                                           folly::CancellationToken(),
                                           0,
                                           0);
    ExpectSameSearchResult(*want, *got, "after a concurrent insert");
}

// group_by puts a SearchGroupByNode above the vector search, making it the
// fork point RebindToPrecomputedBitset has to rewrite instead of
// VectorSearchNode. That branch of the rewrite is otherwise untested.
TEST(SharedFilterBitset, GroupBySearchMatchesIndependentSearch) {
    auto f = MakeFixture();
    auto group_plan = [&](const std::string& vector_field) {
        auto bytes = f.handle->ParseGroupBySearch("int64 > 100",
                                                  vector_field,
                                                  kTopK,
                                                  "L2",
                                                  "{}",
                                                  f.int64_fid.get(),
                                                  /*group_size=*/2);
        return CreateSearchPlanByExpr(f.schema, bytes.data(), bytes.size());
    };

    auto plan_a = group_plan("vec_a");
    auto plan_b = group_plan("vec_b");
    auto ph_a = MakePlaceholder(plan_a.get(), 1024);
    auto ph_b = MakePlaceholder(plan_b.get(), 2048);

    auto want_a = f.segment->Search(plan_a.get(), ph_a.get(), MAX_TIMESTAMP);
    auto want_b = f.segment->Search(plan_b.get(), ph_b.get(), MAX_TIMESTAMP);

    auto bitset = f.segment->ComputeFilterBitset(
        plan_a.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0);
    ASSERT_NE(bitset, nullptr);

    auto got_a = f.segment->SearchWithBitset(plan_a.get(),
                                             ph_a.get(),
                                             bitset.get(),
                                             MAX_TIMESTAMP,
                                             folly::CancellationToken(),
                                             0,
                                             0);
    auto got_b = f.segment->SearchWithBitset(plan_b.get(),
                                             ph_b.get(),
                                             bitset.get(),
                                             MAX_TIMESTAMP,
                                             folly::CancellationToken(),
                                             0,
                                             0);
    ExpectSameSearchResult(*want_a, *got_a, "group_by branch a");
    ExpectSameSearchResult(*want_b, *got_b, "group_by branch b");
}

// Element-level filtering is the one shape where the shared bitset is not a
// row bitset: ElementFilterBitsNode expands it to elements and writes
// bitset_is_element_level / active_element_count / array_offsets onto the
// context. Those are exactly the derived-state fields SharedFilterBitsetResult
// has to carry to every branch, so this is the case that would break first if
// CaptureFrom or ApplyTo missed one.
TEST(SharedFilterBitset, ElementLevelMatchesIndependentSearch) {
    constexpr int kArrayLen = 3;
    const std::string metric = knowhere::metric::L2;

    auto schema = std::make_shared<Schema>();
    schema->AddDebugVectorArrayField(
        "structA[array_vec_a]", DataType::VECTOR_FLOAT, kDim, metric);
    schema->AddDebugVectorArrayField(
        "structA[array_vec_b]", DataType::VECTOR_FLOAT, kDim, metric);
    schema->AddDebugArrayField("structA[price_array]", DataType::INT32, false);
    auto pk = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk);

    auto raw = DataGen(schema, kNumRows, 42, 0, 1, kArrayLen);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw);
    ScopedSchemaHandle handle(*schema);

    const std::string expr = "element_filter(structA, $[price_array] > 10)";
    auto make = [&](const std::string& vector_field) {
        auto bytes = handle.ParseSearch(
            expr, vector_field, kTopK, metric, R"({"ef": 50})", 3);
        return CreateSearchPlanByExpr(schema, bytes.data(), bytes.size());
    };

    auto plan_a = make("structA[array_vec_a]");
    auto plan_b = make("structA[array_vec_b]");
    auto ph_a = MakePlaceholder(plan_a.get(), 1024);
    auto ph_b = MakePlaceholder(plan_b.get(), 2048);

    auto want_a = segment->Search(plan_a.get(), ph_a.get(), MAX_TIMESTAMP);
    auto want_b = segment->Search(plan_b.get(), ph_b.get(), MAX_TIMESTAMP);

    auto bitset = segment->ComputeFilterBitset(
        plan_a.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0);
    ASSERT_NE(bitset, nullptr);
    EXPECT_TRUE(bitset->bitset_is_element_level)
        << "an element_filter must produce an element-level bitset, otherwise "
           "the branch contexts take the row-level path";
    EXPECT_GT(bitset->active_element_count, 0);
    EXPECT_NE(bitset->array_offsets, nullptr)
        << "array_offsets must travel with the bitset; without it a branch "
           "cannot map elements back to rows";

    auto got_a = segment->SearchWithBitset(plan_a.get(),
                                           ph_a.get(),
                                           bitset.get(),
                                           MAX_TIMESTAMP,
                                           folly::CancellationToken(),
                                           0,
                                           0);
    auto got_b = segment->SearchWithBitset(plan_b.get(),
                                           ph_b.get(),
                                           bitset.get(),
                                           MAX_TIMESTAMP,
                                           folly::CancellationToken(),
                                           0,
                                           0);
    ExpectSameSearchResult(*want_a, *got_a, "element-level branch a");
    ExpectSameSearchResult(*want_b, *got_b, "element-level branch b");
}

// The snapshot guard, and the reason active_count alone is not it.
//
// On a sealed segment `get_active_count` returns `get_row_count()` and ignores
// the timestamp (ChunkedSegmentSealedImpl.cpp:6500), so two different MVCC
// snapshots of the same sealed segment report the same active_count. A bitset
// built before a delete therefore passes an active-count-only check while
// describing rows the later snapshot no longer sees -- it would resurrect
// deleted rows into the branch's candidate set.
//
// No production path constructs this call: the hybrid fan-out is the only
// caller of SearchWithBitset, and internal.SubSearchRequest has no timestamp
// field, so every branch inherits the parent's. The test reaches past the Go
// layer on purpose -- it pins the behaviour of the segcore API itself, so that
// a future second caller fails loudly instead of returning deleted rows. It is
// a guard test, not a regression test for a bug that shipped.
TEST(SharedFilterBitset, RejectsBitsetFromAnotherSnapshotOnSealed) {
    auto f = MakeFixture();
    auto plan = MakePlan(f, "int64 > 100", "vec_a");
    auto ph = MakePlaceholder(plan.get(), 1024);

    const Timestamp before_delete = 1000;
    const Timestamp after_delete = 3000;

    auto bitset = f.segment->ComputeFilterBitset(
        plan.get(), before_delete, folly::CancellationToken(), 0, 0);
    ASSERT_NE(bitset, nullptr);
    ASSERT_EQ(bitset->timestamp, before_delete);

    // Retire most of the rows at a timestamp between the two snapshots.
    const int64_t deleted = kNumRows * 3 / 5;
    auto tss = GenTss(deleted, /*begin_ts=*/2000);
    auto pks = GenPKs(deleted, /*begin_pk=*/0);
    ASSERT_TRUE(f.segment->Delete(deleted, pks.get(), tss.data()).ok());

    // The count check cannot tell the two snapshots apart -- this is exactly
    // the hole the timestamp/TTL compares fill.
    ASSERT_EQ(f.segment->get_active_count(after_delete), bitset->active_count);

    auto stale = f.segment->Search(plan.get(), ph.get(), before_delete);
    auto fresh = f.segment->Search(plan.get(), ph.get(), after_delete);
    ASSERT_NE(stale->seg_offsets_, fresh->seg_offsets_)
        << "precondition: the delete must change the result, otherwise this "
           "test cannot tell a reused bitset from a rejected one";

    // Reusing the stale bitset would return `stale`. The guard must degrade to
    // evaluating this branch's own filter, which returns `fresh`.
    auto got = f.segment->SearchWithBitset(plan.get(),
                                           ph.get(),
                                           bitset.get(),
                                           after_delete,
                                           folly::CancellationToken(),
                                           0,
                                           0);
    ExpectSameSearchResult(*fresh, *got, "stale-snapshot bitset must be refused");
}
