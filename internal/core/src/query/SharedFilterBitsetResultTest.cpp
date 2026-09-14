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
         const std::string& vector_field,
         const std::string& search_params = "{}") {
    auto plan_str =
        f.handle->ParseSearch(expr, vector_field, kTopK, "L2", search_params);
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
            << what
            << ": fixture produced no hits, the comparison would be "
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
    f.segment =
        CreateGrowingSegment(f.schema, nullptr, /*segment_id=*/0, config);
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
    f.impl->Insert(offset, count, data.row_ids_.data(), tss.data(), data.raw_);
}

}  // namespace

// The property the whole design rests on: reusing a shared bitset must give
// byte-for-byte what an independent search gives. Anything else is a silent
// wrong-results bug, so this is an equality assertion, not a similarity check.
TEST(SharedFilterBitset, MatchesIndependentSearch) {
    auto f = MakeFixture();
    const std::vector<std::string> exprs = {
        "int64 >= 0",   // matches every row
        "int64 > 100",  // ordinary partial match
        "int64 < 0",    // matches nothing
    };

    for (const auto& expr : exprs) {
        SCOPED_TRACE("expr: " + expr);
        auto plan_a = MakePlan(f, expr, "vec_a");
        auto plan_b = MakePlan(f, expr, "vec_b");
        auto ph_a = MakePlaceholder(plan_a.get(), 1024);
        auto ph_b = MakePlaceholder(plan_b.get(), 2048);

        auto want_a =
            f.segment->Search(plan_a.get(), ph_a.get(), MAX_TIMESTAMP);
        auto want_b =
            f.segment->Search(plan_b.get(), ph_b.get(), MAX_TIMESTAMP);

        // One filter evaluation, then both branches against it.
        auto bitset = f.segment->ComputeFilterBitset(
            plan_a.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0);
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

// Phase 1 records the segment identity and visible-row bound alongside the
// bitset.
//
// The second half covers the one plan shape where the prefix's MvccNode *is*
// the source node and therefore writes query state (all_rows_visible): a
// search with no predicate. A grouped request can never have that shape -- the
// delegator only groups sub-requests that carry a predicate -- but the segcore
// API accepts it, and the result must still match an independent search
// without any context state being carried across the two phases.
TEST(SharedFilterBitset, CarriesSharedFilterMetadata) {
    auto f = MakeFixture(/*segment_id=*/7);

    auto filtered = MakePlan(f, "int64 >= 0", "vec_a");
    auto with_filter = f.segment->ComputeFilterBitset(
        filtered.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0);
    ASSERT_NE(with_filter, nullptr);
    EXPECT_EQ(with_filter->active_count, kNumRows);
    EXPECT_EQ(with_filter->segment_id, f.segment->get_segment_id());

    auto unfiltered = MakePlan(f, "", "vec_a");
    auto ph = MakePlaceholder(unfiltered.get(), 1024);
    auto without_filter = f.segment->ComputeFilterBitset(
        unfiltered.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0);
    ASSERT_NE(without_filter, nullptr);
    ASSERT_NE(without_filter->bitset, nullptr);

    auto want = f.segment->Search(unfiltered.get(), ph.get(), MAX_TIMESTAMP);
    auto got = f.segment->SearchWithBitset(unfiltered.get(),
                                           ph.get(),
                                           without_filter.get(),
                                           MAX_TIMESTAMP,
                                           folly::CancellationToken(),
                                           0,
                                           0);
    ExpectSameSearchResult(*want, *got, "predicate-less branch");
}

// The rebound plan tree -- this branch's chain with its filter prefix replaced
// by a PrecomputedBitsetNode -- is a property of the branch, not of the
// segment: it carries no bitset, so every segment of the search runs the same
// one. Building it inside the segment fan-out would rebuild an identical tree
// (and burn plan-node ids) once per segment, so it is cached on the plan and
// this pins that down.
TEST(SharedFilterBitset, ReboundPlanIsBuiltOncePerPlan) {
    auto first = MakeFixture(/*segment_id=*/1);
    // Same seed, so the two segments hold the same rows; each is still
    // searched against its own bitset.
    auto second = MakeFixture(/*segment_id=*/2);

    auto plan = MakePlan(first, "int64 > 100", "vec_a");
    auto ph = MakePlaceholder(plan.get(), 1024);
    auto* vector_node = plan->plan_node_.get();

    auto want_first =
        first.segment->Search(plan.get(), ph.get(), MAX_TIMESTAMP);
    auto want_second =
        second.segment->Search(plan.get(), ph.get(), MAX_TIMESTAMP);
    auto bitset_first = first.segment->ComputeFilterBitset(
        plan.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0);
    auto bitset_second = second.segment->ComputeFilterBitset(
        plan.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0);
    ASSERT_NE(bitset_first, nullptr);
    ASSERT_NE(bitset_second, nullptr);
    auto got_first = first.segment->SearchWithBitset(plan.get(),
                                                     ph.get(),
                                                     bitset_first.get(),
                                                     MAX_TIMESTAMP,
                                                     folly::CancellationToken(),
                                                     0,
                                                     0);
    const auto* rebound = vector_node->shared_filter_plannodes().get();
    ASSERT_NE(rebound, nullptr);

    auto got_second =
        second.segment->SearchWithBitset(plan.get(),
                                         ph.get(),
                                         bitset_second.get(),
                                         MAX_TIMESTAMP,
                                         folly::CancellationToken(),
                                         0,
                                         0);
    EXPECT_EQ(vector_node->shared_filter_plannodes().get(), rebound)
        << "the second segment must reuse the tree the first one built";

    ExpectSameSearchResult(*want_first, *got_first, "segment 1");
    ExpectSameSearchResult(*want_second, *got_second, "segment 2");
}

// Phase 1 represents "nothing visible at this timestamp" as a null bitset.
// That sentinel is not a snapshot mismatch: phase 2 returns an empty result at
// active_count == 0, before it ever compares snapshots, so every branch of a
// group over an empty snapshot short-circuits -- no fallback, no plan
// execution, and no mismatch warning.
//
// There is no log-assertion seam in these tests, so the observables are the
// result shape (the empty result carries this branch's nq; an executed plan
// would hand back the vector search's own, never-populated result instead) and
// the bitset's unclaimed storage cost.
TEST(SharedFilterBitset, NullBitsetShortCircuitsEveryBranch) {
    auto f = MakeGrowingFixture();
    // Every row is stamped after the query timestamp: the snapshot is empty
    // although the segment is not.
    InsertBatch(f, kNumRows, /*ts=*/2000, /*seed=*/42);
    const Timestamp query_ts = 1000;
    ASSERT_EQ(f.segment->get_active_count(query_ts), 0);

    auto plan_a = MakePlan(f, "int64 > 100", "vec_a");
    auto plan_b = MakePlan(f, "int64 > 100", "vec_b");
    auto ph_a = MakePlaceholder(plan_a.get(), 1024);
    auto ph_b = MakePlaceholder(plan_b.get(), 2048);

    auto bitset = f.segment->ComputeFilterBitset(
        plan_a.get(), query_ts, folly::CancellationToken(), 0, 0);
    ASSERT_NE(bitset, nullptr);
    ASSERT_EQ(bitset->bitset, nullptr) << "the empty-snapshot sentinel";
    ASSERT_EQ(bitset->active_count, 0);

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
    for (const auto* got : {got_a.get(), got_b.get()}) {
        EXPECT_EQ(got->total_nq_, 1);
        EXPECT_EQ(got->unity_topK_, 0);
        EXPECT_EQ(got->total_data_cnt_, 0);
        EXPECT_TRUE(got->seg_offsets_.empty());
    }
    EXPECT_FALSE(bitset->filter_storage_cost_claimed.load())
        << "no branch ran against the bitset, so there is nothing to claim";
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

// Vector-array (embedding list) search is the element-level path on this
// branch: the prefix produces a row-level bitset and PhyVectorSearchNode
// expands it to elements per branch, writing active_element_count onto its
// own QueryContext. The shared bitset must stay row-level and every branch
// must still do its own expansion, so the differential is against the
// element-level plain search.
TEST(SharedFilterBitset, VectorArrayMatchesIndependentSearch) {
    constexpr int kArrayLen = 3;
    const std::string metric = knowhere::metric::L2;

    auto schema = std::make_shared<Schema>();
    schema->AddDebugVectorArrayField(
        "structA[array_vec_a]", DataType::VECTOR_FLOAT, kDim, metric);
    schema->AddDebugVectorArrayField(
        "structA[array_vec_b]", DataType::VECTOR_FLOAT, kDim, metric);
    schema->AddDebugField("int64", DataType::INT64);
    auto pk = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk);

    auto raw = DataGen(schema, kNumRows, 42, 0, 1, kArrayLen);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw);
    ScopedSchemaHandle handle(*schema);

    const std::string expr = "int64 > 100";
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
    ASSERT_TRUE(want_a->element_level_);

    auto bitset = segment->ComputeFilterBitset(
        plan_a.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0);
    ASSERT_NE(bitset, nullptr);

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
    ExpectSameSearchResult(*want_a, *got_a, "vector-array branch a");
    ExpectSameSearchResult(*want_b, *got_b, "vector-array branch b");
}

// An iterative-filter plan keeps its predicate above the vector search
// (IterativeFilterNode -> VectorSearchNode -> MvccNode), so there is no
// filter prefix to share. Phase 1 must refuse it outright rather than extract
// the bare MvccNode underneath and hand back a bitset that carries no
// predicate; and a branch of that shape must be refused in phase 2 as well,
// even against a bitset a well-formed sibling produced.
TEST(SharedFilterBitset, RejectsIterativeFilterPlan) {
    auto f = MakeFixture();
    const std::string iterative = R"({"hints": "iterative_filter"})";
    auto plan_iter = MakePlan(f, "int64 > 100", "vec_a", iterative);
    auto ph = MakePlaceholder(plan_iter.get(), 1024);

    // The plan is valid on its own; only sharing is refused.
    auto plain = f.segment->Search(plan_iter.get(), ph.get(), MAX_TIMESTAMP);
    ASSERT_FALSE(plain->seg_offsets_.empty());

    EXPECT_THROW(
        f.segment->ComputeFilterBitset(
            plan_iter.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0),
        SegcoreError);

    auto plan_prefilter = MakePlan(f, "int64 > 100", "vec_b");
    auto bitset = f.segment->ComputeFilterBitset(
        plan_prefilter.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0);
    ASSERT_NE(bitset, nullptr);
    EXPECT_THROW(f.segment->SearchWithBitset(plan_iter.get(),
                                             ph.get(),
                                             bitset.get(),
                                             MAX_TIMESTAMP,
                                             folly::CancellationToken(),
                                             0,
                                             0),
                 SegcoreError);
}

// Phase 1 runs the same admission check as a plain search. On a sealed
// segment that has not finished loading, the failure must be the retriable
// FieldNotLoaded the query node knows how to handle -- not whatever
// assertion the filter would have tripped over first, and not a quiet empty
// bitset from a zero row count.
TEST(SharedFilterBitset, ComputeFilterBitsetRequiresLoadedFields) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "vec_a", DataType::VECTOR_FLOAT, kDim, knowhere::metric::L2);
    schema->AddDebugField("int64", DataType::INT64);
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    ScopedSchemaHandle handle(*schema);

    // Nothing loaded: no system fields, no int64 column.
    auto segment = CreateSealedSegment(schema, empty_index_meta, 0);
    auto plan_str = handle.ParseSearch("int64 > 100", "vec_a", kTopK, "L2");
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    try {
        segment->ComputeFilterBitset(
            plan.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0);
        FAIL() << "phase 1 on an unloaded segment must throw";
    } catch (const SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), ErrorCode::FieldNotLoaded) << e.what();
    }
}

// A null shared bitset (phase 1 saw no visible rows) must not skip this
// branch's own admission check: a sibling branch may target a vector field
// that is not loaded, and the plain search on the same segment would report
// the retriable FieldNotLoaded rather than quietly return nothing. The cgo
// entry point used to short-circuit a null bitset to an empty result; it now
// always comes through here.
TEST(SharedFilterBitset, NullBitsetStillRunsAdmissionCheck) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "vec_a", DataType::VECTOR_FLOAT, kDim, knowhere::metric::L2);
    schema->AddDebugField("int64", DataType::INT64);
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    ScopedSchemaHandle handle(*schema);

    // Nothing loaded, as a branch whose fields are missing would see it.
    auto segment = CreateSealedSegment(schema, empty_index_meta, 0);
    auto plan_str = handle.ParseSearch("int64 > 100", "vec_a", kTopK, "L2");
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto ph = MakePlaceholder(plan.get(), 1024);

    SharedFilterBitsetResult empty;
    empty.segment_id = segment->get_segment_id();
    ASSERT_EQ(empty.bitset, nullptr);

    try {
        segment->SearchWithBitset(plan.get(),
                                  ph.get(),
                                  &empty,
                                  MAX_TIMESTAMP,
                                  folly::CancellationToken(),
                                  0,
                                  0);
        FAIL() << "phase 2 on an unloaded segment must throw";
    } catch (const SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), ErrorCode::FieldNotLoaded) << e.what();
    }
}

// A plain search reports the filter's bytes and the vector search's bytes on
// one result. Split into two phases, the filter's bytes would otherwise be
// dropped on the floor with phase 1's OpContext. They must show up on
// exactly one branch: summed over the group, the shared path reports the
// same bytes as the plain searches minus the one filter evaluation it
// saved.
TEST(SharedFilterBitset, FilterStorageCostReportedOnce) {
    auto f = MakeFixture();
    auto plan_a = MakePlan(f, "int64 > 100", "vec_a");
    auto plan_b = MakePlan(f, "int64 > 100", "vec_b");
    auto ph_a = MakePlaceholder(plan_a.get(), 1024);
    auto ph_b = MakePlaceholder(plan_b.get(), 2048);

    auto want_a = f.segment->Search(plan_a.get(), ph_a.get(), MAX_TIMESTAMP);
    auto want_b = f.segment->Search(plan_b.get(), ph_b.get(), MAX_TIMESTAMP);

    auto bitset = f.segment->ComputeFilterBitset(
        plan_a.get(), MAX_TIMESTAMP, folly::CancellationToken(), 0, 0);
    ASSERT_NE(bitset, nullptr);
    ASSERT_GT(bitset->filter_storage_cost.scanned_total_bytes, 0)
        << "precondition: the filter must read something, or this test "
           "cannot tell 'attributed once' from 'dropped'";

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

    const auto filter_bytes = bitset->filter_storage_cost.scanned_total_bytes;
    const auto plain_total = want_a->search_storage_cost_.scanned_total_bytes +
                             want_b->search_storage_cost_.scanned_total_bytes;
    const auto shared_total = got_a->search_storage_cost_.scanned_total_bytes +
                              got_b->search_storage_cost_.scanned_total_bytes;
    EXPECT_EQ(shared_total, plain_total - filter_bytes);

    // Exactly one branch carries it: the other one reports only its own
    // vector search, which is what the plain search minus the filter is.
    const bool a_carries = got_a->search_storage_cost_.scanned_total_bytes ==
                           want_a->search_storage_cost_.scanned_total_bytes;
    const bool b_carries = got_b->search_storage_cost_.scanned_total_bytes ==
                           want_b->search_storage_cost_.scanned_total_bytes;
    EXPECT_NE(a_carries, b_carries) << "a=" << a_carries << " b=" << b_carries;
    EXPECT_TRUE(bitset->filter_storage_cost_claimed.load());
}
