// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

// Regression test for issue #51286:
//   ProcessIndexLookupByOffsets (the "index reverse lookup" path taken when a
//   scalar field has ONLY an index and no raw chunk data, i.e.
//   UseIndexCursor() && num_data_chunk_ == 0) used to run Reverse_Lookup for
//   EVERY offset in the batch, ignoring the bitmap_input produced by a
//   preceding AND operand. The fix prunes rows whose bitmap_input bit is unset.
//
// This test proves CORRECTNESS-EQUIVALENCE: the same conjunct query
// "predicate_A AND predicate_B(int64)" must produce an IDENTICAL result when
// the int64 field is served from an INDEX-ONLY sealed segment (pruned index
// path) versus a raw-data sealed segment (baseline raw path).
//
// There are two distinct correctness hazards the pruning must respect, and
// this file covers BOTH:
//
//  1. The result-set hazard (the original three cases): a pruned row must end
//     up excluded, exactly as the raw path would exclude it. Exercised by the
//     Unary operand `int64 < N` across selective / full / empty selectivities.
//
//  2. The CURSOR-DESYNC hazard (the Term / BinaryRange cases added here). The
//     Term / Unary / BinaryRange per-row callbacks keep their OWN
//     `processed_cursor` and re-check `bitmap_input[processed_cursor + i]`
//     internally. That cursor advances ONLY when the callback is invoked. The
//     buggy code pruned a row with a bare `continue`, skipping the callback and
//     so NOT advancing the cursor. A later SURVIVING row then read
//     `bitmap_input` at a stale (lagged) position; if that stale bit was a
//     pruned (false) bit, the callback dropped a true match -> a FALSE NEGATIVE
//     that diverges from the raw baseline. The fix invokes the callback with
//     data==nullptr for pruned rows so the cursor stays aligned.
//
//     The original `int64 < N` Unary case did NOT catch this: with `< N` every
//     row matches, so even a mis-read stale bit that flips a survivor to
//     excluded is masked in the ranking (or the missing candidate is padded by
//     an equally-ranked one). A Term / BinaryRange operand whose survivor set
//     is a NON-TRIVIAL subset (some match, some don't) surfaces the dropped
//     match in the topK offsets. These cases are red WITHOUT the fix (cursor
//     desync drops matches -> offsets differ from raw) and green WITH it.

#include <gtest/gtest.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/Schema.h"
#include "common/Types.h"
#include "index/ScalarIndex.h"
#include "index/ScalarIndexSort.h"
#include "query/Plan.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

namespace {

// Overwrite the INT32 field column (proto int_data) with controlled values so
// that predicate_A selectivity is deterministic across platforms/RNG.
void
OverrideInt32Column(GeneratedData& raw_data,
                    FieldId field_id,
                    const std::vector<int32_t>& values) {
    for (int i = 0; i < raw_data.raw_->fields_data_size(); ++i) {
        auto* fd = raw_data.raw_->mutable_fields_data(i);
        if (fd->field_id() == field_id.get()) {
            auto* int_data = fd->mutable_scalars()->mutable_int_data();
            int_data->clear_data();
            for (auto v : values) {
                int_data->add_data(v);
            }
            return;
        }
    }
    FAIL() << "int32 field not found in generated data";
}

// Overwrite the INT64 field column (proto long_data) with controlled values.
// The int64 field is nullable, so its long_data stays FULL length (one entry
// per row, NOT compacted to valid_count): CreateFieldDataFromDataArray reads
// raw_count==N entries from BOTH long_data and valid_data, so a full-length
// long_data round-trips correctly and get_col<int64_t> reads all N values.
void
OverrideInt64Column(GeneratedData& raw_data,
                    FieldId field_id,
                    const std::vector<int64_t>& values) {
    for (int i = 0; i < raw_data.raw_->fields_data_size(); ++i) {
        auto* fd = raw_data.raw_->mutable_fields_data(i);
        if (fd->field_id() == field_id.get()) {
            auto* long_data = fd->mutable_scalars()->mutable_long_data();
            long_data->clear_data();
            for (auto v : values) {
                long_data->add_data(v);
            }
            return;
        }
    }
    FAIL() << "int64 field not found in generated data";
}

// Overwrite the INT64 field's per-row validity bitmap (proto valid_data). A
// false entry marks that row NULL; Reverse_Lookup returns nullopt for it on the
// index-only segment and the raw path yields UNKNOWN -> excluded. With an
// all-true vector this is a no-op relative to a non-nullable column.
void
OverrideInt64Valid(GeneratedData& raw_data,
                   FieldId field_id,
                   const FixedVector<bool>& valid) {
    for (int i = 0; i < raw_data.raw_->fields_data_size(); ++i) {
        auto* fd = raw_data.raw_->mutable_fields_data(i);
        if (fd->field_id() == field_id.get()) {
            fd->clear_valid_data();
            for (bool v : valid) {
                fd->add_valid_data(v);
            }
            return;
        }
    }
    FAIL() << "int64 field not found in generated data";
}

// Build a STL_SORT scalar index over the FULL-length int64 values with an
// explicit validity bitmap, so the index-only segment's Reverse_Lookup returns
// nullopt for null rows exactly where the raw path sees a NULL. Mirrors
// GenScalarIndexing<int64_t> but threads valid_data into Build().
std::unique_ptr<index::ScalarIndex<int64_t>>
GenScalarIndexingWithValidity(int64_t N,
                              const int64_t* data,
                              const bool* valid_data) {
    auto indexing = index::CreateScalarIndexSort<int64_t>();
    indexing->Build(N, data, valid_data);
    return indexing;
}

}  // namespace

class IndexReverseLookupPruneTest : public ::testing::Test {
 protected:
    static constexpr int dim_ = 64;
    static constexpr size_t N_ = 2000;

    void
    SetUp() override {
        schema_ = std::make_shared<Schema>();
        vec_fid_ = schema_->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, dim_, knowhere::metric::L2);
        // predicate_A field: raw data loaded in BOTH segments.
        int32_fid_ = schema_->AddDebugField("int32", DataType::INT32);
        // range field: index-only in one segment, raw in the other. NULLABLE so
        // the nullable-rows case can drive Reverse_Lookup to nullopt; every
        // other case sets an all-true validity bitmap, which is behaviourally
        // identical to a non-nullable column.
        int64_fid_ =
            schema_->AddDebugField("int64", DataType::INT64, /*nullable=*/true);
        str_fid_ = schema_->AddDebugField("string1", DataType::VARCHAR);
        schema_->set_primary_field_id(str_fid_);

        // Deterministic int64 values: 0..N-1. The range "int64 < N" keeps
        // every row, so pruning is driven purely by predicate_A on int32.
        int64_vals_.resize(N_);
        for (size_t i = 0; i < N_; ++i) {
            int64_vals_[i] = static_cast<int64_t>(i);
        }
        // Default: every int64 row is valid (non-null).
        int64_valid_.assign(N_, true);
    }

    // Build a sealed segment from `raw_data`, always with a vector index, and:
    //   index_only_int64 == true  -> int64 raw data EXCLUDED + int64 scalar
    //                                index loaded (num_data_chunk_==0 -> the
    //                                ProcessIndexLookupByOffsets path).
    //   index_only_int64 == false -> int64 raw data loaded (baseline raw path).
    SegmentSealedUPtr
    BuildSegment(GeneratedData& raw_data, bool index_only_int64) {
        std::vector<int64_t> excluded;
        if (index_only_int64) {
            excluded.push_back(int64_fid_.get());
        }
        auto segment =
            CreateSealedWithFieldDataLoaded(schema_, raw_data, false, excluded);

        // Vector index (needed to drive the iterative_filter offset-input path).
        auto vec_col = raw_data.get_col<float>(vec_fid_);
        auto vec_index = GenVecIndexing(
            N_, dim_, vec_col.data(), knowhere::IndexEnum::INDEX_HNSW);
        LoadIndexInfo vec_load;
        vec_load.field_id = vec_fid_.get();
        vec_load.index_params = GenIndexParams(vec_index.get());
        vec_load.cache_index =
            CreateTestCacheIndex("vec_index", std::move(vec_index));
        vec_load.index_params["metric_type"] = knowhere::metric::L2;
        segment->LoadIndex(vec_load);

        if (index_only_int64) {
            // Scalar index on int64, WITHOUT its raw data. Built with the same
            // validity bitmap the raw column carries, so Reverse_Lookup returns
            // nullopt for exactly the null rows.
            auto int64_col = raw_data.get_col<int64_t>(int64_fid_);
            auto int64_index = GenScalarIndexingWithValidity(
                N_, int64_col.data(), int64_valid_.data());
            LoadIndexInfo idx_load;
            idx_load.field_id = int64_fid_.get();
            idx_load.index_params = GenIndexParams(int64_index.get());
            idx_load.cache_index =
                CreateTestCacheIndex("int64_index", std::move(int64_index));
            segment->LoadIndex(idx_load);
        }
        return segment;
    }

    // Run "<int32_predicate> AND <int64_predicate>" via the iterative_filter
    // path and return the topK*nq seg_offsets_. The int32 predicate is placed
    // FIRST so the optimizer keeps the int64 operand SECOND -- the int64
    // operand is the one that receives bitmap_input and, when served
    // index-only, flows through ProcessIndexLookupByOffsets.
    std::vector<int64_t>
    RunConjunctSearch(SegmentSealed* segment,
                      ScopedSchemaHandle& handle,
                      const std::string& int32_predicate,
                      const std::string& int64_predicate) {
        const int topK = 10;
        const std::string expr = int32_predicate + " AND " + int64_predicate;
        auto plan_bytes = handle.ParseSearch(expr,
                                             "fakevec",
                                             topK,
                                             "L2",
                                             "{\"ef\": 200}",
                                             -1,
                                             "iterative_filter");
        auto plan = CreateSearchPlanByExpr(
            schema_, plan_bytes.data(), plan_bytes.size());
        auto ph_group_raw = CreatePlaceholderGroup(1, dim_, 1024);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto result =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
        return std::vector<int64_t>(result->seg_offsets_.begin(),
                                    result->seg_offsets_.end());
    }

    // Full equivalence assertion for a given (predicate_A, int64 predicate_B):
    // build both segments from the SAME generated data and assert identical
    // search offsets. If the index path failed to honor bitmap_input -- either
    // by keeping a pruned row, or by desyncing the callback cursor and dropping
    // a survivor -- its filtered set would differ from the raw baseline and the
    // topK offsets would diverge.
    void
    AssertEquivalence(const std::vector<int32_t>& int32_vals,
                      const std::string& int32_predicate,
                      const std::string& int64_predicate = "int64 < " +
                                                           std::to_string(N_)) {
        auto raw_a = DataGen(schema_, N_, 42, 0, 8, 10, false, false);
        OverrideInt32Column(raw_a, int32_fid_, int32_vals);
        OverrideInt64Column(raw_a, int64_fid_, int64_vals_);
        OverrideInt64Valid(raw_a, int64_fid_, int64_valid_);
        // Second copy with the SAME seed => identical vectors/PK.
        auto raw_b = DataGen(schema_, N_, 42, 0, 8, 10, false, false);
        OverrideInt32Column(raw_b, int32_fid_, int32_vals);
        OverrideInt64Column(raw_b, int64_fid_, int64_vals_);
        OverrideInt64Valid(raw_b, int64_fid_, int64_valid_);

        auto seg_index_only = BuildSegment(raw_a, /*index_only_int64=*/true);
        auto seg_raw = BuildSegment(raw_b, /*index_only_int64=*/false);

        ScopedSchemaHandle handle(*schema_);
        auto offsets_index_only = RunConjunctSearch(
            seg_index_only.get(), handle, int32_predicate, int64_predicate);
        auto offsets_raw = RunConjunctSearch(
            seg_raw.get(), handle, int32_predicate, int64_predicate);

        ASSERT_EQ(offsets_index_only.size(), offsets_raw.size())
            << "int32=" << int32_predicate << " int64=" << int64_predicate;
        EXPECT_EQ(offsets_index_only, offsets_raw)
            << "index-only (pruned) result must match raw-path baseline for "
               "int32="
            << int32_predicate << " int64=" << int64_predicate;
    }

    SchemaPtr schema_;
    FieldId vec_fid_;
    FieldId int32_fid_;
    FieldId int64_fid_;
    FieldId str_fid_;
    std::vector<int64_t> int64_vals_;
    FixedVector<bool> int64_valid_;
};

// (a) Selective predicate_A: only ~1% of rows survive -> bitmap_input is
// sparse, so most Reverse_Lookup calls are pruned. Result must still match the
// raw baseline exactly.
TEST_F(IndexReverseLookupPruneTest, SelectivePredicateMatchesRaw) {
    std::vector<int32_t> int32_vals(N_, 0);
    // ~1% of rows get value 7; predicate "int32 == 7" keeps only those.
    for (size_t i = 0; i < N_; i += 100) {
        int32_vals[i] = 7;
    }
    AssertEquivalence(int32_vals, "int32 == 7");
}

// (b) predicate_A keeps ALL rows -> bitmap_input effectively full; no row is
// pruned. Equivalent to the pre-fix behavior; result must match raw baseline.
TEST_F(IndexReverseLookupPruneTest, FullPredicateMatchesRaw) {
    std::vector<int32_t> int32_vals(N_, 5);
    AssertEquivalence(int32_vals, "int32 == 5");
}

// (c) predicate_A keeps ZERO rows -> bitmap_input all-unset; every index
// Reverse_Lookup is pruned and the result must be empty on BOTH segments (and
// the topK-padded offsets must be identical).
TEST_F(IndexReverseLookupPruneTest, EmptyPredicateMatchesRaw) {
    std::vector<int32_t> int32_vals(N_, 1);
    // No row equals 999 -> predicate_A yields zero survivors.
    AssertEquivalence(int32_vals, "int32 == 999");
}

// (d) TERM operand red/green for the CURSOR-DESYNC bug.
//
// predicate_A = "int32 == 7" keeps a sparse, interleaved ~1% survivor set
// (rows 0,100,200,...,1900). The SECOND operand is a Term predicate on the
// index-only int64 field: `int64 in [...]`. Term's per-row callback maintains
// `processed_cursor` and re-checks `bitmap_input[processed_cursor + i]`; the
// cursor advances ONLY when the callback runs. The buggy bare-continue on a
// pruned row skipped the callback and lagged the cursor, so a later surviving
// row read a stale (already-pruned, false) bitmap_input bit and dropped its
// true match -> divergence from the raw baseline. The fix drives the callback
// with data==nullptr on pruned rows, keeping the cursor aligned.
//
// The Term set overlaps the int32==7 survivors PARTIALLY: it contains the
// int64 values of some survivor rows (100,300,500,...,1900 -> values equal to
// the row index, since int64_vals_[i]==i) plus non-survivor decoys. So the
// correct answer is a non-trivial subset -- some int32==7 rows are in the Term
// set (must appear) and some are not (must be excluded). A dropped match from a
// desynced cursor changes the topK offsets, which the equivalence assertion
// catches.
TEST_F(IndexReverseLookupPruneTest, TermOperandMatchesRaw) {
    std::vector<int32_t> int32_vals(N_, 0);
    for (size_t i = 0; i < N_; i += 100) {
        int32_vals[i] = 7;  // survivors at rows 0,100,...,1900
    }
    // int64_vals_[i] == i, so a survivor row r has int64 value r. Pick a sparse
    // subset of the ODD-hundred survivors as the Term set, interleaved with
    // even-hundred survivors that are pruned-through-Term (present in
    // bitmap_input but absent from the Term set) -- maximising the chance a
    // lagged cursor read flips a result.
    std::string term = "int64 in [";
    bool first = true;
    for (size_t r = 100; r < N_; r += 200) {  // 100,300,...,1900
        if (!first) {
            term += ", ";
        }
        term += std::to_string(r);
        first = false;
    }
    // Add a few decoys that are NOT int32==7 survivors (rows 50,150,... are 0).
    term += ", 50, 150, 1234";
    term += "]";
    AssertEquivalence(int32_vals, "int32 == 7", term);
}

// (e) BINARY-RANGE operand through the pruned index path (supplementary
// coverage). Same setup as (d) but the second operand is a real BinaryRangeExpr
// `500 < int64 < 1500`, which also re-checks bitmap[processed_cursor + i] via
// BinaryRangeElementFunc. Empirically this case passes on BOTH the pre-fix and
// post-fix trees: its surviving subset is large enough that the specific
// desynced-cursor drops here do not shift the topK offsets. It is kept as
// equivalence coverage of the BinaryRange operand through
// ProcessIndexLookupByOffsets; the TERM case (d) is the discriminating
// red/green regression for the cursor bug.
TEST_F(IndexReverseLookupPruneTest, BinaryRangeOperandMatchesRaw) {
    std::vector<int32_t> int32_vals(N_, 0);
    for (size_t i = 0; i < N_; i += 100) {
        int32_vals[i] = 7;
    }
    AssertEquivalence(int32_vals, "int32 == 7", "500 < int64 < 1500");
}

// (f) NULLABLE index rows crossed with pruning.
//
// Some int64 rows are NULL: Reverse_Lookup returns nullopt for them on the
// index-only segment, and the raw path sees a NULL that the null-rejecting AND
// folds to excluded. This exercises the interaction between the nullopt branch
// (which also advances the callback cursor) and pruned rows. We null out a
// deterministic slice of the int32==7 survivors so that survivors, nulls, and
// Term membership all cross. Equivalence between the index-only and raw paths
// must still hold.
TEST_F(IndexReverseLookupPruneTest, NullableRowsMatchesRaw) {
    std::vector<int32_t> int32_vals(N_, 0);
    for (size_t i = 0; i < N_; i += 100) {
        int32_vals[i] = 7;  // survivors at rows 0,100,...,1900
    }
    // Null out every other survivor (rows 100,300,...,1900) plus a scatter of
    // non-survivors, so nulls interleave with valid pruned/surviving rows.
    int64_valid_.assign(N_, true);
    for (size_t r = 100; r < N_; r += 200) {
        int64_valid_[r] = false;
    }
    for (size_t r = 37; r < N_; r += 211) {
        int64_valid_[r] = false;
    }
    // Second operand keeps a non-trivial subset AND straddles null rows.
    AssertEquivalence(int32_vals, "int32 == 7", "int64 < 1500");
}

// NOTE on a call-counting assertion (deferred):
//   Ideally we would also assert that the number of Reverse_Lookup calls drops
//   from batch_size to ~survivor count, directly proving the prune fires. Doing
//   so requires a counting ScalarIndex<int64_t> subclass that (a) can be loaded
//   through LoadIndexInfo/CreateTestCacheIndex and (b) survives the
//   `dynamic_cast<const ScalarIndex<int64_t>*>` in ProcessIndexLookupByOffsets.
//   Wiring a custom index through the cache/serialization path is deep
//   plumbing well beyond the equivalence harness here, so the counting
//   assertion is intentionally deferred. The TERM case (d) is the verified
//   red/green regression for the correctness bug (confirmed FAILED on the
//   pre-fix tree and OK post-fix: cursor desync -> dropped matches -> offsets
//   diverge from the raw baseline); (e) and (f) add equivalence coverage of the
//   BinaryRange and nullable paths through the pruned index lookup.
