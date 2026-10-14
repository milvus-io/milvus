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

#include "common/ClusteringSearchHints.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "test_utils/index_test_utils.h"
#include "indexbuilder/VectorDiskBuildMaterializer.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentSealed.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "gtest/gtest.h"
using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

TEST(ClusteringSearchHints, CopyValidationAndOwnership) {
    uint8_t presence[] = {1, 0, 1};
    int64_t boundaries[] = {0, 1, 1, 1}, offsets[] = {int64_t{1} << 33},
            counts[] = {2};
    float distances[] = {1.25};
    const int64_t rows = int64_t{1} << 34;
    auto h = CopyClusteringSearchHints(
        rows, presence, 3, boundaries, offsets, counts, distances, 1);
    ASSERT_TRUE(h);
    ASSERT_TRUE(h->queries[0]);
    EXPECT_EQ(h->queries[0]->ranges[0].offset, int64_t{1} << 33);
    EXPECT_FALSE(h->queries[1]);
    ASSERT_TRUE(h->queries[2]);
    EXPECT_TRUE(h->queries[2]->ranges.empty());
    distances[0] = 7;
    EXPECT_EQ(h->queries[0]->ranges[0].id_distance, 1.25);
    EXPECT_FALSE(h->Valid(rows - 1, 3));
    EXPECT_FALSE(h->Valid(rows, 2));
    counts[0] = INT64_MAX;
    EXPECT_FALSE(CopyClusteringSearchHints(
        rows, presence, 3, boundaries, offsets, counts, distances, 1));
    counts[0] = 2;
    distances[0] = NAN;
    EXPECT_FALSE(CopyClusteringSearchHints(
        rows, presence, 3, boundaries, offsets, counts, distances, 1));
    distances[0] = 1;
    presence[0] = 0;
    EXPECT_FALSE(CopyClusteringSearchHints(
        rows, presence, 3, boundaries, offsets, counts, distances, 1));
    presence[0] = 1;
    boundaries[2] = 0;
    EXPECT_FALSE(CopyClusteringSearchHints(
        rows, presence, 3, boundaries, offsets, counts, distances, 1));
}

template <typename T>
void
CheckHintedSearch(DataType dtype,
                  proto::common::PlaceholderType ptype,
                  const std::string& index_type = "HNSW") {
    const int64_t rows = 128, dim = 8, nq = 8, topk = 4;
    auto schema = std::make_shared<Schema>();
    auto vec = schema->AddDebugField("vec", dtype, dim, knowhere::metric::L2);
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto data = DataGen(schema, rows);
    auto segment = CreateSealedSegment(schema, empty_index_meta, 9182);
    LoadGeneratedDataIntoSegment(data, segment.get(), false, {});
    std::vector<T> base(rows * dim, T(0));
    for (int64_t i = 0; i < rows; ++i) base[i * dim] = T(i);
    knowhere::Json config = {{"dim", dim},
                             {"metric_type", "L2"},
                             {"M", 8},
                             {"efConstruction", 32},
                             {"index_algo", "HYBRID"},
                             {"graph_search_dim_threshold", 0},
                             {"graph_build_algo", "pipnn"},
                             {"build_quant_type", "NONE"},
                             {"search_quant_type", "NONE"},
                             {"refine_quant_type", "NONE"}};
    auto adapted = index::AdaptIndexType({
        .index_type = index_type,
        .field_type = dtype,
        .index_engine_version = 10,
        .params = config,
    });
    storage::ArtifactPtr artifact;
    if (adapted.family == index::families::kVectorDisk) {
        adapted.params["local_dir"] = TestLocalPath;
        indexbuilder::VectorDiskBuildMaterializer materializer(
            TestLocalPath,
            dtype,
            adapted.value_type,
            dim,
            false,
            rows,
            adapted.family,
            adapted.params);
        auto batch =
            storage::CreateFieldData(dtype, DataType::NONE, false, dim, rows);
        batch->FillFieldData(base.data(), rows);
        materializer.Add(batch);
        materializer.FinishPrimary();
        artifact = std::move(materializer).Build();
    } else {
        using Input = index::VectorBuildInput<T>;
        auto builder = index::BuilderRegistry<Input>::Instance().Create(
            adapted.family, adapted.params);
        ASSERT_TRUE(builder);
        Input input{.physical_values = base,
                    .logical_rows = rows,
                    .physical_rows = rows,
                    .dim = dim};
        artifact = std::move(*builder).Build(input);
    }
    auto indexing = test_index_detail::PersistAndLoad(
        std::move(artifact), adapted.family, adapted.params);
    LoadIndexInfo load;
    load.field_id = vec.get();
    load.index_params = GenIndexParams(indexing.get());
    load.index_params["metric_type"] = "L2";
    SetTestIndexMetadata(load, *indexing, adapted.family);
    load.cache_index =
        CreateTestCacheIndex("clustering-hints", std::move(indexing));
    segment->DropFieldData(vec);
    segment->LoadIndex(load);
    auto h = std::make_shared<ClusteringSearchHints>();
    h->expected_rows = rows;
    h->queries = {knowhere::QuerySearchHints{{{50, 2, 0.25f}}},
                  std::nullopt,
                  knowhere::QuerySearchHints{}};
    h->queries.resize(nq, knowhere::QuerySearchHints{});
    for (const auto* algo : {"BF", "GRAPH"}) {
        ScopedSchemaHandle handle(*schema);
        auto serialized = handle.ParseSearch(
            "",
            "vec",
            topk,
            "L2",
            std::string("{\"ef\":128,\"index_algo\":\"") + algo + "\"}",
            3);
        auto plan = CreateSearchPlanByExpr(
            schema, serialized.data(), serialized.size());
        proto::common::PlaceholderGroup raw;
        auto* ph = raw.add_placeholders();
        ph->set_tag("$0");
        ph->set_type(ptype);
        std::vector<T> query(dim, T(0));
        for (int i = 0; i < nq; ++i)
            ph->add_values(query.data(), query.size() * sizeof(T));
        auto queries =
            ParsePlaceholderGroup(plan.get(), raw.SerializeAsString());
        auto search = [&](ClusteringSearchHintsPtr hints) {
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
                                   hints);
        };
        auto ordinary = search(nullptr);
        auto result = search(h);
        ASSERT_EQ(result->seg_offsets_.size(), nq * topk);
        if (index_type == "HNSW") {
            if (std::string(algo) == "BF") {
                EXPECT_EQ(result->seg_offsets_[0], 50);
                EXPECT_EQ(result->seg_offsets_[1], 51);
                EXPECT_EQ(result->seg_offsets_[2], -1);
                EXPECT_EQ(result->distances_[0], 2500);
            } else {
                EXPECT_EQ(result->seg_offsets_[0],
                          0);  // Graph is allowed to leave the hinted range.
            }
            for (int64_t i = 2 * topk; i < nq * topk; ++i)
                EXPECT_EQ(result->seg_offsets_[i], -1);
        } else {
            EXPECT_EQ(result->seg_offsets_, ordinary->seg_offsets_);
        }
        for (int64_t i = topk; i < 2 * topk; ++i)
            EXPECT_EQ(result->seg_offsets_[i], ordinary->seg_offsets_[i]);
        EXPECT_EQ(search(nullptr)->seg_offsets_,
                  ordinary->seg_offsets_);  // No shared plan mutation.
        auto bad = std::make_shared<ClusteringSearchHints>(*h);
        bad->expected_rows++;
        EXPECT_EQ(search(bad)->seg_offsets_, ordinary->seg_offsets_);
        bad = std::make_shared<ClusteringSearchHints>(*h);
        bad->queries[0]->ranges[0].count = INT64_MAX;
        EXPECT_EQ(search(bad)->seg_offsets_, ordinary->seg_offsets_);
        auto empty = std::make_shared<ClusteringSearchHints>();
        empty->expected_rows = rows;
        empty->queries.resize(nq, knowhere::QuerySearchHints{});
        EXPECT_EQ(search(empty)->seg_offsets_,
                  index_type == "HNSW" ? std::vector<int64_t>(nq * topk, -1)
                                       : ordinary->seg_offsets_);
    }
    // Real scalar filtering and MVCC deletion must remain independent of hints.
    auto pk_data = data.get_col<int64_t>(pk);
    auto deleted = GenPKs(pk_data.begin() + 51, pk_data.begin() + 52);
    auto timestamps = GenTss(1, 1000);
    ASSERT_TRUE(segment->Delete(1, deleted.get(), timestamps.data()).ok());
    for (const auto* algo : {"BF", "GRAPH"}) {
        ScopedSchemaHandle handle(*schema);
        auto serialized = handle.ParseSearch(
            "pk >= 49",
            "vec",
            topk,
            "L2",
            std::string("{\"ef\":128,\"index_algo\":\"") + algo + "\"}",
            3);
        auto plan = CreateSearchPlanByExpr(
            schema, serialized.data(), serialized.size());
        proto::common::PlaceholderGroup raw;
        auto* ph = raw.add_placeholders();
        ph->set_tag("$0");
        ph->set_type(ptype);
        std::vector<T> query(dim, T(0));
        for (int i = 0; i < nq; ++i)
            ph->add_values(query.data(), query.size() * sizeof(T));
        auto queries =
            ParsePlaceholderGroup(plan.get(), raw.SerializeAsString());
        auto result = segment->Search(plan.get(),
                                      queries.get(),
                                      1000000,
                                      folly::CancellationToken(),
                                      0,
                                      51,
                                      0,
                                      false,
                                      false,
                                      nullptr,
                                      h);
        EXPECT_EQ(result->seg_offsets_[topk], 52);
        if (index_type == "HNSW") {
            EXPECT_EQ(result->seg_offsets_[0],
                      std::string(algo) == "BF" ? -1 : 52);
            EXPECT_EQ(result->seg_offsets_[2 * topk], -1);
        }
        folly::CancellationSource cancellation;
        cancellation.requestCancellation();
        EXPECT_ANY_THROW(segment->Search(plan.get(),
                                         queries.get(),
                                         1000000,
                                         cancellation.getToken(),
                                         0,
                                         0,
                                         0,
                                         false,
                                         false,
                                         nullptr,
                                         h));
    }
}
TEST(ClusteringSearchHints, FloatGraphAndBF) {
    CheckHintedSearch<float>(DataType::VECTOR_FLOAT,
                             proto::common::FloatVector);
}
TEST(ClusteringSearchHints, Float16GraphAndBF) {
    CheckHintedSearch<knowhere::fp16>(DataType::VECTOR_FLOAT16,
                                      proto::common::Float16Vector);
}
TEST(ClusteringSearchHints, BFloat16GraphAndBF) {
    CheckHintedSearch<knowhere::bf16>(DataType::VECTOR_BFLOAT16,
                                      proto::common::BFloat16Vector);
}
TEST(ClusteringSearchHints, FlatBackendMayIgnoreHints) {
    CheckHintedSearch<float>(
        DataType::VECTOR_FLOAT, proto::common::FloatVector, "FLAT");
}
