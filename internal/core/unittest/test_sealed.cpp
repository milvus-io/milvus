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

//
// Created by mike on 12/28/20.
//
#include "test_utils/DataGen.h"
#include <gtest/gtest.h>
#include <knowhere/index/vector_index/VecIndex.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/VecIndexFactory.h>
#include <knowhere/index/vector_index/IndexIVF.h>
#include "segcore/SegmentSealedImpl.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::query;

namespace {
const int64_t ROW_COUNT = 100 * 1000;
}

TEST(Sealed, without_predicate) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    auto dim = 16;
    auto topK = 5;
    auto metric_type = MetricType::METRIC_L2;
    auto fake_id = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    schema->AddDebugField("age", DataType::FLOAT);
    std::string dsl = R"({
        "bool": {
            "must": [
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5,
                        "round_decimal": 3
                    }
                }
            }
            ]
        }
    })";

    auto N = ROW_COUNT;

    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(0);
    for (int64_t i = 0; i < 1000 * dim; ++i) {
        vec_col.push_back(0);
    }
    auto query_ptr = vec_col.data() + 4200 * dim;
    auto segment = CreateGrowingSegment(schema);
    segment->PreInsert(N);
    segment->Insert(0, N, dataset.row_ids_.data(), dataset.timestamps_.data(), dataset.raw_);

    auto plan = CreatePlan(*schema, dsl);
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroupFromBlob(num_queries, 16, query_ptr);
    auto ph_group = ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    SearchResult sr;
    Timestamp time = 1000000;
    std::vector<const PlaceholderGroup*> ph_group_arr = {ph_group.get()};

    sr = segment->Search(plan.get(), *ph_group, time);
    auto pre_result = SearchResultToJson(sr);
    auto indexing = std::make_shared<knowhere::IVF>();

    auto conf = knowhere::Config{{knowhere::meta::DIM, dim},
                                 {knowhere::meta::TOPK, topK},
                                 {knowhere::IndexParams::nlist, 100},
                                 {knowhere::IndexParams::nprobe, 10},
                                 {knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                 {knowhere::meta::DEVICEID, 0}};

    auto database = knowhere::GenDataset(N, dim, vec_col.data() + 1000 * dim);
    indexing->Train(database, conf);
    indexing->AddWithoutIds(database, conf);

    EXPECT_EQ(indexing->Count(), N);
    EXPECT_EQ(indexing->Dim(), dim);

    auto query_dataset = knowhere::GenDataset(num_queries, dim, query_ptr);

    auto result = indexing->Query(query_dataset, conf, nullptr);

    auto ids = result->Get<int64_t*>(milvus::knowhere::meta::IDS);     // for comparison
    auto dis = result->Get<float*>(milvus::knowhere::meta::DISTANCE);  // for comparison
    std::vector<int64_t> vec_ids(ids, ids + topK * num_queries);
    std::vector<float> vec_dis(dis, dis + topK * num_queries);

    sr.internal_seg_offsets_ = vec_ids;
    sr.result_distances_ = vec_dis;
    auto ref_result = SearchResultToJson(sr);

    LoadIndexInfo load_info;
    load_info.field_id = fake_id.get();
    load_info.index = indexing;
    load_info.index_params["metric_type"] = "L2";

    auto sealed_segment = SealedCreator(schema, dataset, load_info);
    sr = sealed_segment->Search(plan.get(), *ph_group, time);

    auto post_result = SearchResultToJson(sr);
    std::cout << "ref_result"<< std::endl;
    std::cout << ref_result.dump(1) << std::endl;
    std::cout << "post_result" << std::endl;
    std::cout << post_result.dump(1);
    // ASSERT_EQ(ref_result.dump(1), post_result.dump(1));
}

TEST(Sealed, with_predicate) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    auto dim = 16;
    auto topK = 5;
    auto metric_type = MetricType::METRIC_L2;
    auto fake_id = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    schema->AddDebugField("counter", DataType::INT64);
    std::string dsl = R"({
        "bool": {
            "must": [
            {
                "range": {
                    "counter": {
                        "GE": 42000,
                        "LT": 42005
                    }
                }
            },
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5,
                        "round_decimal": 6
                    }
                }
            }
            ]
        }
    })";

    auto N = ROW_COUNT;

    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(0);
    auto query_ptr = vec_col.data() + 42000 * dim;
    auto segment = CreateGrowingSegment(schema);
    segment->PreInsert(N);
    segment->Insert(0, N, dataset.row_ids_.data(), dataset.timestamps_.data(), dataset.raw_);

    auto plan = CreatePlan(*schema, dsl);
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroupFromBlob(num_queries, 16, query_ptr);
    auto ph_group = ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    SearchResult sr;
    Timestamp time = 10000000;
    std::vector<const PlaceholderGroup*> ph_group_arr = {ph_group.get()};

    sr = segment->Search(plan.get(), *ph_group, time);
    auto pre_sr = sr;
    auto indexing = std::make_shared<knowhere::IVF>();

    auto conf = knowhere::Config{{knowhere::meta::DIM, dim},
                                 {knowhere::meta::TOPK, topK},
                                 {knowhere::IndexParams::nlist, 100},
                                 {knowhere::IndexParams::nprobe, 10},
                                 {knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                                 {knowhere::meta::DEVICEID, 0}};

    auto database = knowhere::GenDataset(N, dim, vec_col.data());
    indexing->Train(database, conf);
    indexing->AddWithoutIds(database, conf);

    EXPECT_EQ(indexing->Count(), N);
    EXPECT_EQ(indexing->Dim(), dim);

    auto query_dataset = knowhere::GenDataset(num_queries, dim, query_ptr);

    auto result = indexing->Query(query_dataset, conf, nullptr);

    LoadIndexInfo load_info;
    load_info.field_id = fake_id.get();
    load_info.index = indexing;
    load_info.index_params["metric_type"] = "L2";

    auto sealed_segment = SealedCreator(schema, dataset, load_info);
    sr = sealed_segment->Search(plan.get(), *ph_group, time);

    auto post_sr = sr;
    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * topK;
        ASSERT_EQ(post_sr.internal_seg_offsets_[offset], 42000 + i);
        ASSERT_EQ(post_sr.result_distances_[offset], 0.0);
    }
}

TEST(Sealed, LoadFieldData) {
    auto dim = 16;
    auto topK = 5;
    auto N = ROW_COUNT;
    auto metric_type = MetricType::METRIC_L2;
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto counter_id = schema->AddDebugField("counter", DataType::INT64);
    auto double_id = schema->AddDebugField("double", DataType::DOUBLE);
    auto nothing_id = schema->AddDebugField("nothing", DataType::INT32);

    auto dataset = DataGen(schema, N);

    auto fakevec = dataset.get_col<float>(0);

    auto indexing = GenIndexing(N, dim, fakevec.data());

    auto segment = CreateSealedSegment(schema);
    std::string dsl = R"({
        "bool": {
            "must": [
            {
                "range": {
                    "double": {
                        "GE": -1,
                        "LT": 1
                    }
                }
            },
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5,
                        "round_decimal": 3
                    }
                }
            }
            ]
        }
    })";

    Timestamp time = 1000000;
    auto plan = CreatePlan(*schema, dsl);
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group = ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    ASSERT_ANY_THROW(segment->Search(plan.get(), *ph_group, time));

    SealedLoader(dataset, *segment);
    segment->DropFieldData(nothing_id);
    segment->Search(plan.get(), *ph_group, time);

    segment->DropFieldData(fakevec_id);
    ASSERT_ANY_THROW(segment->Search(plan.get(), *ph_group, time));

    LoadIndexInfo vec_info;
    vec_info.field_id = fakevec_id.get();
    vec_info.index = indexing;
    vec_info.index_params["metric_type"] = milvus::knowhere::Metric::L2;
    segment->LoadIndex(vec_info);

    ASSERT_EQ(segment->num_chunk(), 1);
    auto chunk_span1 = segment->chunk_data<int64_t>(FieldOffset(1), 0);
    auto chunk_span2 = segment->chunk_data<double>(FieldOffset(2), 0);
    auto ref1 = dataset.get_col<int64_t>(1);
    auto ref2 = dataset.get_col<double>(2);
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(chunk_span1[i], ref1[i]);
        ASSERT_EQ(chunk_span2[i], ref2[i]);
    }

    auto sr = segment->Search(plan.get(), *ph_group, time);
    auto json = SearchResultToJson(sr);
    std::cout << json.dump(1);

    segment->DropIndex(fakevec_id);
    ASSERT_ANY_THROW(segment->Search(plan.get(), *ph_group, time));
    segment->LoadIndex(vec_info);
    auto sr2 = segment->Search(plan.get(), *ph_group, time);
    auto json2 = SearchResultToJson(sr);
    ASSERT_EQ(json.dump(-2), json2.dump(-2));
    segment->DropFieldData(double_id);
    ASSERT_ANY_THROW(segment->Search(plan.get(), *ph_group, time));
    auto std_json = Json::parse(R"(
[
	[
		["982->0.000000", "25315->4.742000", "57893->4.758000", "48201->6.075000", "53853->6.223000"],
		["41772->10.111000", "74859->11.790000", "79777->11.842000", "3785->11.983000", "35888->12.193000"],
		["59251->2.543000", "65551->4.454000", "72204->5.332000", "96905->5.479000", "87833->5.765000"],
		["59219->5.458000", "21995->6.078000", "97922->6.764000", "25710->7.158000", "14048->7.294000"],
		["66353->5.696000", "30664->5.881000", "41087->5.917000", "10393->6.633000", "90215->7.202000"]
	]
])");
    ASSERT_EQ(std_json.dump(-2), json.dump(-2));
}