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

#include <arrow/array/builder_nested.h>
#include <gtest/gtest.h>
#include <boost/format.hpp>

#include "common/ArrowConverter.h"
#include "knowhere/index/VecIndex.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "segcore/SegmentSealedImpl.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

const int64_t ROW_COUNT = 100 * 1000;

TEST(Sealed, without_predicate) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    auto dim = 16;
    auto topK = 5;
    auto metric_type = knowhere::metric::L2;
    auto fake_id = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto float_fid = schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
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
    auto vec_col = dataset.get_col<float>(fake_id);
    for (int64_t i = 0; i < 1000 * dim; ++i) {
        vec_col.push_back(0);
    }
    auto query_ptr = vec_col.data() + 4200 * dim;
    auto segment = CreateGrowingSegment(schema);
    segment->PreInsert(N);
    segment->Insert(0, N, dataset.get_raw_row_ids(), dataset.get_raw_timestamps(), dataset.raw_.get());

    auto plan = CreatePlan(*schema, dsl);
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroupFromBlob(num_queries, 16, query_ptr);
    auto ph_group = ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    Timestamp time = 1000000;
    std::vector<const PlaceholderGroup*> ph_group_arr = {ph_group.get()};

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    auto pre_result = SearchResultToJson(*sr);
    auto indexing = std::make_shared<knowhere::IVF>();

    auto conf = knowhere::Config{{knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
                                 {knowhere::meta::DIM, dim},
                                 {knowhere::meta::TOPK, topK},
                                 {knowhere::indexparam::NLIST, 100},
                                 {knowhere::indexparam::NPROBE, 10},
                                 {knowhere::meta::DEVICE_ID, 0}};

    auto database = knowhere::GenDataset(N, dim, vec_col.data() + 1000 * dim);
    indexing->Train(database, conf);
    indexing->AddWithoutIds(database, conf);

    EXPECT_EQ(indexing->Count(), N);
    EXPECT_EQ(indexing->Dim(), dim);

    auto query_dataset = knowhere::GenDataset(num_queries, dim, query_ptr);

    auto result = indexing->Query(query_dataset, conf, nullptr);

    auto ids = knowhere::GetDatasetIDs(result);       // for comparison
    auto dis = knowhere::GetDatasetDistance(result);  // for comparison
    std::vector<int64_t> vec_ids(ids, ids + topK * num_queries);
    std::vector<float> vec_dis(dis, dis + topK * num_queries);

    sr->seg_offsets_ = vec_ids;
    sr->distances_ = vec_dis;
    auto ref_result = SearchResultToJson(*sr);

    LoadIndexInfo load_info;
    load_info.field_id = fake_id.get();
    load_info.index = indexing;
    load_info.index_params["metric_type"] = "L2";

    // load index for vec field, load raw data for scalar filed
    auto sealed_segment = SealedCreator(schema, dataset);
    sealed_segment->DropFieldData(fake_id);
    sealed_segment->LoadIndex(load_info);

    sr = sealed_segment->Search(plan.get(), ph_group.get(), time);

    auto post_result = SearchResultToJson(*sr);
    std::cout << "ref_result" << std::endl;
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
    auto metric_type = knowhere::metric::L2;
    auto fake_id = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
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
    auto vec_col = dataset.get_col<float>(fake_id);
    auto query_ptr = vec_col.data() + 42000 * dim;
    auto segment = CreateGrowingSegment(schema);
    segment->PreInsert(N);
    segment->Insert(0, N, dataset.get_raw_row_ids(), dataset.get_raw_timestamps(), dataset.raw_.get());

    auto plan = CreatePlan(*schema, dsl);
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroupFromBlob(num_queries, 16, query_ptr);
    auto ph_group = ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    Timestamp time = 10000000;
    std::vector<const PlaceholderGroup*> ph_group_arr = {ph_group.get()};

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    auto indexing = std::make_shared<knowhere::IVF>();

    auto conf = knowhere::Config{{knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
                                 {knowhere::meta::DIM, dim},
                                 {knowhere::meta::TOPK, topK},
                                 {knowhere::indexparam::NLIST, 100},
                                 {knowhere::indexparam::NPROBE, 10},
                                 {knowhere::meta::DEVICE_ID, 0}};

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

    // load index for vec field, load raw data for scalar filed
    auto sealed_segment = SealedCreator(schema, dataset);
    sealed_segment->DropFieldData(fake_id);
    sealed_segment->LoadIndex(load_info);

    sr = sealed_segment->Search(plan.get(), ph_group.get(), time);

    for (int i = 0; i < num_queries; ++i) {
        auto offset = i * topK;
        ASSERT_EQ(sr->seg_offsets_[offset], 42000 + i);
        ASSERT_EQ(sr->distances_[offset], 0.0);
    }
}

TEST(Sealed, LoadFieldData) {
    auto dim = 16;
    auto topK = 5;
    auto N = ROW_COUNT;
    auto metric_type = knowhere::metric::L2;
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto counter_id = schema->AddDebugField("counter", DataType::INT64);
    auto double_id = schema->AddDebugField("double", DataType::DOUBLE);
    auto nothing_id = schema->AddDebugField("nothing", DataType::INT32);
    auto str_id = schema->AddDebugField("str", DataType::VARCHAR, 100);
    schema->set_primary_field_id(counter_id);

    auto dataset = DataGen(schema, N);

    auto fakevec = dataset.get_col<float>(fakevec_id);

    auto indexing = GenVecIndexing(N, dim, fakevec.data());

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

    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), time));

    SealedLoadFieldData(dataset, *segment);
    segment->DropFieldData(nothing_id);
    segment->Search(plan.get(), ph_group.get(), time);

    segment->DropFieldData(fakevec_id);
    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), time));

    LoadIndexInfo vec_info;
    vec_info.field_id = fakevec_id.get();
    vec_info.index = indexing;
    vec_info.index_params["metric_type"] = knowhere::metric::L2;
    segment->LoadIndex(vec_info);

    ASSERT_EQ(segment->num_chunk(), 1);
    ASSERT_EQ(segment->num_chunk_index(double_id), 0);
    ASSERT_EQ(segment->num_chunk_index(str_id), 0);
    auto chunk_span1 = segment->chunk_data<int64_t>(counter_id, 0);
    auto chunk_span2 = segment->chunk_data<double>(double_id, 0);
    auto chunk_span3 = segment->chunk_data<std::string>(str_id, 0);
    auto ref1 = dataset.get_col<int64_t>(counter_id);
    auto ref2 = dataset.get_col<double>(double_id);
    auto ref3 = arrow::StringArray(dataset.get_data_array(str_id).data->data());
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(chunk_span1[i], ref1[i]);
        ASSERT_EQ(chunk_span2[i], ref2[i]);
        ASSERT_EQ(chunk_span3[i], ref3[i]);
    }

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    auto json = SearchResultToJson(*sr);
    std::cout << json.dump(1);

    segment->DropIndex(fakevec_id);
    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), time));
    segment->LoadIndex(vec_info);
    auto sr2 = segment->Search(plan.get(), ph_group.get(), time);
    auto json2 = SearchResultToJson(*sr);
    ASSERT_EQ(json.dump(-2), json2.dump(-2));
    segment->DropFieldData(double_id);
    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), time));
#ifdef __linux__
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
#else  // for mac
    auto std_json = Json::parse(R"(
[
	[
        ["982->0.000000", "31864->4.270000", "18916->4.651000", "71547->5.125000", "86706->5.991000"],
        ["96984->4.192000", "65514->6.011000", "89328->6.138000", "80284->6.526000", "68218->6.563000"],
        ["30119->2.464000", "82365->4.725000", "74834->5.009000", "79995->5.725000", "33359->5.816000"],
        ["99625->6.129000", "86582->6.900000", "85934->7.792000", "60450->8.087000", "19257->8.530000"],
        ["37759->3.581000", "31292->5.780000", "98124->6.216000", "63535->6.439000", "11707->6.553000"]
    ]
])");
#endif
    ASSERT_EQ(std_json.dump(-2), json.dump(-2));
}

TEST(Sealed, LoadScalarIndex) {
    auto dim = 16;
    auto N = ROW_COUNT;
    auto metric_type = knowhere::metric::L2;
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto counter_id = schema->AddDebugField("counter", DataType::INT64);
    auto double_id = schema->AddDebugField("double", DataType::DOUBLE);
    auto nothing_id = schema->AddDebugField("nothing", DataType::INT32);
    schema->set_primary_field_id(counter_id);

    auto dataset = DataGen(schema, N);

    auto fakevec = dataset.get_col<float>(fakevec_id);

    auto indexing = GenVecIndexing(N, dim, fakevec.data());

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

    LoadFieldDataInfo row_id_info;
    FieldMeta row_id_field_meta(FieldName("RowID"), RowFieldID, DataType::INT64);
    row_id_info.field_data = new DataArray{ToArrowField(row_id_field_meta), dataset.row_ids_};
    row_id_info.row_count = dataset.row_ids_->length();
    row_id_info.field_id = RowFieldID.get();  // field id for RowId
    segment->LoadFieldData(row_id_info);

    LoadFieldDataInfo ts_info;
    FieldMeta ts_field_meta(FieldName("Timestamp"), TimestampFieldID, DataType::INT64);
    ts_info.field_data = new DataArray{ToArrowField(ts_field_meta), dataset.timestamps_};
    ts_info.row_count = dataset.timestamps_->length();
    ts_info.field_id = TimestampFieldID.get();
    segment->LoadFieldData(ts_info);

    LoadIndexInfo vec_info;
    vec_info.field_id = fakevec_id.get();
    vec_info.field_type = CDataType::FloatVector;
    vec_info.index = indexing;
    vec_info.index_params["metric_type"] = knowhere::metric::L2;
    segment->LoadIndex(vec_info);

    LoadIndexInfo counter_index;
    counter_index.field_id = counter_id.get();
    counter_index.field_type = CDataType::Int64;
    counter_index.index_params["index_type"] = "sort";
    auto counter_data = dataset.get_col<int64_t>(counter_id);
    counter_index.index = std::move(GenScalarIndexing<int64_t>(N, counter_data.data()));
    segment->LoadIndex(counter_index);

    LoadIndexInfo double_index;
    double_index.field_id = double_id.get();
    double_index.field_type = CDataType::Double;
    double_index.index_params["index_type"] = "sort";
    auto double_data = dataset.get_col<double>(double_id);
    double_index.index = std::move(GenScalarIndexing<double>(N, double_data.data()));
    segment->LoadIndex(double_index);

    LoadIndexInfo nothing_index;
    nothing_index.field_id = nothing_id.get();
    nothing_index.field_type = CDataType::Int32;
    nothing_index.index_params["index_type"] = "sort";
    auto nothing_data = dataset.get_col<int32_t>(nothing_id);
    nothing_index.index = std::move(GenScalarIndexing<int32_t>(N, nothing_data.data()));
    segment->LoadIndex(nothing_index);

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    auto json = SearchResultToJson(*sr);
    std::cout << json.dump(1);
}

TEST(Sealed, Delete) {
    auto dim = 16;
    auto topK = 5;
    auto N = 10;
    auto metric_type = knowhere::metric::L2;
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto counter_id = schema->AddDebugField("counter", DataType::INT64);
    auto double_id = schema->AddDebugField("double", DataType::DOUBLE);
    auto nothing_id = schema->AddDebugField("nothing", DataType::INT32);
    schema->set_primary_field_id(counter_id);

    auto dataset = DataGen(schema, N);

    auto fakevec = dataset.get_col<float>(fakevec_id);

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

    ASSERT_ANY_THROW(segment->Search(plan.get(), ph_group.get(), time));

    SealedLoadFieldData(dataset, *segment);

    int64_t row_count = 5;
    std::vector<idx_t> pks{1, 2, 3, 4, 5};
    auto ids_builder = arrow::Int64Builder();
    ids_builder.AppendValues(pks);
    auto ids = ids_builder.Finish().ValueOrDie();
    std::vector<Timestamp> timestamps{10, 10, 10, 10, 10};

    LoadDeletedRecordInfo info = {timestamps.data(), ids.get(), row_count};
    segment->LoadDeletedRecord(info);

    std::vector<uint8_t> tmp_block{0, 0};
    BitsetType bitset(N, false);
    segment->mask_with_delete(bitset, 10, 11);
    ASSERT_EQ(bitset.count(), pks.size());

    int64_t new_count = 3;
    std::vector<idx_t> new_pks{6, 7, 8};
    auto new_ids_builder = arrow::Int64Builder();
    new_ids_builder.AppendValues(new_pks);
    auto new_ids = new_ids_builder.Finish().ValueOrDie();
    std::vector<idx_t> new_timestamps{10, 10, 10};
    auto reserved_offset = segment->PreDelete(new_count);
    ASSERT_EQ(reserved_offset, row_count);
    segment->Delete(reserved_offset, new_count, new_ids.get(),
                    reinterpret_cast<const Timestamp*>(new_timestamps.data()));
}

auto
GenMaxFloatVecs(int N, int dim) {
    std::vector<float> vecs;
    for (int i = 0; i < N; i++) {
        for (int j = 0; j < dim; j++) {
            vecs.push_back(std::numeric_limits<float>::max());
        }
    }
    return vecs;
}

auto
GenRandomFloatVecs(int N, int dim) {
    std::vector<float> vecs;
    srand(time(NULL));
    for (int i = 0; i < N; i++) {
        for (int j = 0; j < dim; j++) {
            vecs.push_back(static_cast<float>(rand()) / static_cast<float>(RAND_MAX));
        }
    }
    return vecs;
}

auto
GenQueryVecs(int N, int dim) {
    std::vector<float> vecs;
    for (int i = 0; i < N; i++) {
        for (int j = 0; j < dim; j++) {
            vecs.push_back(1);
        }
    }
    return vecs;
}

auto
transfer_to_fields_data(const std::vector<float>& vecs, const FieldMeta& field_meta, const int dim) {
    auto arr_builder = arrow::FixedSizeBinaryBuilder(arrow::fixed_size_binary(dim * 4));
    for (int n = 0; n < vecs.size() / dim; ++n) {
        std::vector<float> data(dim);

        for (int i = 0; i < dim; i++) {
            data[i] = vecs[n * dim + i];
        }
        arr_builder.Append((const uint8_t*)data.data());
    }
    return DataArray{ToArrowField(field_meta), arr_builder.Finish().ValueOrDie()};
}

TEST(Sealed, BF) {
    auto schema = std::make_shared<Schema>();
    auto dim = 128;
    auto metric_type = "L2";
    auto fake_id = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    int64_t N = 100000;
    auto base = GenRandomFloatVecs(N, dim);
    auto meta = schema->get_fields().at(fake_id);
    auto base_arr = transfer_to_fields_data(base, meta, dim);

    LoadFieldDataInfo load_info{100, &base_arr, N};

    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    std::cout << fake_id.get() << std::endl;
    SealedLoadFieldData(dataset, *segment, {fake_id.get()});

    segment->LoadFieldData(load_info);

    auto topK = 1;
    auto fmt = boost::format(R"(vector_anns: <
                                            field_id: 100
                                            query_info: <
                                                topk: %1%
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0">
                                            output_field_ids: 101)") %
               topK;
    auto serialized_expr_plan = fmt.str();
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan.data());
    auto plan = CreateSearchPlanByExpr(*schema, binary_plan.data(), binary_plan.size());

    auto num_queries = 10;
    auto query = GenQueryVecs(num_queries, dim);
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, query);
    auto ph_group = ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto result = segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
    auto ves = SearchResultToVector(*result);
    // first: offset, second: distance
    EXPECT_GT(ves[0].first, 0);
    EXPECT_LE(ves[0].first, N);
    EXPECT_LE(ves[0].second, dim);
}

TEST(Sealed, BF_Overflow) {
    auto schema = std::make_shared<Schema>();
    auto dim = 128;
    auto metric_type = "L2";
    auto fake_id = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    int64_t N = 10;
    FieldMeta meta = schema->get_fields().at(fake_id);
    auto base = GenMaxFloatVecs(N, dim);
    auto data_array = transfer_to_fields_data(base, meta, dim);
    LoadFieldDataInfo load_info{100, &data_array, N};
    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    std::cout << fake_id.get() << std::endl;
    SealedLoadFieldData(dataset, *segment, {fake_id.get()});

    segment->LoadFieldData(load_info);

    auto topK = 1;
    auto fmt = boost::format(R"(vector_anns: <
                                            field_id: 100
                                            query_info: <
                                                topk: %1%
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0">
                                            output_field_ids: 101)") %
               topK;
    auto serialized_expr_plan = fmt.str();
    auto binary_plan = translate_text_plan_to_binary_plan(serialized_expr_plan.data());
    auto plan = CreateSearchPlanByExpr(*schema, binary_plan.data(), binary_plan.size());

    auto num_queries = 10;
    auto query = GenQueryVecs(num_queries, dim);
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, query);
    auto ph_group = ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto result = segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
    auto ves = SearchResultToVector(*result);
    for (int i = 0; i < num_queries; ++i) {
        EXPECT_EQ(ves[0].first, -1);
    }
}

TEST(Sealed, DeleteCount) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateSealedSegment(schema);

    int64_t c = 10;
    auto offset = segment->PreDelete(c);
    ASSERT_EQ(offset, 0);

    Timestamp begin_ts = 100;
    auto tss = GenTss(c, begin_ts);
    auto pks = GenPKs(c, 0);
    auto status = segment->Delete(offset, c, pks.get(), tss.data());
    ASSERT_TRUE(status.ok());

    auto cnt = segment->get_deleted_count();
    ASSERT_EQ(cnt, c);
}

TEST(Sealed, RealCount) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateSealedSegment(schema);

    int64_t c = 10;
    auto dataset = DataGen(schema, c);
    auto pks = dataset.get_col<int64_t>(pk);
    SealedLoadFieldData(dataset, *segment);

    // no delete.
    ASSERT_EQ(c, segment->get_real_count());

    // delete half.
    auto half = c / 2;
    auto del_offset1 = segment->PreDelete(half);
    ASSERT_EQ(del_offset1, 0);
    auto del_ids1 = GenPKs(pks.begin(), pks.begin() + half);
    auto del_tss1 = GenTss(half, c);
    auto status = segment->Delete(del_offset1, half, del_ids1.get(), del_tss1.data());
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(c - half, segment->get_real_count());

    // delete duplicate.
    auto del_offset2 = segment->PreDelete(half);
    ASSERT_EQ(del_offset2, half);
    auto del_tss2 = GenTss(half, c + half);
    status = segment->Delete(del_offset2, half, del_ids1.get(), del_tss2.data());
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(c - half, segment->get_real_count());

    // delete all.
    auto del_offset3 = segment->PreDelete(c);
    ASSERT_EQ(del_offset3, half * 2);
    auto del_ids3 = GenPKs(pks.begin(), pks.end());
    auto del_tss3 = GenTss(c, c + half * 2);
    status = segment->Delete(del_offset3, c, del_ids3.get(), del_tss3.data());
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(0, segment->get_real_count());
}
