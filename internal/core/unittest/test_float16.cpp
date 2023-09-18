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

#include <gtest/gtest.h>

#include "common/LoadInfo.h"
#include "common/Types.h"
#include "index/IndexFactory.h"
#include "knowhere/comp/index_param.h"
#include "query/ExprImpl.h"
#include "segcore/Reduce.h"
#include "segcore/reduce_c.h"
#include "test_utils/PbHelper.h"
#include "test_utils/indexbuilder_test_utils.h"

#include "pb/schema.pb.h"
#include "pb/plan.pb.h"
#include "query/Expr.h"
#include "query/Plan.h"
#include "query/Utils.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "query/PlanProto.h"
#include "query/SearchBruteForce.h"
#include "query/generated/ExecPlanNodeVisitor.h"
#include "query/generated/PlanNodeVisitor.h"
#include "query/generated/ExecExprVisitor.h"
#include "query/generated/ExprVisitor.h"
#include "query/generated/ShowPlanNodeVisitor.h"
#include "segcore/Collection.h"
#include "segcore/SegmentSealed.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/AssertUtils.h"
#include "test_utils/DataGen.h"

using namespace milvus::segcore;
using namespace milvus;
using namespace milvus::index;
using namespace knowhere;
using milvus::index::VectorIndex;
using milvus::segcore::LoadIndexInfo;

const int64_t ROW_COUNT = 100 * 1000;

TEST(Float16, Insert) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    int64_t N = ROW_COUNT;
    constexpr int64_t size_per_chunk = 32 * 1024;
    auto schema = std::make_shared<Schema>();
    auto float16_vec_fid = schema->AddDebugField(
        "float16vec", DataType::VECTOR_FLOAT16, 32, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto dataset = DataGen(schema, N);
    // auto seg_conf = SegcoreConfig::default_config();
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);
    auto float16_ptr = dataset.get_col<float16>(float16_vec_fid);
    SegmentInternalInterface& interface = *segment;
    auto num_chunk = interface.num_chunk();
    ASSERT_EQ(num_chunk, upper_div(N, size_per_chunk));
    auto row_count = interface.get_row_count();
    ASSERT_EQ(N, row_count);
    for (auto chunk_id = 0; chunk_id < num_chunk; ++chunk_id) {
        auto float16_span = interface.chunk_data<milvus::Float16Vector>(
            float16_vec_fid, chunk_id);
        auto begin = chunk_id * size_per_chunk;
        auto end = std::min((chunk_id + 1) * size_per_chunk, N);
        auto size_of_chunk = end - begin;
        for (int i = 0; i < size_of_chunk; ++i) {
            // std::cout << float16_span.data()[i] << " " << float16_ptr[i + begin * 32] << std::endl;
            ASSERT_EQ(float16_span.data()[i], float16_ptr[i + begin * 32]);
        }
    }
}

TEST(Float16, ShowExecutor) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    using namespace milvus;
    auto metric_type = knowhere::metric::L2;
    auto node = std::make_unique<Float16VectorANNS>();
    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT16, 16, metric_type);
    int64_t num_queries = 100L;
    auto raw_data = DataGen(schema, num_queries);
    auto& info = node->search_info_;
    info.metric_type_ = metric_type;
    info.topk_ = 20;
    info.field_id_ = field_id;
    node->predicate_ = std::nullopt;
    ShowPlanNodeVisitor show_visitor;
    PlanNodePtr base(node.release());
    auto res = show_visitor.call_child(*base);
    auto dup = res;
    std::cout << dup.dump(4);
}

TEST(Float16, ExecWithoutPredicateFlat) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    using namespace milvus;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT16, 32, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    query_info: <
                                      topk: 5
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
        >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);
    auto vec_ptr = dataset.get_col<float16>(vec_fid);

    auto num_queries = 5;
    auto ph_group_raw = CreateFloat16PlaceholderGroup(num_queries, 32, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto sr = segment->Search(plan.get(), ph_group.get());
    int topk = 5;

    query::Json json = SearchResultToJson(*sr);
    std::cout << json.dump(2);
}

TEST(Float16, GetVector) {
    auto metricType = knowhere::metric::L2;
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto random = schema->AddDebugField("random", DataType::DOUBLE);
    auto vec = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT16, 128, metricType);
    schema->set_primary_field_id(pk);
    std::map<std::string, std::string> index_params = {
        {"index_type", "IVF_FLAT"},
        {"metric_type", metricType},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    auto& config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_growing_segment_index(true);
    std::map<FieldId, FieldIndexMeta> filedMap = {{vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_growing = CreateGrowingSegment(schema, metaPtr);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    int64_t per_batch = 5000;
    int64_t n_batch = 20;
    int64_t dim = 128;
    for (int64_t i = 0; i < n_batch; i++) {
        auto dataset = DataGen(schema, per_batch);
        auto fakevec = dataset.get_col<float16>(vec);
        auto offset = segment->PreInsert(per_batch);
        segment->Insert(offset,
                        per_batch,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);
        auto num_inserted = (i + 1) * per_batch;
        auto ids_ds = GenRandomIds(num_inserted);
        auto result =
            segment->bulk_subscript(vec, ids_ds->GetIds(), num_inserted);

        auto vector = result.get()->mutable_vectors()->float16_vector();
        EXPECT_TRUE(vector.size() == num_inserted * dim * sizeof(float16));
        // EXPECT_TRUE(vector.size() == num_inserted * dim);
        // for (size_t i = 0; i < num_inserted; ++i) {
        //     auto id = ids_ds->GetIds()[i];
        //     for (size_t j = 0; j < 128; ++j) {
        //         EXPECT_TRUE(vector[i * dim + j] ==
        //                     fakevec[(id % per_batch) * dim + j]);
        //     }
        // }
    }
}

std::string
generate_collection_schema(std::string metric_type, int dim, bool is_fp16) {
    namespace schema = milvus::proto::schema;
    schema::CollectionSchema collection_schema;
    collection_schema.set_name("collection_test");

    auto vec_field_schema = collection_schema.add_fields();
    vec_field_schema->set_name("fakevec");
    vec_field_schema->set_fieldid(100);
    if (is_fp16) {
        vec_field_schema->set_data_type(schema::DataType::Float16Vector);
    } else {
        vec_field_schema->set_data_type(schema::DataType::FloatVector);
    }
    auto metric_type_param = vec_field_schema->add_index_params();
    metric_type_param->set_key("metric_type");
    metric_type_param->set_value(metric_type);
    auto dim_param = vec_field_schema->add_type_params();
    dim_param->set_key("dim");
    dim_param->set_value(std::to_string(dim));

    auto other_field_schema = collection_schema.add_fields();
    other_field_schema->set_name("counter");
    other_field_schema->set_fieldid(101);
    other_field_schema->set_data_type(schema::DataType::Int64);
    other_field_schema->set_is_primary_key(true);

    auto other_field_schema2 = collection_schema.add_fields();
    other_field_schema2->set_name("doubleField");
    other_field_schema2->set_fieldid(102);
    other_field_schema2->set_data_type(schema::DataType::Double);

    std::string schema_string;
    auto marshal = google::protobuf::TextFormat::PrintToString(
        collection_schema, &schema_string);
    assert(marshal);
    return schema_string;
}

CCollection
NewCollection(const char* schema_proto_blob) {
    auto proto = std::string(schema_proto_blob);
    auto collection = std::make_unique<milvus::segcore::Collection>(proto);
    return (void*)collection.release();
}

TEST(Float16, CApiCPlan) {
    std::string schema_string =
        generate_collection_schema(knowhere::metric::L2, 16, true);
    auto collection = NewCollection(schema_string.c_str());

    //  const char* dsl_string = R"(
    //  {
    //      "bool": {
    //          "vector": {
    //              "fakevec": {
    //                  "metric_type": "L2",
    //                  "params": {
    //                      "nprobe": 10
    //                  },
    //                  "query": "$0",
    //                  "topk": 10,
    //                  "round_decimal": 3
    //             }
    //          }
    //      }
    // })";

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(
        milvus::proto::plan::VectorType::Float16Vector);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(100);
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_round_decimal(3);
    query_info->set_metric_type("L2");
    query_info->set_search_params(R"({"nprobe": 10})");
    auto plan_str = plan_node.SerializeAsString();

    void* plan = nullptr;
    auto status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    int64_t field_id = -1;
    status = GetFieldID(plan, &field_id);
    ASSERT_EQ(status.error_code, Success);

    auto col = static_cast<Collection*>(collection);
    for (auto& [target_field_id, field_meta] :
         col->get_schema()->get_fields()) {
        if (field_meta.is_vector()) {
            ASSERT_EQ(field_id, target_field_id.get());
        }
    }
    ASSERT_NE(field_id, -1);

    DeleteSearchPlan(plan);
    DeleteCollection(collection);
}

TEST(Float16, RetrieveEmpty) {
    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    auto DIM = 16;
    auto fid_vec = schema->AddDebugField(
        "vector_64", DataType::VECTOR_FLOAT16, DIM, knowhere::metric::L2);
    schema->set_primary_field_id(fid_64);

    int64_t N = 100;
    int64_t req_size = 10;
    auto choose = [=](int i) { return i * 3 % N; };

    auto segment = CreateSealedSegment(schema);

    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    std::vector<int64_t> values;
    for (int i = 0; i < req_size; ++i) {
        values.emplace_back(choose(i));
    }
    auto term_expr = std::make_unique<query::TermExprImpl<int64_t>>(
        milvus::query::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        values,
        proto::plan::GenericValue::kInt64Val);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->predicate_ = std::move(term_expr);
    std::vector<FieldId> target_offsets{fid_64, fid_vec};
    plan->field_ids_ = target_offsets;

    auto retrieve_results =
        segment->Retrieve(plan.get(), 100, DEFAULT_MAX_OUTPUT_SIZE);

    Assert(retrieve_results->fields_data_size() == target_offsets.size());
    auto field0 = retrieve_results->fields_data(0);
    auto field1 = retrieve_results->fields_data(1);
    Assert(field0.has_scalars());
    auto field0_data = field0.scalars().long_data();
    Assert(field0_data.data_size() == 0);
    Assert(field1.vectors().float16_vector().size() == 0);
}

TEST(Float16, ExecWithPredicate) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT16, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Float
                                        >
                                        lower_inclusive: true,
                                        upper_inclusive: false,
                                        lower_value: <
                                          float_val: -1
                                        >
                                        upper_value: <
                                          float_val: 1
                                        >
                                      >
                                    >
                                    query_info: <
                                      topk: 5
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreateFloat16PlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto sr = segment->Search(plan.get(), ph_group.get());
    int topk = 5;

    query::Json json = SearchResultToJson(*sr);
    std::cout << json.dump(2);
}