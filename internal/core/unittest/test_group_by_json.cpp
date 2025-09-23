#include "gtest/gtest.h"
#include "test_utils/c_api_test_utils.h"
#include "test_utils/storage_test_utils.h"
#include "test_utils/cachinglayer_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
using namespace milvus::storage;
using namespace milvus::tracer;

static std::unique_ptr<SearchResult>
run_group_by_search(const std::string& raw_plan,
                    const SchemaPtr& schema,
                    SegmentInternalInterface* segment,
                    int dim,
                    int topK,
                    int num_queries = 1) {
    proto::plan::PlanNode plan_node;
    auto ok =
        google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
    auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
    auto seed = 1024;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto search_result = segment->Search(plan.get(), ph_group.get(), 1L << 63);
    CheckGroupBySearchResult(*search_result, topK, num_queries, false);
    return search_result;
}

template <typename T>
void
validate_group_by_search_result(
    const std::vector<GroupByValueType>& group_by_values,
    const std::vector<float>& distances,
    int group_size,
    int topK,
    int num_queries = 1) {
    for (int i = 0; i < num_queries; i++) {
        int nq_start = i * topK * group_size;
        int nq_end = nq_start + topK * group_size;
        std::unordered_map<T, int> value_map;
        float lastDistance = 0.0;
        for (size_t j = nq_start; j < nq_end; j++) {
            if (std::holds_alternative<T>(group_by_values[j].value())) {
                T value = std::get<T>(group_by_values[j].value());
                value_map[value] += 1;
                ASSERT_TRUE(value_map[value] <= group_size);
                auto distance = distances.at(j);
                ASSERT_TRUE(lastDistance <= distance);
                lastDistance = distance;
            }
        }
        ASSERT_EQ(value_map.size(), topK);
    }
}

TEST(GroupBYJSON, SealedData) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    // 0. prepare schema
    int dim = 64;
    auto schema = std::make_shared<Schema>();
    auto str_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto json_fid = schema->AddDebugField("json_field", DataType::JSON);
    schema->set_primary_field_id(str_fid);
    size_t N = 100;

    // 1. load raw data
    auto raw_data = DataGen(schema, N, 42, 0, 1, 10, 10);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    SegmentInternalInterface* segment_ptr = segment.get();

    // 2. load index
    auto vector_data = raw_data.get_col<float>(vec_fid);
    auto indexing = GenVecIndexing(
        N, dim, vector_data.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo load_index_info;
    load_index_info.field_id = vec_fid.get();
    load_index_info.index_params = GenIndexParams(indexing.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(indexing));
    load_index_info.index_params["metric_type"] = knowhere::metric::L2;
    segment->LoadIndex(load_index_info);

    int topK = 10;
    int group_size = 2;

    // 3. search group by json_field.int8
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 101
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 102
                                          group_size: 2
                                          json_path: "/int8"
                                          json_type: Int8
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result =
            run_group_by_search(raw_plan, schema, segment_ptr, dim, topK);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<int8_t>(
            group_by_values, search_result->distances_, group_size, topK, 1);
    }

    // 4. search group by json_field.int16
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 101
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 102
                                          group_size: 2
                                          json_path: "/int16"
                                          json_type: Int16
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result =
            run_group_by_search(raw_plan, schema, segment_ptr, dim, topK);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<int16_t>(
            group_by_values, search_result->distances_, group_size, topK, 1);
    }

    // 5. search group by json_field.int32
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 101
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 102
                                          group_size: 2
                                          json_path: "/int32"
                                          json_type: Int32
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result =
            run_group_by_search(raw_plan, schema, segment_ptr, dim, topK);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<int32_t>(
            group_by_values, search_result->distances_, group_size, topK, 1);
    }

    // 6. search group by json_field.int64
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 101
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 102
                                          group_size: 2
                                          json_path: "/int64"
                                          json_type: Int64
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result =
            run_group_by_search(raw_plan, schema, segment_ptr, dim, topK);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<int64_t>(
            group_by_values, search_result->distances_, group_size, topK, 1);
    }

    // 7. search group by json_field.bool
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 101
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 102
                                          group_size: 2
                                          json_path: "/bool"
                                          json_type: Bool
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result =
            run_group_by_search(raw_plan, schema, segment_ptr, dim, topK);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(2 * group_size, group_by_values.size());
        validate_group_by_search_result<bool>(
            group_by_values, search_result->distances_, group_size, 2, 1);
    }

    // 8. search group by json_field.string
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 101
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 102
                                          group_size: 2
                                          json_path: "/string"
                                          json_type: VarChar
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result =
            run_group_by_search(raw_plan, schema, segment_ptr, dim, topK);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<std::string>(
            group_by_values, search_result->distances_, group_size, topK, 1);
    }

    // 9. search group by json_field.string without json_type
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 101
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 102
                                          group_size: 2
                                          json_path: "/string"
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result =
            run_group_by_search(raw_plan, schema, segment_ptr, dim, topK);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<std::string>(
            group_by_values, search_result->distances_, group_size, topK, 1);
    }
}

TEST(GroupBYJSON, GrowingRawData) {
    // 0. set up growing segment
    int dim = 64;
    uint64_t seed = 512;
    auto schema = std::make_shared<Schema>();
    auto metric_type = knowhere::metric::L2;
    auto str_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto json_fid = schema->AddDebugField("json_field", DataType::JSON);
    auto vec_fid = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT, dim, metric_type);
    schema->set_primary_field_id(str_fid);

    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(128);
    config.set_enable_interim_segment_index(
        false);  // no growing index, test brute force
    auto segment_growing = CreateGrowingSegment(schema, nullptr, 1, config);
    auto segment_growing_impl =
        dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    // 1. prepare raw data in growing segment
    int64_t N = 100;
    auto data_set = DataGen(schema, N, 42, 0, 1, 10, 10, false, false);
    auto offset = segment_growing_impl->PreInsert(N);
    segment_growing_impl->Insert(offset,
                                 N,
                                 data_set.row_ids_.data(),
                                 data_set.timestamps_.data(),
                                 data_set.raw_);

    // 2. Search group by json_field.int8
    auto num_queries = 10;
    auto topK = 10;
    int group_size = 2;
    const char* raw_plan = R"(vector_anns: <
                                        field_id: 102
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 101
                                          group_size: 2
                                          json_path: "/int8"
                                          json_type: Int8,
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
    auto search_result = run_group_by_search(
        raw_plan, schema, segment_growing_impl, dim, topK, num_queries);
    auto& group_by_values = search_result->group_by_values_.value();
    ASSERT_EQ(group_by_values.size(), num_queries * topK * group_size);
    validate_group_by_search_result<int8_t>(group_by_values,
                                            search_result->distances_,
                                            group_size,
                                            topK,
                                            num_queries);

    // 3. Search group by json_field.int16
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 102
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 101
                                          group_size: 2
                                          json_path: "/int16"
                                          json_type: Int16,
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(
            raw_plan, schema, segment_growing_impl, dim, topK, num_queries);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(group_by_values.size(), num_queries * topK * group_size);
        validate_group_by_search_result<int16_t>(group_by_values,
                                                 search_result->distances_,
                                                 group_size,
                                                 topK,
                                                 num_queries);
    }

    // 4. Search group by json_field.int32
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 102
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 101
                                          group_size: 2
                                          json_path: "/int32"
                                          json_type: Int32,
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(
            raw_plan, schema, segment_growing_impl, dim, topK, num_queries);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(group_by_values.size(), num_queries * topK * group_size);
        validate_group_by_search_result<int32_t>(group_by_values,
                                                 search_result->distances_,
                                                 group_size,
                                                 topK,
                                                 num_queries);
    }

    // 5. Search group by json_field.int64
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 102
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 101
                                          group_size: 2
                                          json_path: "/int64"
                                          json_type: Int64,
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(
            raw_plan, schema, segment_growing_impl, dim, topK, num_queries);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(group_by_values.size(), num_queries * topK * group_size);
        validate_group_by_search_result<int64_t>(group_by_values,
                                                 search_result->distances_,
                                                 group_size,
                                                 topK,
                                                 num_queries);
    }

    // 6. Search group by json_field.bool
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 102
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 101
                                          group_size: 2
                                          json_path: "/bool"
                                          json_type: Bool,
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(
            raw_plan, schema, segment_growing_impl, dim, topK, num_queries);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(group_by_values.size(), num_queries * 2 * group_size);
        validate_group_by_search_result<bool>(group_by_values,
                                              search_result->distances_,
                                              group_size,
                                              2,
                                              num_queries);
    }

    // 7. Search group by json_field.string
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 102
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 101
                                          group_size: 2
                                          json_path: "/string"
                                          json_type: VarChar,
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(
            raw_plan, schema, segment_growing_impl, dim, topK, num_queries);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(group_by_values.size(), num_queries * topK * group_size);
        validate_group_by_search_result<std::string>(group_by_values,
                                                     search_result->distances_,
                                                     group_size,
                                                     topK,
                                                     num_queries);
    }

    // 8. Search group by json_field.string without json_type
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 102
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 101
                                          group_size: 2
                                          json_path: "/string"
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(
            raw_plan, schema, segment_growing_impl, dim, topK, num_queries);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(group_by_values.size(), num_queries * topK * group_size);
        validate_group_by_search_result<std::string>(group_by_values,
                                                     search_result->distances_,
                                                     group_size,
                                                     topK,
                                                     num_queries);
    }

    // 9. Search group by entire JSON object (json_path="")
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 102
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 101
                                          group_size: 1
                                          json_path: ""
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";

        auto search_result = run_group_by_search(
            raw_plan, schema, segment_growing_impl, dim, topK, num_queries);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(group_by_values.size(), num_queries * topK);
        validate_group_by_search_result<std::string>(
            group_by_values, search_result->distances_, 1, topK, num_queries);
    }

    // 10. Search group by with wrong type and strict_cast=true
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 102
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 101
                                          group_size: 2
                                          json_path: "/bool"
                                          json_type: VarChar,
                                          strict_group_size: true,
                                          strict_cast: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        ASSERT_ANY_THROW(run_group_by_search(
            raw_plan, schema, segment_growing_impl, dim, topK, num_queries));
    }
}

TEST(GroupBYJSON, Reduce) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    // 0. prepare schema
    int dim = 64;
    auto schema = std::make_shared<Schema>();
    auto str_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto json_fid = schema->AddDebugField("json_field", DataType::JSON);
    schema->set_primary_field_id(str_fid);

    // 1. load raw data
    size_t N = 100;
    uint64_t seed = 512;
    uint64_t ts_offset = 0;
    int repeat_count_1 = 2;
    int repeat_count_2 = 5;
    auto raw_data1 =
        DataGen(schema, N, seed, ts_offset, repeat_count_1, 10, 10);
    auto raw_data2 =
        DataGen(schema, N, seed, ts_offset, repeat_count_2, 10, 10);

    auto segment1 = CreateSealedWithFieldDataLoaded(schema, raw_data1);
    auto segment2 = CreateSealedWithFieldDataLoaded(schema, raw_data2);

    // 2. load index
    auto vector_data_1 = raw_data1.get_col<float>(vec_fid);
    auto indexing_1 = GenVecIndexing(
        N, dim, vector_data_1.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo load_index_info_1;
    load_index_info_1.field_id = vec_fid.get();
    load_index_info_1.index_params = GenIndexParams(indexing_1.get());
    load_index_info_1.cache_index =
        CreateTestCacheIndex("test", std::move(indexing_1));
    load_index_info_1.index_params["metric_type"] = knowhere::metric::L2;
    segment1->LoadIndex(load_index_info_1);

    auto vector_data_2 = raw_data2.get_col<float>(vec_fid);
    auto indexing_2 = GenVecIndexing(
        N, dim, vector_data_2.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo load_index_info_2;
    load_index_info_2.field_id = vec_fid.get();
    load_index_info_2.index_params = GenIndexParams(indexing_2.get());
    load_index_info_2.cache_index =
        CreateTestCacheIndex("test", std::move(indexing_2));
    load_index_info_2.index_params["metric_type"] = knowhere::metric::L2;
    segment2->LoadIndex(load_index_info_2);

    CSegmentInterface c_segment_1 = segment1.release();
    CSegmentInterface c_segment_2 = segment2.release();
    // 3. search group by respectively
    auto num_queries = 10;
    auto topK = 10;
    int group_size = 3;
    auto slice_nqs = std::vector<int64_t>{num_queries / 2, num_queries / 2};
    auto slice_topKs = std::vector<int64_t>{topK / 2, topK};

    // Lambda function to execute search and reduce with given raw plan for JSON
    auto executeJSONGroupBySearchAndReduce = [&](const char* raw_plan) {
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        CPlaceholderGroup c_ph_group = ph_group.release();
        CSearchPlan c_plan = plan.release();

        CSearchResult c_search_res_1;
        CSearchResult c_search_res_2;
        auto status =
            CSearch(c_segment_1, c_plan, c_ph_group, 1L << 63, &c_search_res_1);
        ASSERT_EQ(status.error_code, Success);
        status =
            CSearch(c_segment_2, c_plan, c_ph_group, 1L << 63, &c_search_res_2);
        ASSERT_EQ(status.error_code, Success);
        std::vector<CSearchResult> results;
        results.push_back(c_search_res_1);
        results.push_back(c_search_res_2);

        CSearchResultDataBlobs cSearchResultData;
        status = ReduceSearchResultsAndFillData({},
                                                &cSearchResultData,
                                                c_plan,
                                                results.data(),
                                                results.size(),
                                                slice_nqs.data(),
                                                slice_topKs.data(),
                                                slice_nqs.size());
        ASSERT_EQ(status.error_code, Success);
        CheckSearchResultDuplicate(results, group_size);
        DeleteSearchResult(c_search_res_1);
        DeleteSearchResult(c_search_res_2);
        DeleteSearchResultDataBlobs(cSearchResultData);
        DeleteSearchPlan(c_plan);
        DeletePlaceholderGroup(c_ph_group);
    };

    // Test Case: Group by JSON field int8
    const char* raw_plan_json_int8 = R"(vector_anns: <
                                        field_id: 101
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 102
                                          group_size: 3
                                          json_path: "/int8"
                                          json_type: Int8
                                        >
                                        placeholder_tag: "$0"
         >)";
    executeJSONGroupBySearchAndReduce(raw_plan_json_int8);

    // Test Case: Group by JSON field int16
    const char* raw_plan_json_int16 = R"(vector_anns: <
                                        field_id: 101
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 102
                                          group_size: 3
                                          json_path: "/int16"
                                          json_type: Int16
                                        >
                                        placeholder_tag: "$0"
         >)";
    executeJSONGroupBySearchAndReduce(raw_plan_json_int16);

    // Test Case: Group by JSON field int32
    const char* raw_plan_json_int32 = R"(vector_anns: <
                                        field_id: 101
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 102
                                          group_size: 3
                                          json_path: "/int32"
                                          json_type: Int32
                                        >
                                        placeholder_tag: "$0"
         >)";
    executeJSONGroupBySearchAndReduce(raw_plan_json_int32);

    // Test Case: Group by JSON field int64
    const char* raw_plan_json_int64 = R"(vector_anns: <
                                        field_id: 101
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 102
                                          group_size: 3
                                          json_path: "/int64"
                                          json_type: Int64
                                        >
                                        placeholder_tag: "$0"
         >)";
    executeJSONGroupBySearchAndReduce(raw_plan_json_int64);

    // Test Case: Group by JSON field bool
    const char* raw_plan_json_bool = R"(vector_anns: <
                                        field_id: 101
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 102
                                          group_size: 3
                                          json_path: "/bool"
                                          json_type: Bool
                                        >
                                        placeholder_tag: "$0"
         >)";
    executeJSONGroupBySearchAndReduce(raw_plan_json_bool);

    // Test Case: Group by JSON field string
    const char* raw_plan_json_string = R"(vector_anns: <
                                        field_id: 101
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 102
                                          group_size: 3
                                          json_path: "/string"
                                          json_type: VarChar
                                        >
                                        placeholder_tag: "$0"
         >)";
    executeJSONGroupBySearchAndReduce(raw_plan_json_string);

    // Test Case: Group by JSON field string without json_type
    const char* raw_plan_json_string_no_cast = R"(vector_anns: <
                                        field_id: 101
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 102
                                          group_size: 3
                                          json_path: "/string"
                                        >
                                        placeholder_tag: "$0"
         >)";
    executeJSONGroupBySearchAndReduce(raw_plan_json_string_no_cast);

    DeleteSegment(c_segment_1);
    DeleteSegment(c_segment_2);
}
