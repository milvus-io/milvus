#include "test_utils/c_api_test_utils.h"
#include "test_utils/storage_test_utils.h"
#include "test_cachinglayer/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
using namespace milvus::storage;
using namespace milvus::tracer;

static std::unique_ptr<SearchResult> run_group_by_search(const std::string& raw_plan,
                               const SchemaPtr& schema,
                               SegmentInternalInterface* segment,
                               int dim,
                               int topK,
                               int num_queries = 1) {
    proto::plan::PlanNode plan_node;
    auto ok = google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
    auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
    auto seed = 1024;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
    auto ph_group = ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto search_result = segment->Search(plan.get(), ph_group.get(), 1L << 63);
    CheckGroupBySearchResult(*search_result, topK, num_queries, false);
    return search_result;
}

template <typename T>
void validate_group_by_search_result(const std::vector<GroupByValueType>& group_by_values,
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
                                          json_cast_type: Int8
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(raw_plan, schema, segment_ptr, dim, topK);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<int8_t>(group_by_values, search_result->distances_, group_size, topK, 1);
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
                                          json_cast_type: Int16
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(raw_plan, schema, segment_ptr, dim, topK);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<int16_t>(group_by_values, search_result->distances_, group_size, topK, 1);
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
                                          json_cast_type: Int32
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(raw_plan, schema, segment_ptr, dim, topK);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<int32_t>(group_by_values, search_result->distances_, group_size, topK, 1);
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
                                          json_cast_type: Int64
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(raw_plan, schema, segment_ptr, dim, topK);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<int64_t>(group_by_values, search_result->distances_, group_size, topK, 1);
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
                                          json_cast_type: Bool
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(raw_plan, schema, segment_ptr, dim, topK);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(2 * group_size, group_by_values.size());
        validate_group_by_search_result<bool>(group_by_values, search_result->distances_, group_size, 2, 1);
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
                                          json_cast_type: VarChar
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(raw_plan, schema, segment_ptr, dim, topK);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<std::string>(group_by_values, search_result->distances_, group_size, topK, 1);
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
    auto data_set =
        DataGen(schema, N, 42, 0, 1, 10, 10, false, false);
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
                                          json_cast_type: Int8,
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
    auto search_result = run_group_by_search(raw_plan, schema, segment_growing_impl, dim, topK, num_queries);
    auto& group_by_values = search_result->group_by_values_.value();
    ASSERT_EQ(group_by_values.size(), num_queries * topK * group_size);
    validate_group_by_search_result<int8_t>(group_by_values, search_result->distances_, group_size, topK, num_queries);

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
                                          json_cast_type: Int16,
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(raw_plan, schema, segment_growing_impl, dim, topK, num_queries);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(group_by_values.size(), num_queries * topK * group_size);
        validate_group_by_search_result<int16_t>(group_by_values, search_result->distances_, group_size, topK, num_queries);
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
                                          json_cast_type: Int32,
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(raw_plan, schema, segment_growing_impl, dim, topK, num_queries);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(group_by_values.size(), num_queries * topK * group_size);
        validate_group_by_search_result<int32_t>(group_by_values, search_result->distances_, group_size, topK, num_queries);
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
                                          json_cast_type: Int64,
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(raw_plan, schema, segment_growing_impl, dim, topK, num_queries);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(group_by_values.size(), num_queries * topK * group_size);
        validate_group_by_search_result<int64_t>(group_by_values, search_result->distances_, group_size, topK, num_queries);
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
                                          json_cast_type: Bool,
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(raw_plan, schema, segment_growing_impl, dim, topK, num_queries);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(group_by_values.size(), num_queries * 2 * group_size);
        validate_group_by_search_result<bool>(group_by_values, search_result->distances_, group_size, 2, num_queries);
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
                                          json_cast_type: VarChar,
                                          strict_group_size: true,
                                        >
                                        placeholder_tag: "$0"
         >)";
        auto search_result = run_group_by_search(raw_plan, schema, segment_growing_impl, dim, topK, num_queries);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(group_by_values.size(), num_queries * topK * group_size);
        validate_group_by_search_result<std::string>(group_by_values, search_result->distances_, group_size, topK, num_queries);
    }
}





