#include "test_utils/c_api_test_utils.h"
#include "test_utils/storage_test_utils.h"
#include "test_cachinglayer/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
using namespace milvus::storage;
using namespace milvus::tracer;

TEST(GroupBYJSON, SealedIndex) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    // 0. prepare schema
    int dim = 64;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto json_fid = schema->AddDebugField("json_field", DataType::JSON);
    schema->set_primary_field_id(json_fid);
    size_t N = 50;

    // 1. load raw data
    auto raw_data = DataGen(schema, N, 42, 0, 8, 10, false, false);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

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

    int topK = 15;
    int group_size = 3;

    // 3. search group by json_field.metadata.product_info.brand
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 100
                                        query_info: <
                                          topk: 15
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 101
                                          group_size: 3
                                          group_by_json_path: "metadata.product_info.brand"
                                          group_by_json_cast_type: INT64
                                        >
                                        placeholder_tag: "$0"
         >)";
        proto::plan::PlanNode plan_node;
        auto ok =
            google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
        auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);

        CheckGroupBySearchResult(*search_result, topK, num_queries, false);

        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(20, group_by_values.size());
        // 7个brand分组，每组最多3个，最后一组2个
        std::unordered_map<int64_t, int> brand_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < group_by_values.size(); i++) {
            if (std::holds_alternative<int64_t>(group_by_values[i].value())) {
                int64_t brand = std::get<int64_t>(group_by_values[i].value());
                brand_map[brand] += 1;
                ASSERT_TRUE(brand_map[brand] <= group_size);
                auto distance = search_result->distances_.at(i);
                ASSERT_TRUE(lastDistance <= distance);
                lastDistance = distance;
            }
        }
        ASSERT_EQ(brand_map.size(), 7);
    }
}