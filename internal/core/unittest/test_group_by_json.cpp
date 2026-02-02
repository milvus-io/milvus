#include "gtest/gtest.h"
#include "test_utils/c_api_test_utils.h"
#include "test_utils/storage_test_utils.h"
#include "test_utils/cachinglayer_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
using namespace milvus::storage;
using namespace milvus::tracer;

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
    auto seed = 1024;
    int num_queries = 1;

    ScopedSchemaHandle handle(*schema);

    // Helper lambda to run group-by search
    auto run_group_by_search = [&](const std::string& json_path,
                                   milvus::proto::schema::DataType json_type)
        -> std::unique_ptr<SearchResult> {
        auto plan_str =
            handle.ParseGroupBySearch("",              // no filter expression
                                      "fakevec",       // vector field name
                                      topK,            // topk
                                      "L2",            // metric type
                                      "{\"ef\": 10}",  // search params
                                      json_fid.get(),  // group_by_field_id
                                      group_size,      // group_size
                                      json_path,       // json_path
                                      json_type,       // json_type
                                      true);           // strict_group_size
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment_ptr->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);
        return search_result;
    };

    // 3. search group by json_field.int8
    {
        auto search_result =
            run_group_by_search("/int8", milvus::proto::schema::DataType::Int8);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<int8_t>(
            group_by_values, search_result->distances_, group_size, topK, 1);
    }

    // 4. search group by json_field.int16
    {
        auto search_result = run_group_by_search(
            "/int16", milvus::proto::schema::DataType::Int16);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<int16_t>(
            group_by_values, search_result->distances_, group_size, topK, 1);
    }

    // 5. search group by json_field.int32
    {
        auto search_result = run_group_by_search(
            "/int32", milvus::proto::schema::DataType::Int32);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<int32_t>(
            group_by_values, search_result->distances_, group_size, topK, 1);
    }

    // 6. search group by json_field.int64
    {
        auto search_result = run_group_by_search(
            "/int64", milvus::proto::schema::DataType::Int64);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<int64_t>(
            group_by_values, search_result->distances_, group_size, topK, 1);
    }

    // 7. search group by json_field.bool
    {
        auto search_result =
            run_group_by_search("/bool", milvus::proto::schema::DataType::Bool);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(2 * group_size, group_by_values.size());
        validate_group_by_search_result<bool>(
            group_by_values, search_result->distances_, group_size, 2, 1);
    }

    // 8. search group by json_field.string
    {
        auto search_result = run_group_by_search(
            "/string", milvus::proto::schema::DataType::VarChar);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(topK * group_size, group_by_values.size());
        validate_group_by_search_result<std::string>(
            group_by_values, search_result->distances_, group_size, topK, 1);
    }

    // 9. search group by json_field.string without json_type
    {
        auto search_result = run_group_by_search(
            "/string", milvus::proto::schema::DataType::None);
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

    // 2. Search group by json_field
    auto num_queries = 10;
    auto topK = 10;
    int group_size = 2;

    ScopedSchemaHandle handle(*schema);

    // Helper lambda to run group-by search on growing segment
    auto run_group_by_search = [&](const std::string& json_path,
                                   milvus::proto::schema::DataType json_type,
                                   bool strict_cast =
                                       false) -> std::unique_ptr<SearchResult> {
        auto plan_str =
            handle.ParseGroupBySearch("",              // no filter expression
                                      "embeddings",    // vector field name
                                      topK,            // topk
                                      "L2",            // metric type
                                      "{\"ef\": 10}",  // search params
                                      json_fid.get(),  // group_by_field_id
                                      group_size,      // group_size
                                      json_path,       // json_path
                                      json_type,       // json_type
                                      true,            // strict_group_size
                                      strict_cast);    // strict_cast
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result = segment_growing_impl->Search(
            plan.get(), ph_group.get(), MAX_TIMESTAMP);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);
        return search_result;
    };

    // Search group by json_field.int8
    {
        auto search_result =
            run_group_by_search("/int8", milvus::proto::schema::DataType::Int8);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(group_by_values.size(), num_queries * topK * group_size);
        validate_group_by_search_result<int8_t>(group_by_values,
                                                search_result->distances_,
                                                group_size,
                                                topK,
                                                num_queries);
    }

    // 3. Search group by json_field.int16
    {
        auto search_result = run_group_by_search(
            "/int16", milvus::proto::schema::DataType::Int16);
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
        auto search_result = run_group_by_search(
            "/int32", milvus::proto::schema::DataType::Int32);
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
        auto search_result = run_group_by_search(
            "/int64", milvus::proto::schema::DataType::Int64);
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
        auto search_result =
            run_group_by_search("/bool", milvus::proto::schema::DataType::Bool);
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
        auto search_result = run_group_by_search(
            "/string", milvus::proto::schema::DataType::VarChar);
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
        auto search_result = run_group_by_search(
            "/string", milvus::proto::schema::DataType::None);
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
        auto plan_str = handle.ParseGroupBySearch(
            "",              // no filter expression
            "embeddings",    // vector field name
            topK,            // topk
            "L2",            // metric type
            "{\"ef\": 10}",  // search params
            json_fid.get(),  // group_by_field_id
            1,               // group_size = 1 for entire JSON
            "",              // json_path (empty for entire object)
            milvus::proto::schema::DataType::None,  // json_type
            true);                                  // strict_group_size
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result = segment_growing_impl->Search(
            plan.get(), ph_group.get(), MAX_TIMESTAMP);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);
        auto& group_by_values = search_result->group_by_values_.value();
        ASSERT_EQ(group_by_values.size(), num_queries * topK);
        validate_group_by_search_result<std::string>(
            group_by_values, search_result->distances_, 1, topK, num_queries);
    }

    // 10. Search group by with wrong type and strict_cast=true
    {
        auto plan_str = handle.ParseGroupBySearch(
            "",                                        // no filter expression
            "embeddings",                              // vector field name
            topK,                                      // topk
            "L2",                                      // metric type
            "{\"ef\": 10}",                            // search params
            json_fid.get(),                            // group_by_field_id
            group_size,                                // group_size
            "/bool",                                   // json_path
            milvus::proto::schema::DataType::VarChar,  // wrong json_type
            true,                                      // strict_group_size
            true);                                     // strict_cast = true
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        ASSERT_ANY_THROW(segment_growing_impl->Search(
            plan.get(), ph_group.get(), MAX_TIMESTAMP));
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

    ScopedSchemaHandle handle(*schema);

    // Lambda function to execute search and reduce with given parameters for JSON
    auto executeJSONGroupBySearchAndReduce = [&](const std::string& json_path,
                                                 milvus::proto::schema::DataType
                                                     json_type) {
        auto plan_str =
            handle.ParseGroupBySearch("",              // no filter expression
                                      "fakevec",       // vector field name
                                      topK,            // topk
                                      "L2",            // metric type
                                      "{\"ef\": 10}",  // search params
                                      json_fid.get(),  // group_by_field_id
                                      group_size,      // group_size
                                      json_path,       // json_path
                                      json_type);      // json_type
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        CPlaceholderGroup c_ph_group = ph_group.release();
        CSearchPlan c_plan = plan.release();

        CSearchResult c_search_res_1;
        CSearchResult c_search_res_2;
        auto status = CSearch(
            c_segment_1, c_plan, c_ph_group, MAX_TIMESTAMP, &c_search_res_1);
        ASSERT_EQ(status.error_code, Success);
        status = CSearch(
            c_segment_2, c_plan, c_ph_group, MAX_TIMESTAMP, &c_search_res_2);
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
    executeJSONGroupBySearchAndReduce("/int8",
                                      milvus::proto::schema::DataType::Int8);

    // Test Case: Group by JSON field int16
    executeJSONGroupBySearchAndReduce("/int16",
                                      milvus::proto::schema::DataType::Int16);

    // Test Case: Group by JSON field int32
    executeJSONGroupBySearchAndReduce("/int32",
                                      milvus::proto::schema::DataType::Int32);

    // Test Case: Group by JSON field int64
    executeJSONGroupBySearchAndReduce("/int64",
                                      milvus::proto::schema::DataType::Int64);

    // Test Case: Group by JSON field bool
    executeJSONGroupBySearchAndReduce("/bool",
                                      milvus::proto::schema::DataType::Bool);

    // Test Case: Group by JSON field string
    executeJSONGroupBySearchAndReduce("/string",
                                      milvus::proto::schema::DataType::VarChar);

    // Test Case: Group by JSON field string without json_type
    executeJSONGroupBySearchAndReduce("/string",
                                      milvus::proto::schema::DataType::None);

    DeleteSegment(c_segment_1);
    DeleteSegment(c_segment_2);
}
