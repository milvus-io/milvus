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
#include "test_utils/DataGen.h"
#include "test_utils/c_api_test_utils.h"

TEST(CApiTest, StreamReduce) {
    int N = 300;
    int topK = 100;
    int num_queries = 2;
    auto collection = NewCollection(get_default_schema_config());

    //1. set up segments
    CSegmentInterface segment_1;
    auto status = NewSegment(collection, Growing, -1, &segment_1, false);
    ASSERT_EQ(status.error_code, Success);
    CSegmentInterface segment_2;
    status = NewSegment(collection, Growing, -1, &segment_2, false);
    ASSERT_EQ(status.error_code, Success);

    //2. insert data into segments
    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    auto dataset_1 = DataGen(schema, N, 55, 0, 1, 10, true);
    int64_t offset_1;
    PreInsert(segment_1, N, &offset_1);
    auto insert_data_1 = serialize(dataset_1.raw_);
    auto ins_res_1 = Insert(segment_1,
                            offset_1,
                            N,
                            dataset_1.row_ids_.data(),
                            dataset_1.timestamps_.data(),
                            insert_data_1.data(),
                            insert_data_1.size());
    ASSERT_EQ(ins_res_1.error_code, Success);

    auto dataset_2 = DataGen(schema, N, 66, 0, 1, 10, true);
    int64_t offset_2;
    PreInsert(segment_2, N, &offset_2);
    auto insert_data_2 = serialize(dataset_2.raw_);
    auto ins_res_2 = Insert(segment_2,
                            offset_2,
                            N,
                            dataset_2.row_ids_.data(),
                            dataset_2.timestamps_.data(),
                            insert_data_2.data(),
                            insert_data_2.size());
    ASSERT_EQ(ins_res_2.error_code, Success);

    //3. search two segments
    auto fmt = boost::format(R"(vector_anns: <
                                            field_id: 100
                                            query_info: <
                                                topk: %1%
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0">
                                            output_field_ids: 100)") %
               topK;
    auto serialized_expr_plan = fmt.str();
    auto blob = generate_query_data(num_queries);
    void* plan = nullptr;
    auto binary_plan =
        translate_text_plan_to_binary_plan(serialized_expr_plan.data());
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);
    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);
    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);

    dataset_1.timestamps_.clear();
    dataset_1.timestamps_.push_back(1);
    dataset_2.timestamps_.clear();
    dataset_2.timestamps_.push_back(1);
    CSearchResult res1;
    CSearchResult res2;
    auto stats1 = CSearch(
        segment_1, plan, placeholderGroup, dataset_1.timestamps_[N - 1], &res1);
    ASSERT_EQ(stats1.error_code, Success);
    auto stats2 = CSearch(
        segment_2, plan, placeholderGroup, dataset_2.timestamps_[N - 1], &res2);
    ASSERT_EQ(stats2.error_code, Success);

    //4. stream reduce two search results
    auto slice_nqs = std::vector<int64_t>{num_queries / 2, num_queries / 2};
    if (num_queries == 1) {
        slice_nqs = std::vector<int64_t>{num_queries};
    }
    auto slice_topKs = std::vector<int64_t>{topK, topK};
    if (topK == 1) {
        slice_topKs = std::vector<int64_t>{topK, topK};
    }

    //5. set up stream reducer
    CSearchStreamReducer c_search_stream_reducer;
    NewStreamReducer(plan,
                     slice_nqs.data(),
                     slice_topKs.data(),
                     slice_nqs.size(),
                     &c_search_stream_reducer);
    StreamReduce(c_search_stream_reducer, &res1, 1);
    StreamReduce(c_search_stream_reducer, &res2, 1);
    CSearchResultDataBlobs c_search_result_data_blobs;
    GetStreamReduceResult(c_search_stream_reducer, &c_search_result_data_blobs);
    SearchResultDataBlobs* search_result_data_blob =
        (SearchResultDataBlobs*)(c_search_result_data_blobs);

    //6. check
    for (size_t i = 0; i < slice_nqs.size(); i++) {
        milvus::proto::schema::SearchResultData search_result_data;
        auto suc = search_result_data.ParseFromArray(
            search_result_data_blob->blobs[i].data(),
            search_result_data_blob->blobs[i].size());
        ASSERT_TRUE(suc);
        ASSERT_EQ(search_result_data.num_queries(), slice_nqs[i]);
        ASSERT_EQ(search_result_data.top_k(), slice_topKs[i]);
        ASSERT_EQ(search_result_data.ids().int_id().data_size(),
                  search_result_data.topks().at(0) * slice_nqs[i]);
        ASSERT_EQ(search_result_data.scores().size(),
                  search_result_data.topks().at(0) * slice_nqs[i]);

        ASSERT_EQ(search_result_data.topks().size(), slice_nqs[i]);
        for (auto real_topk : search_result_data.topks()) {
            ASSERT_LE(real_topk, slice_topKs[i]);
        }
    }

    DeleteSearchResultDataBlobs(c_search_result_data_blobs);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(res1);
    DeleteSearchResult(res2);
    DeleteCollection(collection);
    DeleteSegment(segment_1);
    DeleteSegment(segment_2);
    DeleteStreamSearchReducer(c_search_stream_reducer);
    DeleteStreamSearchReducer(nullptr);
}

TEST(CApiTest, StreamReduceGroupBY) {
    int N = 300;
    int topK = 100;
    int num_queries = 2;
    int dim = 16;
    namespace schema = milvus::proto::schema;

    void* c_collection;
    //1. set up schema and collection
    {
        schema::CollectionSchema collection_schema;
        auto pk_field_schema = collection_schema.add_fields();
        pk_field_schema->set_name("pk_field");
        pk_field_schema->set_fieldid(100);
        pk_field_schema->set_data_type(schema::DataType::Int64);
        pk_field_schema->set_is_primary_key(true);

        auto i8_field_schema = collection_schema.add_fields();
        i8_field_schema->set_name("int8_field");
        i8_field_schema->set_fieldid(101);
        i8_field_schema->set_data_type(schema::DataType::Int8);
        i8_field_schema->set_is_primary_key(false);

        auto i16_field_schema = collection_schema.add_fields();
        i16_field_schema->set_name("int16_field");
        i16_field_schema->set_fieldid(102);
        i16_field_schema->set_data_type(schema::DataType::Int16);
        i16_field_schema->set_is_primary_key(false);

        auto i32_field_schema = collection_schema.add_fields();
        i32_field_schema->set_name("int32_field");
        i32_field_schema->set_fieldid(103);
        i32_field_schema->set_data_type(schema::DataType::Int32);
        i32_field_schema->set_is_primary_key(false);

        auto str_field_schema = collection_schema.add_fields();
        str_field_schema->set_name("str_field");
        str_field_schema->set_fieldid(104);
        str_field_schema->set_data_type(schema::DataType::VarChar);
        auto str_type_params = str_field_schema->add_type_params();
        str_type_params->set_key(MAX_LENGTH);
        str_type_params->set_value(std::to_string(64));
        str_field_schema->set_is_primary_key(false);

        auto vec_field_schema = collection_schema.add_fields();
        vec_field_schema->set_name("fake_vec");
        vec_field_schema->set_fieldid(105);
        vec_field_schema->set_data_type(schema::DataType::FloatVector);
        auto metric_type_param = vec_field_schema->add_index_params();
        metric_type_param->set_key("metric_type");
        metric_type_param->set_value(knowhere::metric::L2);
        auto dim_param = vec_field_schema->add_type_params();
        dim_param->set_key("dim");
        dim_param->set_value(std::to_string(dim));
        c_collection = NewCollection(&collection_schema, knowhere::metric::L2);
    }

    CSegmentInterface segment;
    auto status = NewSegment(c_collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    //2. generate data and insert
    auto c_schema = ((milvus::segcore::Collection*)c_collection)->get_schema();
    auto dataset = DataGen(c_schema, N);
    int64_t offset;
    PreInsert(segment, N, &offset);
    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    //3. search
    auto fmt = boost::format(R"(vector_anns: <
                                            field_id: 105
                                            query_info: <
                                                topk: %1%
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                                group_by_field_id: 101
                                            >
                                            placeholder_tag: "$0">
                                            output_field_ids: 100)") %
               topK;
    auto serialized_expr_plan = fmt.str();
    auto blob = generate_query_data(num_queries);
    void* plan = nullptr;
    auto binary_plan =
        translate_text_plan_to_binary_plan(serialized_expr_plan.data());
    status = CreateSearchPlanByExpr(
        c_collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    dataset.timestamps_.clear();
    dataset.timestamps_.push_back(1);

    CSearchResult res1;
    CSearchResult res2;
    auto res = CSearch(
        segment, plan, placeholderGroup, dataset.timestamps_[N - 1], &res1);
    ASSERT_EQ(res.error_code, Success);
    res = CSearch(
        segment, plan, placeholderGroup, dataset.timestamps_[N - 1], &res2);
    ASSERT_EQ(res.error_code, Success);

    //4. set up stream reducer
    auto slice_nqs = std::vector<int64_t>{num_queries / 2, num_queries / 2};
    if (num_queries == 1) {
        slice_nqs = std::vector<int64_t>{num_queries};
    }
    auto slice_topKs = std::vector<int64_t>{topK, topK};
    if (topK == 1) {
        slice_topKs = std::vector<int64_t>{topK, topK};
    }
    CSearchStreamReducer c_search_stream_reducer;
    NewStreamReducer(plan,
                     slice_nqs.data(),
                     slice_topKs.data(),
                     slice_nqs.size(),
                     &c_search_stream_reducer);

    //5. stream reduce
    StreamReduce(c_search_stream_reducer, &res1, 1);
    StreamReduce(c_search_stream_reducer, &res2, 1);
    CSearchResultDataBlobs c_search_result_data_blobs;
    GetStreamReduceResult(c_search_stream_reducer, &c_search_result_data_blobs);
    SearchResultDataBlobs* search_result_data_blob =
        (SearchResultDataBlobs*)(c_search_result_data_blobs);

    //6. check result
    for (size_t i = 0; i < slice_nqs.size(); i++) {
        milvus::proto::schema::SearchResultData search_result_data;
        auto suc = search_result_data.ParseFromArray(
            search_result_data_blob->blobs[i].data(),
            search_result_data_blob->blobs[i].size());
        ASSERT_TRUE(suc);
        ASSERT_EQ(search_result_data.num_queries(), slice_nqs[i]);
        ASSERT_EQ(search_result_data.top_k(), slice_topKs[i]);
        ASSERT_EQ(search_result_data.ids().int_id().data_size(),
                  search_result_data.topks().at(0) * slice_nqs[i]);
        ASSERT_EQ(search_result_data.scores().size(),
                  search_result_data.topks().at(0) * slice_nqs[i]);
        ASSERT_TRUE(search_result_data.has_group_by_field_value());

        // check real topks
        ASSERT_EQ(search_result_data.topks().size(), slice_nqs[i]);
        for (auto real_topk : search_result_data.topks()) {
            ASSERT_LE(real_topk, slice_topKs[i]);
        }
    }

    DeleteSearchResultDataBlobs(c_search_result_data_blobs);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(res1);
    DeleteSearchResult(res2);
    DeleteCollection(c_collection);
    DeleteSegment(segment);
    DeleteStreamSearchReducer(c_search_stream_reducer);
    DeleteStreamSearchReducer(nullptr);
}