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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/common_type_c.h"
#include "common/protobuf_utils.h"
#include "common/type_c.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "segcore/Collection.h"
#include "segcore/collection_c.h"
#include "segcore/plan_c.h"
#include "segcore/reduce/Reduce.h"
#include "segcore/reduce_c.h"
#include "segcore/segment_c.h"
#include "test_utils/DataGen.h"
#include "test_utils/PbHelper.h"
#include "test_utils/c_api_test_utils.h"

TEST(CApiTest, ReduceSearchResultsAndFillDataCost) {
    int N = 100;
    int topK = 10;
    int num_queries = 2;

    auto collection = NewCollection(get_default_schema_config().c_str());

    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    auto dataset = DataGen(schema, N, 77, 0, 1, 10, true);
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

    milvus::segcore::ScopedSchemaHandle schema_handle(*schema);
    auto binary_plan =
        schema_handle.ParseSearch("",  // expression (empty for no filter)
                                  "fakevec",             // vector field name
                                  topK,                  // topK
                                  "L2",                  // metric_type
                                  R"({"nprobe": 10})");  // search_params
    auto blob = generate_query_data(num_queries);
    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);
    void* placeholderGroup = nullptr;

    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    dataset.timestamps_.clear();
    dataset.timestamps_.push_back(1);
    CSearchResult res;
    auto stats = CSearch(
        segment, plan, placeholderGroup, dataset.timestamps_[N - 1], &res);
    ASSERT_EQ(stats.error_code, Success);

    // Reduce and fetch blob + scanned bytes
    std::vector<int64_t> slice_nqs{num_queries};
    std::vector<int64_t> slice_topKs{topK};
    CSearchResultDataBlobs c_search_result_data_blobs;
    uint8_t trace_id[16] = {0};
    uint8_t span_id[8] = {0};
    CTraceContext trace{
        .traceID = trace_id,
        .spanID = span_id,
        .traceFlags = 0,
    };
    auto st = ReduceSearchResultsAndFillData(trace,
                                             &c_search_result_data_blobs,
                                             plan,
                                             &res,
                                             1,
                                             slice_nqs.data(),
                                             slice_topKs.data(),
                                             slice_nqs.size());
    ASSERT_EQ(st.error_code, Success);

    CProto blob_proto;
    int64_t scanned_remote_bytes = 0;
    int64_t scanned_total_bytes = 0;
    auto st2 = GetSearchResultDataBlob(&blob_proto,
                                       &scanned_remote_bytes,
                                       &scanned_total_bytes,
                                       c_search_result_data_blobs,
                                       0);
    ASSERT_EQ(st2.error_code, Success);
    ASSERT_EQ(scanned_remote_bytes, 0);
    ASSERT_EQ(scanned_total_bytes, scanned_remote_bytes);
    ASSERT_GT(blob_proto.proto_size, 0);

    DeleteSearchResultDataBlobs(c_search_result_data_blobs);
    DeleteSearchResult(res);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchPlan(plan);
    DeleteSegment(segment);
    DeleteCollection(collection);
}
