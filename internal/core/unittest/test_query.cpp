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
#include "query/deprecated/Parser.h"
#include "query/Expr.h"
#include "query/PlanNode.h"
#include "query/generated/ExprVisitor.h"
#include "query/generated/PlanNodeVisitor.h"
#include "test_utils/DataGen.h"
#include "query/generated/ShowPlanNodeVisitor.h"
#include "query/generated/ExecPlanNodeVisitor.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentSmallIndex.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
TEST(Query, Naive) {
    SUCCEED();
    using namespace milvus::wtf;
    std::string dsl_string = R"(
{
    "bool": {
        "must": [
            {
                "term": {
                    "A": [
                        1,
                        2,
                        5
                    ]
                }
            },
            {
                "range": {
                    "B": {
                        "GT": 1,
                        "LT": 100
                    }
                }
            },
            {
                "vector": {
                    "Vec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 10
                    }
                }
            }
        ]
    }
})";
}

TEST(Query, ShowExecutor) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    using namespace milvus;
    auto node = std::make_unique<FloatVectorANNS>();
    auto schema = std::make_shared<Schema>();
    schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);
    int64_t num_queries = 100L;
    auto raw_data = DataGen(schema, num_queries);
    auto& info = node->query_info_;
    info.metric_type_ = "L2";
    info.topK_ = 20;
    info.field_id_ = "fakevec";
    node->predicate_ = std::nullopt;
    ShowPlanNodeVisitor show_visitor;
    PlanNodePtr base(node.release());
    auto res = show_visitor.call_child(*base);
    auto dup = res;
    std::cout << dup.dump(4);
}

TEST(Query, DSL) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    ShowPlanNodeVisitor shower;

    std::string dsl_string = R"(
{
    "bool": {
        "must": [
            {
                "vector": {
                    "Vec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 10
                    }
                }
            }
        ]
    }
})";

    auto schema = std::make_shared<Schema>();
    schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);

    auto plan = CreatePlan(*schema, dsl_string);
    auto res = shower.call_child(*plan->plan_node_);
    std::cout << res.dump(4) << std::endl;

    std::string dsl_string2 = R"(
{
    "bool": {
        "vector": {
            "Vec": {
                "metric_type": "L2",
                "params": {
                    "nprobe": 10
                },
                "query": "$0",
                "topk": 10
            }
        }
    }
})";
    auto plan2 = CreatePlan(*schema, dsl_string2);
    auto res2 = shower.call_child(*plan2->plan_node_);
    std::cout << res2.dump(4) << std::endl;
    ASSERT_EQ(res, res2);
}

TEST(Query, ParsePlaceholderGroup) {
    namespace ser = milvus::proto::service;
    std::string dsl_string = R"(
{
    "bool": {
        "vector": {
            "fakevec": {
                "metric_type": "L2",
                "params": {
                    "nprobe": 10
                },
                "query": "$0",
                "topk": 10
            }
        }
    }
})";

    auto schema = std::make_shared<Schema>();
    schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);
    auto plan = CreatePlan(*schema, dsl_string);
    int64_t num_queries = 100000;
    int dim = 16;
    auto raw_group = CreatePlaceholderGroup(num_queries, dim);
    auto blob = raw_group.SerializeAsString();
    auto placeholder = ParsePlaceholderGroup(plan.get(), blob);
}

TEST(Query, ExecWithPredicate) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);
    schema->AddField("age", DataType::FLOAT);
    std::string dsl = R"({
        "bool": {
            "must": [
            {
                "range": {
                    "age": {
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
                        "topk": 5
                    }
                }
            }
            ]
        }
    })";
    int64_t N = 1000 * 1000;
    auto dataset = DataGen(schema, N);
    auto segment = std::make_unique<SegmentSmallIndex>(schema);
    segment->PreInsert(N);
    segment->Insert(0, N, dataset.row_ids_.data(), dataset.timestamps_.data(), dataset.raw_);

    auto plan = CreatePlan(*schema, dsl);
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group = ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    QueryResult qr;
    Timestamp time = 1000000;
    std::vector<const PlaceholderGroup*> ph_group_arr = {ph_group.get()};
    segment->Search(plan.get(), ph_group_arr.data(), &time, 1, qr);
    std::vector<std::vector<std::string>> results;
    int topk = 5;
    for (int q = 0; q < num_queries; ++q) {
        std::vector<std::string> result;
        for (int k = 0; k < topk; ++k) {
            int index = q * topk + k;
            result.emplace_back(std::to_string(qr.result_ids_[index]) + "->" +
                                std::to_string(qr.result_distances_[index]));
        }
        results.emplace_back(std::move(result));
    }

    auto ref = Json::parse(R"([
  [
    [
      "980486->3.149221",
      "318367->3.661235",
      "302798->4.553688",
      "321424->4.757450",
      "565529->5.083780"
    ],
    [
      "233390->7.931535",
      "238958->8.109344",
      "230645->8.439169",
      "901939->8.658772",
      "380328->8.731251"
    ],
    [
      "897246->3.749835",
      "750683->3.897577",
      "857598->4.230977",
      "299009->4.379639",
      "440010->4.454046"
    ],
    [
      "840855->4.782170",
      "709627->5.063170",
      "72322->5.166143",
      "107142->5.180207",
      "948403->5.247065"
    ],
    [
      "810401->3.926393",
      "46575->4.054171",
      "201740->4.274491",
      "669040->4.399628",
      "231500->4.831223"
    ]
  ]
])");

    Json json{results};
    ASSERT_EQ(json, ref);
}

TEST(Query, ExecWihtoutPredicate) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);
    schema->AddField("age", DataType::FLOAT);
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
                        "topk": 5
                    }
                }
            }
            ]
        }
    })";
    int64_t N = 1000 * 1000;
    auto dataset = DataGen(schema, N);
    auto segment = std::make_unique<SegmentSmallIndex>(schema);
    segment->PreInsert(N);
    segment->Insert(0, N, dataset.row_ids_.data(), dataset.timestamps_.data(), dataset.raw_);

    auto plan = CreatePlan(*schema, dsl);
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group = ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    QueryResult qr;
    Timestamp time = 1000000;
    std::vector<const PlaceholderGroup*> ph_group_arr = {ph_group.get()};
    segment->Search(plan.get(), ph_group_arr.data(), &time, 1, qr);
    std::vector<std::vector<std::string>> results;
    int topk = 5;
    for (int q = 0; q < num_queries; ++q) {
        std::vector<std::string> result;
        for (int k = 0; k < topk; ++k) {
            int index = q * topk + k;
            result.emplace_back(std::to_string(qr.result_ids_[index]) + "->" +
                                std::to_string(qr.result_distances_[index]));
        }
        results.emplace_back(std::move(result));
    }

    Json json{results};
    std::cout << json.dump(2);
}
