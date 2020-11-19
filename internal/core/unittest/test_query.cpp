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
    int num_queries = 10;
    int dim = 16;
    std::default_random_engine e;
    std::normal_distribution<double> dis(0, 1);
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::VECTOR_FLOAT);
    for (int i = 0; i < num_queries; ++i) {
        std::vector<float> vec;
        for (int d = 0; d < dim; ++d) {
            vec.push_back(dis(e));
        }
        // std::string line((char*)vec.data(), (char*)vec.data() + vec.size() * sizeof(float));
        value->add_values(vec.data(), vec.size() * sizeof(float));
    }
    auto blob = raw_group.SerializeAsString();
    // ser::PlaceholderGroup new_group;
    // new_group.ParseFromString()
    auto placeholder = ParsePlaceholderGroup(plan.get(), blob);
}

TEST(Query, Exec) {
    using namespace milvus::query;
    using namespace milvus::segcore;
}
