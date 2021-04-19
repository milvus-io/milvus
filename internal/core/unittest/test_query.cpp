#include <gtest/gtest.h>
#include "query/Parser.h"
#include "query/Expr.h"
#include "query/PlanNode.h"
#include "query/generated/ExprVisitor.h"
#include "query/generated/PlanNodeVisitor.h"
#include "test_utils/DataGen.h"
#include "query/generated/ShowPlanNodeVisitor.h"

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
    auto node = std::make_unique<FloatVectorANNS>();
    auto schema = std::make_shared<Schema>();
    int64_t num_queries = 100L;
    schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);
    auto raw_data = DataGen(schema, num_queries);
    auto& info = node->query_info_;
    info.metric_type_ = "L2";
    info.num_queries_ = 10;
    info.dim_ = 16;
    info.topK_ = 20;
    info.field_id_ = "fakevec";
    node->predicate_ = std::nullopt;
    ShowPlanNodeVisitor show_visitor;
    PlanNodePtr base(node.release());
    auto res = show_visitor.call_child(*base);
    auto dup = res;
    dup["data"] = "...collased...";
    std::cout << dup.dump(4);
}