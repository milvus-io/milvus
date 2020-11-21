#include <gtest/gtest.h>
#include "query/deprecated/Parser.h"
#include "query/Expr.h"
#include "query/PlanNode.h"
#include "query/generated/ExprVisitor.h"
#include "query/generated/PlanNodeVisitor.h"
#include "test_utils/DataGen.h"
#include "query/generated/ShowPlanNodeVisitor.h"
#include "query/generated/ExecExprVisitor.h"
#include "query/Plan.h"
#include "utils/tools.h"
#include <regex>
#include "segcore/SegmentSmallIndex.h"
using namespace milvus;

TEST(Expr, Naive) {
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

TEST(Expr, Range) {
    SUCCEED();
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::string dsl_string = R"(
{
    "bool": {
        "must": [
            {
                "range": {
                    "age": {
                        "GT": 1,
                        "LT": 100
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
                        "topk": 10
                    }
                }
            }
        ]
    }
})";
    auto schema = std::make_shared<Schema>();
    schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);
    schema->AddField("age", DataType::INT32);
    auto plan = CreatePlan(*schema, dsl_string);
    ShowPlanNodeVisitor shower;
    Assert(plan->tag2field_.at("$0") == "fakevec");
    auto out = shower.call_child(*plan->plan_node_);
    std::cout << out.dump(4);
}

TEST(Expr, ShowExecutor) {
    using namespace milvus::query;
    using namespace milvus::segcore;
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
    dup["data"] = "...collased...";
    std::cout << dup.dump(4);
}

TEST(Expr, TestRange) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string, std::function<bool(int)>>> testcases = {
        {R"("GT": 2000, "LT": 3000)", [](int v) { return 2000 < v && v < 3000; }},
        {R"("GE": 2000, "LT": 3000)", [](int v) { return 2000 <= v && v < 3000; }},
        {R"("GT": 2000, "LE": 3000)", [](int v) { return 2000 < v && v <= 3000; }},
        {R"("GE": 2000, "LE": 3000)", [](int v) { return 2000 <= v && v <= 3000; }},
        {R"("GE": 2000)", [](int v) { return v >= 2000; }},
        {R"("GT": 2000)", [](int v) { return v > 2000; }},
        {R"("LE": 2000)", [](int v) { return v <= 2000; }},
        {R"("LT": 2000)", [](int v) { return v < 2000; }},
        {R"("EQ": 2000)", [](int v) { return v == 2000; }},
        {R"("NE": 2000)", [](int v) { return v != 2000; }},
    };

    std::string dsl_string_tmp = R"(
{
    "bool": {
        "must": [
            {
                "range": {
                    "age": {
                        @@@@
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
                        "topk": 10
                    }
                }
            }
        ]
    }
})";
    auto schema = std::make_shared<Schema>();
    schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);
    schema->AddField("age", DataType::INT32);

    auto seg = CreateSegment(schema);
    int N = 10000;
    std::vector<int> age_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age_col = raw_data.get_col<int>(1);
        age_col.insert(age_col.end(), new_age_col.begin(), new_age_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N, N, raw_data.row_ids_.data(), raw_data.timestamps_.data(), raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentSmallIndex*>(seg.get());
    ExecExprVisitor visitor(*seg_promote);
    for (auto [clause, ref_func] : testcases) {
        auto loc = dsl_string_tmp.find("@@@@");
        auto dsl_string = dsl_string_tmp;
        dsl_string.replace(loc, 4, clause);
        auto plan = CreatePlan(*schema, dsl_string);
        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), upper_div(N * num_iters, DefaultElementPerChunk));

        for (int i = 0; i < N * num_iters; ++i) {
            auto vec_id = i / DefaultElementPerChunk;
            auto offset = i % DefaultElementPerChunk;
            auto ans = final[vec_id][offset];

            auto val = age_col[i];
            auto ref = !ref_func(val);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
        }
    }
}