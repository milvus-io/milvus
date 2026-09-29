// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <cstdlib>
#include <tuple>

#include "exec/AnnFusingPolicy.h"
#include "exec/expression/Expr.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "storage/LocalChunkManagerSingleton.h"

namespace milvus::exec {
namespace {

class TimestamptzPolicyTest
    : public testing::TestWithParam<
          std::tuple<proto::plan::ArithOpType, proto::plan::OpType>> {
 protected:
    static void
    SetUpTestSuite() {
        char directory[] = "/tmp/milvus-timestamptz-policy-XXXXXX";
        ASSERT_NE(mkdtemp(directory), nullptr);
        storage::LocalChunkManagerSingleton::GetInstance().Init(directory);
        storage::MmapConfig config{};
        config.cache_read_ahead_policy = "willneed";
        config.mmap_path = directory;
        config.disk_limit = 512 * 1024 * 1024;
        config.fix_file_size = 4 * 1024 * 1024;
        storage::MmapManager::GetInstance().Init(config);
    }

    void
    SetUp() override {
        // Metadata-only test: no generated timestamp rows, vectors or queries
        // are inserted. Constants below belong to the predicate, not a dataset.
        schema_ = std::make_shared<Schema>();
        auto field = schema_->AddDebugField("timestamp", DataType::TIMESTAMPTZ);
        segment_ = segcore::CreateSealedSegment(schema_);
        context_ = std::make_unique<QueryContext>(
            "timestamptz-policy", segment_.get(), 0, 0);
        proto::plan::Interval interval;
        proto::plan::GenericValue value;
        value.set_int64_val(0);
        auto logical = std::make_shared<expr::TimestamptzArithCompareExpr>(
            expr::ColumnInfo(field, DataType::TIMESTAMPTZ),
            std::get<0>(GetParam()),
            interval,
            std::get<1>(GetParam()),
            value);
        physical_ = CompileExpression(logical, context_.get(), {}, false);
    }

    SchemaPtr schema_;
    segcore::SegmentSealedUPtr segment_;
    std::unique_ptr<QueryContext> context_;
    ExprPtr physical_;
};

TEST_P(TimestamptzPolicyTest, OriginalOperationMetadata) {
    ASSERT_TRUE(physical_->SupportOffsetInput());
    const auto facts = physical_->DescribeFilterSource();
    EXPECT_EQ(facts.data_type, DataType::TIMESTAMPTZ);
    EXPECT_EQ(facts.expr_type, proto::plan::Expr::kTimestamptzArithCompareExpr);
    EXPECT_EQ(facts.arith_operation, std::get<0>(GetParam()));
    EXPECT_EQ(facts.operation, std::get<1>(GetParam()));
    EXPECT_EQ(facts.access_path, ExprExecPath::RawData);
    EXPECT_EQ(facts.index_type, index::ScalarIndexType::NONE);
}

TEST_P(TimestamptzPolicyTest, YamlThroughHostPolicy) {
    // Startup policy is immutable. The focused integration runner executes
    // each YAML in a separate process; normal OSS unit tests need no plugin.
    const auto library = std::getenv("MILVUS_TIMESTAMP_POLICY_LIBRARY");
    const auto config = std::getenv("MILVUS_TIMESTAMP_POLICY_CONFIG");
    const auto mode = std::getenv("MILVUS_TIMESTAMP_POLICY_MODE");
    if (!library || !config || !mode) {
        GTEST_SKIP() << "isolated native plugin integration not configured";
    }
    ASSERT_TRUE(AnnFusingPolicy::Initialize(library, config));
    const std::string rule(mode);
    ASSERT_TRUE(rule == "add" || rule == "compare" || rule == "both" ||
                rule == "all" || rule == "none");
    const bool add = std::get<0>(GetParam()) == proto::plan::Add;
    const bool greater = std::get<1>(GetParam()) == proto::plan::GreaterThan;
    const bool blocked = rule == "all" || (rule == "add" && add) ||
                         (rule == "compare" && greater) ||
                         (rule == "both" && add && greater);
    EXPECT_EQ(physical_->ConsiderAnnFusing(AnnFilterFusingRequest::Auto),
              !blocked);
    EXPECT_FALSE(
        physical_->ConsiderAnnFusing(AnnFilterFusingRequest::Baseline));
    EXPECT_TRUE(
        physical_->ConsiderAnnFusing(AnnFilterFusingRequest::ExplicitFusing));
}

INSTANTIATE_TEST_SUITE_P(
    OriginalEnums,
    TimestamptzPolicyTest,
    testing::Combine(testing::Values(proto::plan::Add, proto::plan::Sub),
                     testing::Values(proto::plan::GreaterThan,
                                     proto::plan::GreaterEqual,
                                     proto::plan::LessThan,
                                     proto::plan::LessEqual,
                                     proto::plan::Equal,
                                     proto::plan::NotEqual)));

}  // namespace
}  // namespace milvus::exec
