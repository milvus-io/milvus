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
#include "segcore/SegmentSealed.h"
#include "plan/PlanNode.h"
#include "exec/QueryContext.h"
#include "exec/Task.h"
#include <glog/logging.h>
#include "config/ConfigKnowhere.h"
#include "segcore/segcore_init_c.h"
#include "exec/operator/query-agg/RegisterAggregateFunctions.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::plan;
using namespace milvus::exec;

class QueryAggTest : public testing::TestWithParam<std::pair<bool, bool>> {
 public:
    constexpr static const char bool_field[] = "bool";
    constexpr static const char int8_field[] = "int8";
    constexpr static const char int16_field[] = "int16";
    constexpr static const char int32_field[] = "int32";
    constexpr static const char int64_field[] = "int64";
    constexpr static const char float_field[] = "float";
    constexpr static const char double_field[] = "double";
    constexpr static const char string_field[] = "string";

 protected:
    void
    SetUp() override {
        registerAllAggregateFunctions();
        schema_ = std::make_shared<Schema>();
        auto vec_fid = schema_->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        auto [nullable, _] = GetParam();
        auto bool_fid =
            schema_->AddDebugField(bool_field, DataType::BOOL, nullable);
        auto int8_fid =
            schema_->AddDebugField(int8_field, DataType::INT8, nullable);
        auto int16_fid =
            schema_->AddDebugField(int16_field, DataType::INT16, nullable);
        auto int32_fid =
            schema_->AddDebugField(int32_field, DataType::INT32, nullable);
        auto int64_fid =
            schema_->AddDebugField(int64_field, DataType::INT64, nullable);
        auto float_fid =
            schema_->AddDebugField(float_field, DataType::FLOAT, nullable);
        auto double_fid =
            schema_->AddDebugField(double_field, DataType::DOUBLE, nullable);
        auto str1_fid =
            schema_->AddDebugField(string_field, DataType::VARCHAR, nullable);
        field_map_[bool_field] = bool_fid;
        field_map_[int8_field] = int8_fid;
        field_map_[int16_field] = int16_fid;
        field_map_[int32_field] = int32_fid;
        field_map_[int64_field] = int64_fid;
        field_map_[float_field] = float_fid;
        field_map_[double_field] = double_fid;
        field_map_[string_field] = str1_fid;

        schema_->set_primary_field_id(str1_fid);

        auto segment = CreateSealedSegment(schema_);
        num_rows_ = 10;
        auto raw_data =
            DataGen(schema_, num_rows_, 42, 0, 2, 10, false, false, false);
        auto fields = schema_->get_fields();
        for (auto field_data : raw_data.raw_->fields_data()) {
            int64_t field_id = field_data.field_id();

            auto info =
                FieldDataInfo(field_data.field_id(), num_rows_, "/tmp/a");
            auto field_meta = fields.at(FieldId(field_id));
            info.channel->push(CreateFieldDataFromDataArray(
                num_rows_, &field_data, field_meta));
            info.channel->close();

            segment->LoadFieldData(FieldId(field_id), info);
        }
        // load ts field data
        auto field_data =
            std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
        field_data->FillFieldData(raw_data.timestamps_.data(), num_rows_);
        auto ts_field_data_info =
            FieldDataInfo{TimestampFieldID.get(),
                          static_cast<size_t>(num_rows_),
                          std::vector<FieldDataPtr>{field_data}};
        segment->LoadFieldData(TimestampFieldID, ts_field_data_info);

        segment_ = SegmentSealedSPtr(segment.release());
    }

    void
    TearDown() override {
    }

 public:
    int64_t num_rows_{0};
    SegmentSealedSPtr segment_;
    std::shared_ptr<Schema> schema_;
    std::map<std::string, FieldId> field_map_;
};

INSTANTIATE_TEST_SUITE_P(TaskTestSuite,
                         QueryAggTest,
                         ::testing::Values(std::make_pair(true, true),
                                           std::make_pair(true, false),
                                           std::make_pair(false, false)));

RowVectorPtr
execPlan(std::shared_ptr<Task>& task) {
    RowVectorPtr ret = nullptr;
    for (;;) {
        auto result = task->Next();
        if (!result) {
            break;
        }
        if (ret) {
            auto childrens = result->childrens();
            AssertInfo(childrens.size() == ret->childrens().size(),
                       "column count of row vectors in different rounds"
                       "should be consistent, ret_column_count:{}, "
                       "new_result_column_count:{}",
                       childrens.size(),
                       ret->childrens().size());
            for (auto i = 0; i < childrens.size(); i++) {
                if (auto column_vec =
                        std::dynamic_pointer_cast<ColumnVector>(childrens[i])) {
                    auto ret_column_vector =
                        std::dynamic_pointer_cast<ColumnVector>(ret->child(i));
                    ret_column_vector->append(*column_vec);
                } else {
                    PanicInfo(UnexpectedError, "expr return type not matched");
                }
            }
        } else {
            ret = result;
        }
    }
    return ret;
}

TEST_P(QueryAggTest, GroupFixedLengthType) {
    std::vector<milvus::plan::PlanNodePtr> sources;
    auto [nullable, ignore_null_keys] = GetParam();
    //set up mvcc_node + project_node + agg_node
    // group by int16_field
    // mvcc node
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};
    // project node
    auto int16_id = field_map_[int16_field];
    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{int16_id},
        std::vector<std::string>{int16_field},
        std::vector<DataType>{DataType::INT16},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};
    // agg node
    std::vector<expr::FieldAccessTypeExprPtr> groupingKeys;
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::INT16, int16_field, int16_id));
    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        milvus::plan::AggregationNode::Step::kSingle,
        std::move(groupingKeys),
        std::vector<std::string>{},
        std::vector<plan::AggregationNode::Aggregate>{},
        ignore_null_keys,
        num_rows_,
        sources);

    auto plan = plan::PlanFragment(agg_node);
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        "test1",
        segment_.get(),
        num_rows_,
        MAX_TIMESTAMP,
        std::make_shared<milvus::exec::QueryConfig>(
            std::unordered_map<std::string, std::string>{}));

    auto task = Task::Create("task_query_group_by", plan, 0, query_context);
    RowVectorPtr ret = execPlan(task);
    EXPECT_EQ(1, ret->childrens().size());
    auto column = std::dynamic_pointer_cast<ColumnVector>(ret->child(0));
    if (nullable && ignore_null_keys) {
        // as there are 10 values repeating 2 three times, after groupby, at most 7 valid unique values will be returned
        EXPECT_TRUE(column->size() <= 5);
    } else if (!nullable) {
        EXPECT_TRUE(column->size() == 5);
    }

    if (!nullable || ignore_null_keys) {
        auto count = column->size();
        std::set<int16_t> set;
        for (auto i = 0; i < count; i++) {
            int16_t val = column->ValueAt<int16_t>(i);
            if (set.count(val) > 0) {
                EXPECT_TRUE(false);
                // there should not be any duplicated vals in the returned column
            }
            set.insert(val);
        }
        EXPECT_TRUE(set.size() == column->size());
    }
}

TEST_P(QueryAggTest, GroupFixedLengthMultipleColumn) {
    std::vector<milvus::plan::PlanNodePtr> sources;
    auto [nullable, ignore_null_keys] = GetParam();
    //set up mvcc_node + project_node + agg_node
    // group by int16_field and int32_field
    // mvcc node
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};
    // project node
    auto int16_id = field_map_[int16_field];
    auto int32_id = field_map_[int32_field];
    auto int64_id = field_map_[int64_field];
    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{int16_id, int32_id, int64_id},
        std::vector<std::string>{int16_field, int32_field, int64_field},
        std::vector<DataType>{
            DataType::INT16, DataType::INT32, DataType::INT64},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};
    // agg node, group by int16, int32, sum(int64)
    std::vector<expr::FieldAccessTypeExprPtr> groupingKeys;
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::INT16, int16_field, int16_id));
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::INT32, int32_field, int32_id));
    std::string agg_name = "sum";
    std::vector<plan::AggregationNode::Aggregate> aggregates;
    auto agg_input = std::make_shared<expr::FieldAccessTypeExpr>(
        DataType::INT64, int64_field, int64_id);
    auto call = std::make_shared<const expr::CallExpr>(
        agg_name, std::vector<expr::TypedExprPtr>{agg_input}, nullptr);
    aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
    aggregates.back().rawInputTypes_.emplace_back(DataType::INT64);
    aggregates.back().resultType_ = GetAggResultType(agg_name, DataType::INT64);
    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        milvus::plan::AggregationNode::Step::kSingle,
        std::move(groupingKeys),
        std::vector<std::string>{"sum"},
        std::move(aggregates),
        ignore_null_keys,
        num_rows_,
        sources);

    auto plan = plan::PlanFragment(agg_node);
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        "test1",
        segment_.get(),
        num_rows_,
        MAX_TIMESTAMP,
        std::make_shared<milvus::exec::QueryConfig>(
            std::unordered_map<std::string, std::string>{}));

    auto task = Task::Create("task_query_group_by", plan, 0, query_context);
    RowVectorPtr ret = execPlan(task);
    EXPECT_EQ(3, ret->childrens().size());
    int size = -1;
    for (int i = 0; i < 3; i++) {
        auto column = std::dynamic_pointer_cast<ColumnVector>(ret->child(0));
        if (size == -1) {
            size = column->size();
        } else {
            EXPECT_TRUE(size == column->size());
            // all columns in the returned row vector should be the same size
        }
    }
    if (nullable && ignore_null_keys) {
        EXPECT_TRUE(size <= 5);
    } else if (!nullable) {
        EXPECT_TRUE(size == 5);
    }

    for (int i = 0; i < 3; i++) {
        auto column = std::dynamic_pointer_cast<ColumnVector>(ret->child(i));
        for (auto j = 0; j < size; j++) {
            if (i == 0) {
                auto val = column->ValueAt<int16_t>(j);
                std::cout << "int16_val:" << val << std::endl;
            }
            if (i == 1) {
                auto val = column->ValueAt<int32_t>(j);
                std::cout << "int32_val:" << val << std::endl;
            }
            if (i == 2) {
                auto val = column->ValueAt<int64_t>(j);
                std::cout << "int64_val:" << val << std::endl;
            }
        }
    }
}

TEST_P(QueryAggTest, GroupVariableLengthMultipleColumn) {
    std::vector<milvus::plan::PlanNodePtr> sources;
    auto [nullable, ignore_null_keys] = GetParam();
    //set up mvcc_node + project_node + agg_node
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};
    // project node
    auto int8_id = field_map_[int8_field];
    auto str_id = field_map_[string_field];
    auto float_id = field_map_[float_field];
    auto double_id = field_map_[double_field];
    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{int8_id, str_id, float_id, double_id},
        std::vector<std::string>{
            int8_field, string_field, float_field, double_field},
        std::vector<DataType>{DataType::INT8,
                              DataType::VARCHAR,
                              DataType::FLOAT,
                              DataType::DOUBLE},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};
    // group by int8_field, str_field, sum(float), sum(double)
    std::vector<expr::FieldAccessTypeExprPtr> groupingKeys;
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::INT8, int8_field, int8_id));
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::VARCHAR, string_field, str_id));
    std::string agg_name = "sum";
    std::vector<plan::AggregationNode::Aggregate> aggregates;
    // agg1: sum(float)
    {
        auto agg_input = std::make_shared<expr::FieldAccessTypeExpr>(
            DataType::FLOAT, float_field, float_id);
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{agg_input}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().rawInputTypes_.emplace_back(DataType::FLOAT);
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::FLOAT);
    }
    // agg2: sum(double)
    {
        auto agg_input = std::make_shared<expr::FieldAccessTypeExpr>(
            DataType::DOUBLE, double_field, double_id);
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{agg_input}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().rawInputTypes_.emplace_back(DataType::DOUBLE);
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::DOUBLE);
    }
    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        milvus::plan::AggregationNode::Step::kSingle,
        std::move(groupingKeys),
        std::vector<std::string>{"sum", "sum"},
        std::move(aggregates),
        ignore_null_keys,
        num_rows_,
        sources);

    auto plan = plan::PlanFragment(agg_node);
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        "test1",
        segment_.get(),
        num_rows_,
        MAX_TIMESTAMP,
        std::make_shared<milvus::exec::QueryConfig>(
            std::unordered_map<std::string, std::string>{}));

    auto task = Task::Create("task_query_group_by", plan, 0, query_context);
    RowVectorPtr ret = execPlan(task);
    EXPECT_EQ(4, ret->childrens().size());
    int size = -1;
    for (int i = 0; i < 4; i++) {
        auto column = std::dynamic_pointer_cast<ColumnVector>(ret->child(0));
        if (size == -1) {
            size = column->size();
        } else {
            EXPECT_TRUE(size == column->size());
            // all columns in the returned row vector should be the same size
        }
    }
    if (nullable && ignore_null_keys) {
        EXPECT_TRUE(size <= 5);
    } else if (!nullable) {
        EXPECT_EQ(size, 5);
    }

    for (int i = 0; i < 4; i++) {
        auto column = std::dynamic_pointer_cast<ColumnVector>(ret->child(i));
        for (auto j = 0; j < size; j++) {
            if (i == 0) {
                auto val = column->ValueAt<int8_t>(j);
                std::cout << "int8_val:" << int32_t(val) << std::endl;
            }
            if (i == 1) {
                auto val = column->ValueAt<std::string>(j);
                std::cout << "str_val:" << val << std::endl;
            }
            if (i == 2) {
                auto val = column->ValueAt<double>(j);
                std::cout << "float_val:" << val << std::endl;
            }
            if (i == 3) {
                auto val = column->ValueAt<double>(j);
                std::cout << "double_val:" << val << std::endl;
            }
        }
    }
}

TEST_P(QueryAggTest, CountAggTest) {
    std::vector<milvus::plan::PlanNodePtr> sources;
    auto [nullable, ignore_null_keys] = GetParam();
    //set up mvcc_node + project_node + agg_node
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};
    // project node
    auto int8_id = field_map_[int8_field];
    auto str_id = field_map_[string_field];
    auto double_id = field_map_[double_field];
    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{int8_id, str_id, double_id},
        std::vector<std::string>{int8_field, string_field, double_field},
        std::vector<DataType>{
            DataType::INT8, DataType::VARCHAR, DataType::DOUBLE},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};
    // group by int8_field, str_field, count(*), count(double)
    std::vector<expr::FieldAccessTypeExprPtr> groupingKeys;
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::INT8, int8_field, int8_id));
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::VARCHAR, string_field, str_id));
    std::string agg_name = "count";
    std::vector<plan::AggregationNode::Aggregate> aggregates;
    //  count(*)
    {
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::NONE);
    }
    // count(double)
    {
        auto agg_input = std::make_shared<expr::FieldAccessTypeExpr>(
            DataType::DOUBLE, double_field, double_id);
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{agg_input}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().rawInputTypes_.emplace_back(DataType::DOUBLE);
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::DOUBLE);
    }
    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        milvus::plan::AggregationNode::Step::kSingle,
        std::move(groupingKeys),
        std::vector<std::string>{agg_name, agg_name},
        std::move(aggregates),
        ignore_null_keys,
        num_rows_,
        sources);

    auto plan = plan::PlanFragment(agg_node);
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        "test1",
        segment_.get(),
        num_rows_,
        MAX_TIMESTAMP,
        std::make_shared<milvus::exec::QueryConfig>(
            std::unordered_map<std::string, std::string>{}));

    auto task = Task::Create("task_query_group_by", plan, 0, query_context);
    RowVectorPtr ret = execPlan(task);
    EXPECT_EQ(4, ret->childrens().size());
    int size = -1;
    for (int i = 0; i < 4; i++) {
        auto column = std::dynamic_pointer_cast<ColumnVector>(ret->child(i));
        if (size == -1) {
            size = column->size();
        } else {
            EXPECT_TRUE(size == column->size());
            // all columns in the returned row vector should be the same size
        }
    }
    if (nullable && ignore_null_keys) {
        EXPECT_TRUE(size <= 5);
    } else if (!nullable) {
        EXPECT_EQ(size, 5);
    }

    for (int i = 0; i < 4; i++) {
        auto column = std::dynamic_pointer_cast<ColumnVector>(ret->child(i));
        for (auto j = 0; j < size; j++) {
            if (i == 0) {
                auto val = column->ValueAt<int8_t>(j);
                std::cout << "int8_val:" << int32_t(val) << std::endl;
            }
            if (i == 1) {
                auto val = column->ValueAt<std::string>(j);
                std::cout << "str_val:" << val << std::endl;
            }
            if (i == 2) {
                auto val = column->ValueAt<int64_t>(j);
                std::cout << "int64_t_val:" << val << std::endl;
            }
            if (i == 3) {
                auto val = column->ValueAt<int64_t>(j);
                std::cout << "int64_t_val:" << val << std::endl;
            }
        }
    }
}

TEST_P(QueryAggTest, GlobalCountAggTest) {
    std::vector<milvus::plan::PlanNodePtr> sources;
    auto [nullable, ignore_null_keys] = GetParam();
    //set up mvcc_node + agg_node: global aggregation no need project column
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};
    std::string agg_name = "count";
    std::vector<plan::AggregationNode::Aggregate> aggregates;
    //  count(*)
    {
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::NONE);
    }
    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        milvus::plan::AggregationNode::Step::kSingle,
        std::vector<expr::FieldAccessTypeExprPtr>{},
        std::vector<std::string>{agg_name},
        std::move(aggregates),
        ignore_null_keys,
        num_rows_,
        sources);

    auto plan = plan::PlanFragment(agg_node);
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        "test1",
        segment_.get(),
        num_rows_,
        MAX_TIMESTAMP,
        std::make_shared<milvus::exec::QueryConfig>(
            std::unordered_map<std::string, std::string>{}));

    auto task = Task::Create("task_query_group_by", plan, 0, query_context);
    RowVectorPtr ret = execPlan(task);
    EXPECT_EQ(1, ret->childrens().size());
    auto output = ret->childrens()[0];
    EXPECT_EQ(1, output->size());
    auto output_column = std::dynamic_pointer_cast<ColumnVector>(output);
    auto actual_count = output_column->ValueAt<int64_t>(0);
    std::cout << "count:" << actual_count << std::endl;
    EXPECT_EQ(num_rows_, actual_count);
    // count(*) will always get all results' count no matter nullable or ignoreNullKeys
}

// Test count(*) when activeCount is zero
TEST_P(QueryAggTest, GlobalCountEmptyTest) {
    std::vector<milvus::plan::PlanNodePtr> sources;
    auto [nullable, ignore_null_keys] = GetParam();
    //set up mvcc_node + agg_node: global aggregation no need project column
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};
    std::string agg_name = "count";
    std::vector<plan::AggregationNode::Aggregate> aggregates;
    //  count(*)
    {
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::NONE);
    }
    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        milvus::plan::AggregationNode::Step::kSingle,
        std::vector<expr::FieldAccessTypeExprPtr>{},
        std::vector<std::string>{agg_name},
        std::move(aggregates),
        ignore_null_keys,
        num_rows_,
        sources);

    auto plan = plan::PlanFragment(agg_node);
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        "test1",
        segment_.get(),
        0,
        MAX_TIMESTAMP,
        std::make_shared<milvus::exec::QueryConfig>(
            std::unordered_map<std::string, std::string>{}));

    auto task = Task::Create("task_query_group_by", plan, 0, query_context);
    RowVectorPtr ret = execPlan(task);
    EXPECT_EQ(1, ret->childrens().size());
    auto output = ret->childrens()[0];
    EXPECT_EQ(1, output->size());
    auto output_column = std::dynamic_pointer_cast<ColumnVector>(output);
    auto actual_count = output_column->ValueAt<int64_t>(0);
    EXPECT_EQ(0, actual_count);
    // count(*) will get zero if no valid input into agg node
}

TEST_P(QueryAggTest, AggLimitTest) {
    std::vector<milvus::plan::PlanNodePtr> sources;
    auto [nullable, ignore_null_keys] = GetParam();
    //set up mvcc_node + project_node + agg_node
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};
    // project node
    auto int8_id = field_map_[int8_field];
    auto str_id = field_map_[string_field];
    auto double_id = field_map_[double_field];
    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{int8_id, str_id, double_id},
        std::vector<std::string>{int8_field, string_field, double_field},
        std::vector<DataType>{
            DataType::INT8, DataType::VARCHAR, DataType::DOUBLE},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};
    // group by int8_field, str_field, limit=2
    int64_t group_limit = 2;
    std::vector<expr::FieldAccessTypeExprPtr> groupingKeys;
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::INT8, int8_field, int8_id));
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::VARCHAR, string_field, str_id));
    std::vector<plan::AggregationNode::Aggregate> aggregates;
    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        milvus::plan::AggregationNode::Step::kSingle,
        std::move(groupingKeys),
        std::vector<std::string>{},
        std::move(aggregates),
        ignore_null_keys,
        group_limit,
        sources);

    auto plan = plan::PlanFragment(agg_node);
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        "test1",
        segment_.get(),
        num_rows_,
        MAX_TIMESTAMP,
        std::make_shared<milvus::exec::QueryConfig>(
            std::unordered_map<std::string, std::string>{}));

    auto task = Task::Create("task_query_group_by", plan, 0, query_context);
    RowVectorPtr ret = execPlan(task);
    EXPECT_EQ(2, ret->childrens().size());
    int size = -1;
    for (int i = 0; i < 2; i++) {
        auto column = std::dynamic_pointer_cast<ColumnVector>(ret->child(i));
        if (size == -1) {
            size = column->size();
        } else {
            EXPECT_TRUE(size == column->size());
            // all columns in the returned row vector should be the same size
        }
    }
    EXPECT_EQ(size, group_limit);
    for (int i = 0; i < 2; i++) {
        auto column = std::dynamic_pointer_cast<ColumnVector>(ret->child(i));
        for (auto j = 0; j < size; j++) {
            if (i == 0) {
                auto val = column->ValueAt<int8_t>(j);
                std::cout << "int8_val:" << int8_t(val) << std::endl;
            }
            if (i == 1) {
                auto val = column->ValueAt<std::string>(j);
                std::cout << "str_val:" << val << std::endl;
            }
        }
    }
}