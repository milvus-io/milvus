// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <algorithm>
#include <memory>
#include <numeric>
#include <vector>

#include "common/Types.h"
#include "common/Vector.h"
#include "plan/PlanNode.h"
#include "plan/PlanNodeIdGenerator.h"
#include "gtest/gtest.h"

using namespace milvus;
using namespace milvus::plan;

// Helper: create a ColumnVector of int64 values
static std::shared_ptr<ColumnVector>
MakeInt64Column(const std::vector<int64_t>& vals) {
    auto col = std::make_shared<ColumnVector>(DataType::INT64, vals.size());
    for (size_t i = 0; i < vals.size(); i++) {
        col->SetValueAt<int64_t>(i, vals[i]);
    }
    return col;
}

// Helper: create a ColumnVector of double values
static std::shared_ptr<ColumnVector>
MakeDoubleColumn(const std::vector<double>& vals) {
    auto col = std::make_shared<ColumnVector>(DataType::DOUBLE, vals.size());
    for (size_t i = 0; i < vals.size(); i++) {
        col->SetValueAt<double>(i, vals[i]);
    }
    return col;
}

// Helper: create a RowVector from columns
static RowVectorPtr
MakeRowVector(std::vector<VectorPtr>&& cols) {
    return std::make_shared<RowVector>(std::move(cols));
}

// ==================== ComputeProjectNode Tests ====================

TEST(ComputeProjectNodeTest, OutputTypeInherited) {
    std::vector<ComputeProjectNode::ProjectItem> items;
    items.push_back(ComputeProjectNode::ProjectItem{
        "div", {"value", "1000.0"}, "result"});

    auto project_node = std::make_shared<ComputeProjectNode>(
        GetNextPlanNodeId(), std::move(items));

    // Without sources, output_type should be None
    EXPECT_EQ(project_node->output_type(), RowType::None);
}

TEST(ComputeProjectNodeTest, ProjectItems) {
    std::vector<ComputeProjectNode::ProjectItem> items;
    items.push_back(ComputeProjectNode::ProjectItem{
        "mul", {"a", "b"}, "product"});
    items.push_back(ComputeProjectNode::ProjectItem{
        "add", {"x", "1.0"}, "x_plus_one"});

    auto project_node = std::make_shared<ComputeProjectNode>(
        GetNextPlanNodeId(), std::move(items));

    EXPECT_EQ(project_node->items().size(), 2);
    EXPECT_EQ(project_node->items()[0].function_name, "mul");
    EXPECT_EQ(project_node->items()[0].alias, "product");
    EXPECT_EQ(project_node->items()[1].function_name, "add");
    EXPECT_EQ(project_node->items()[1].args.size(), 2);
}

// ==================== RowVector Utility Tests ====================

TEST(RowVectorTest, BasicConstruction) {
    auto col1 = MakeInt64Column({10, 20, 30});
    auto col2 = MakeDoubleColumn({1.1, 2.2, 3.3});

    auto rv = MakeRowVector({col1, col2});
    EXPECT_EQ(rv->size(), 3);
    EXPECT_EQ(rv->childrens().size(), 2);

    auto c1 = std::dynamic_pointer_cast<ColumnVector>(rv->child(0));
    EXPECT_EQ(c1->ValueAt<int64_t>(0), 10);
    EXPECT_EQ(c1->ValueAt<int64_t>(2), 30);

    auto c2 = std::dynamic_pointer_cast<ColumnVector>(rv->child(1));
    EXPECT_DOUBLE_EQ(c2->ValueAt<double>(1), 2.2);
}

TEST(RowVectorTest, ColumnVectorSetAndGet) {
    auto col = std::make_shared<ColumnVector>(DataType::INT64, 5);
    for (int i = 0; i < 5; i++) {
        col->SetValueAt<int64_t>(i, i * 100);
    }
    EXPECT_EQ(col->ValueAt<int64_t>(0), 0);
    EXPECT_EQ(col->ValueAt<int64_t>(3), 300);
    EXPECT_EQ(col->ValueAt<int64_t>(4), 400);
}

// ==================== RowType Tests ====================

TEST(RowTypeTest, GetChildIndex) {
    auto row_type = std::make_shared<RowType>(
        std::vector<std::string>{"id", "name", "score"},
        std::vector<DataType>{
            DataType::INT64, DataType::INT64, DataType::DOUBLE});

    EXPECT_EQ(row_type->GetChildIndex("id"), 0);
    EXPECT_EQ(row_type->GetChildIndex("name"), 1);
    EXPECT_EQ(row_type->GetChildIndex("score"), 2);
}

TEST(RowTypeTest, ColumnCount) {
    auto row_type = std::make_shared<RowType>(
        std::vector<std::string>{"a", "b"},
        std::vector<DataType>{DataType::INT64, DataType::DOUBLE});

    EXPECT_EQ(row_type->column_count(), 2);
    EXPECT_EQ(row_type->column_type(0), DataType::INT64);
    EXPECT_EQ(row_type->column_type(1), DataType::DOUBLE);
}
