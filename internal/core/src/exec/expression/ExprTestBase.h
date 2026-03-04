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

// This file contains common includes and the ExprTest base class
// used by all ExprTest files.
#pragma once

#include <algorithm>
#include <any>
#include <boost/format.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>
#include <chrono>
#include <roaring/roaring.hh>

#include "common/FieldDataInterface.h"
#include "common/Geometry.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/Types.h"
#include "common/Exception.h"
#include "folly/CancellationToken.h"
#include "gtest/gtest.h"
#include "index/Meta.h"
#include "index/JsonInvertedIndex.h"
#include "index/BitmapIndex.h"
#include "knowhere/comp/index_param.h"
#include "mmap/Types.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "query/Plan.h"
#include "query/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegmentGrowingImpl.h"
#include "simdjson/padded_string.h"
#include "storage/FileManager.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/storage_test_utils.h"
#include "index/IndexFactory.h"
#include "exec/Task.h"
#include "exec/expression/function/FunctionFactory.h"
#include "expr/ITypeExpr.h"
#include "mmap/Types.h"
#include "test_utils/cachinglayer_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

class ExprTest
    : public ::testing::TestWithParam<
          std::tuple<std::pair<milvus::DataType, knowhere::MetricType>, bool>> {
 public:
    void
    SetUp() override {
        auto param = GetParam();
        data_type = std::get<0>(param).first;  // Get the DataType from the pair
        metric_type =
            std::get<0>(param).second;  // Get the MetricType from the pair
        GROWING_JSON_KEY_STATS_ENABLED =
            std::get<1>(param);  // Get the bool parameter
    }

    void
    TearDown() override {
        // Clear the cached schema handle
        schema_handle_.reset();
        schema_.reset();
    }

    // This registers the schema to the plan parser, so call SetSchema only after all fields
    // are added to the schema.
    void
    SetSchema(const milvus::SchemaPtr& schema) {
        if (schema_ != schema) {
            schema_handle_.reset();
            schema_ = schema;
            schema_handle_.emplace(*schema);
        }
    }

    // Create a search plan from a string expression using the parameterized
    // metric_type. This is the preferred approach over text proto format.
    // Parameters:
    //   schema: the schema to use for parsing
    //   expr: the filter expression (e.g., "age > 10 && age < 100")
    //   vector_field_name: name of the vector field (default: "fakevec")
    //   topk: number of top results (default: 10)
    //   search_params: JSON search parameters (default: {"nprobe": 10})
    //   round_decimal: decimal places for rounding (default: 3)
    std::vector<char>
    create_search_plan_from_expr(
        const std::string& expr,
        const std::string& vector_field_name = "fakevec",
        int64_t topk = 10,
        const std::string& search_params = R"({"nprobe": 10})",
        int64_t round_decimal = 3) {
        Assert(schema_handle_.has_value() &&
               "Must call SetSchema before using this overload");
        return schema_handle_->ParseSearch(expr,
                                           vector_field_name,
                                           topk,
                                           metric_type,
                                           search_params,
                                           round_decimal);
    }

    milvus::DataType data_type;
    knowhere::MetricType metric_type;

 protected:
    milvus::SchemaPtr schema_;
    std::optional<milvus::segcore::ScopedSchemaHandle> schema_handle_;
};

// Macro to instantiate the ExprTest suite with standard parameter combinations.
// Use this macro once at the top of each test file after including ExprTestBase.h.
#define EXPR_TEST_INSTANTIATE()                                                \
    INSTANTIATE_TEST_SUITE_P(                                                  \
        ExprTestSuite,                                                         \
        ExprTest,                                                              \
        ::testing::Values(                                                     \
            std::make_tuple(std::pair(milvus::DataType::VECTOR_FLOAT,          \
                                      knowhere::metric::L2),                   \
                            false),                                            \
            std::make_tuple(std::pair(milvus::DataType::VECTOR_SPARSE_U32_F32, \
                                      knowhere::metric::IP),                   \
                            false),                                            \
            std::make_tuple(std::pair(milvus::DataType::VECTOR_BINARY,         \
                                      knowhere::metric::JACCARD),              \
                            false),                                            \
            std::make_tuple(std::pair(milvus::DataType::VECTOR_FLOAT,          \
                                      knowhere::metric::L2),                   \
                            true),                                             \
            std::make_tuple(std::pair(milvus::DataType::VECTOR_SPARSE_U32_F32, \
                                      knowhere::metric::IP),                   \
                            true),                                             \
            std::make_tuple(std::pair(milvus::DataType::VECTOR_BINARY,         \
                                      knowhere::metric::JACCARD),              \
                            true)))
