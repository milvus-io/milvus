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

#include <gtest/gtest.h>
#include "exec/QueryContext.h"
#include "exec/expression/Expr.h"
#include "query/PlanProto.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::segcore;

// Test: No TTL field in schema (should return nullptr)
TEST(PhysicalTimeEdgeCase, NoTTLField) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    // No TTL field set

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto segment_internal =
        dynamic_cast<SegmentInternalInterface*>(segment.get());

    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        "test_query",
        segment_internal,
        0,
        1000ULL << 18,
        0,
        0,
        query::PlanOptions(),
        std::make_shared<milvus::exec::QueryConfig>(),
        nullptr,
        std::unordered_map<std::string,
                           std::shared_ptr<milvus::exec::BaseConfig>>(),
        1770026400000000LL);

    // Should return nullptr when no TTL field is configured
    auto ttl_expr = exec::CreateTTLFieldFilterExpression(query_context.get());
    EXPECT_EQ(ttl_expr, nullptr) << "Should return nullptr when no TTL field";
}

// Test edge case: entity_ttl_physical_time_us = 0 (should use default from query_timestamp)
TEST(PhysicalTimeEdgeCase, ZeroPhysicalTimeUs) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto segment_internal =
        dynamic_cast<SegmentInternalInterface*>(segment.get());

    uint64_t query_ts = 1000000ULL << 18;
    int64_t expected_physical_us =
        static_cast<int64_t>(TimestampToPhysicalMs(query_ts)) * 1000;

    // Create QueryContext with entity_ttl_physical_time_us = 0
    milvus::exec::QueryContext ctx(
        "test_query",
        segment_internal,
        0,
        query_ts,
        0,
        0,
        query::PlanOptions(),
        std::make_shared<milvus::exec::QueryConfig>(),
        nullptr,
        std::unordered_map<std::string,
                           std::shared_ptr<milvus::exec::BaseConfig>>(),
        0  // entity_ttl_physical_time_us = 0, should use query_timestamp conversion
    );

    // Should use converted query_timestamp
    EXPECT_EQ(ctx.get_entity_ttl_physical_time_us(), expected_physical_us);
}
