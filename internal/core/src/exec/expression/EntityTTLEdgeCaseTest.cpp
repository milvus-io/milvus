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

// Test: search without filter + TTL field applies TTL filtering (issue #47977)
TEST(PhysicalTimeEdgeCase, SearchWithoutFilterTTLFiltering) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto ttl_fid = schema->AddDebugField("ttl_field", DataType::TIMESTAMPTZ);
    schema->set_primary_field_id(pk_fid);
    schema->set_ttl_field_id(ttl_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto segment_internal =
        dynamic_cast<SegmentInternalInterface*>(segment.get());

    int64_t test_data_count = 100;
    uint64_t base_ts = 1000000000ULL << 18;

    std::vector<Timestamp> ts_data(test_data_count);
    std::vector<idx_t> row_ids(test_data_count);
    std::vector<int64_t> pk_data(test_data_count);
    std::vector<int64_t> ttl_data(test_data_count);

    int64_t current_physical_us = 1770026400000000LL;

    for (int i = 0; i < test_data_count; i++) {
        ts_data[i] = base_ts + i;
        row_ids[i] = i;
        pk_data[i] = i;

        if (i < test_data_count / 2) {
            ttl_data[i] = current_physical_us - 1000000;
        } else {
            ttl_data[i] = current_physical_us + 1000000;
        }
    }

    auto insert_record_proto = std::make_unique<InsertRecordProto>();
    insert_record_proto->set_num_rows(test_data_count);

    {
        auto field_data = insert_record_proto->add_fields_data();
        field_data->set_field_id(pk_fid.get());
        field_data->set_type(proto::schema::DataType::Int64);
        auto* scalars = field_data->mutable_scalars();
        auto* data = scalars->mutable_long_data();
        for (auto v : pk_data) {
            data->add_data(v);
        }
    }

    {
        auto field_data = insert_record_proto->add_fields_data();
        field_data->set_field_id(ttl_fid.get());
        field_data->set_type(proto::schema::DataType::Timestamptz);
        auto* scalars = field_data->mutable_scalars();
        auto* data = scalars->mutable_timestamptz_data();
        for (auto v : ttl_data) {
            data->add_data(v);
        }
    }

    auto offset = segment->PreInsert(test_data_count);
    segment->Insert(offset,
                    test_data_count,
                    row_ids.data(),
                    ts_data.data(),
                    insert_record_proto.get());

    uint64_t query_ts = base_ts + test_data_count;
    int64_t active_count = segment->get_active_count(query_ts);

    // AlwaysTrueExpr + FilterBitsNode: simulates the fixed plan path
    // for search without filter when TTL field is configured.
    auto always_true_expr = std::make_shared<expr::AlwaysTrueExpr>();
    auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       always_true_expr);
    BitsetType bitset = query::ExecuteQueryExpr(
        plan, segment_internal, active_count, query_ts, current_physical_us);
    BitsetTypeView bitset_view(bitset);

    int expired_count = 0;
    for (int i = 0; i < test_data_count; i++) {
        if (!bitset_view[i]) {
            expired_count++;
        }
    }

    EXPECT_EQ(expired_count, test_data_count / 2);
    for (int i = 0; i < test_data_count / 2; i++) {
        EXPECT_FALSE(bitset_view[i]) << "Row " << i << " should be expired";
    }
    for (int i = test_data_count / 2; i < test_data_count; i++) {
        EXPECT_TRUE(bitset_view[i]) << "Row " << i << " should not be expired";
    }
}
