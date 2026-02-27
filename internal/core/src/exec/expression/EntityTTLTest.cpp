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

// Test that QueryContext correctly stores and returns entity_ttl_physical_time_us
TEST(PhysicalTime, QueryContextPhysicalTimeUs) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto segment_internal =
        dynamic_cast<SegmentInternalInterface*>(segment.get());

    // Test 1: entity_ttl_physical_time_us explicitly provided
    {
        int64_t expected_physical_us = 1234567890123456LL;
        milvus::exec::QueryContext ctx(
            "test_query",
            segment_internal,
            0,
            100ULL << 18,  // query_timestamp (TSO)
            0,             // collection_ttl
            0,             // consistency_level
            query::PlanOptions(),
            std::make_shared<milvus::exec::QueryConfig>(),
            nullptr,
            std::unordered_map<std::string,
                               std::shared_ptr<milvus::exec::BaseConfig>>(),
            expected_physical_us  // entity_ttl_physical_time_us
        );

        EXPECT_EQ(ctx.get_entity_ttl_physical_time_us(), expected_physical_us);
    }

    // Test 2: entity_ttl_physical_time_us not provided (uses default from query_timestamp)
    {
        uint64_t query_ts = 1000000ULL << 18;
        int64_t expected_physical_us =
            static_cast<int64_t>(TimestampToPhysicalMs(query_ts)) * 1000;

        milvus::exec::QueryContext ctx(
            "test_query",
            segment_internal,
            0,
            query_ts,  // query_timestamp
            0,
            0,
            query::PlanOptions(),
            std::make_shared<milvus::exec::QueryConfig>(),
            nullptr,
            std::unordered_map<std::string,
                               std::shared_ptr<milvus::exec::BaseConfig>>(),
            0  // entity_ttl_physical_time_us = 0, should use query_timestamp
        );

        EXPECT_EQ(ctx.get_entity_ttl_physical_time_us(), expected_physical_us);
    }

    // Test 3: entity_ttl_physical_time_us different from query_timestamp
    {
        uint64_t old_query_ts = 1000000ULL << 18;
        int64_t current_physical_us = 1770026400000000LL;

        milvus::exec::QueryContext ctx(
            "test_query",
            segment_internal,
            0,
            old_query_ts,
            0,
            0,
            query::PlanOptions(),
            std::make_shared<milvus::exec::QueryConfig>(),
            nullptr,
            std::unordered_map<std::string,
                               std::shared_ptr<milvus::exec::BaseConfig>>(),
            current_physical_us);

        EXPECT_EQ(ctx.get_query_timestamp(), old_query_ts);
        EXPECT_EQ(ctx.get_entity_ttl_physical_time_us(), current_physical_us);
    }
}

// Test that TTL filter uses entity_ttl_physical_time_us correctly
TEST(PhysicalTime, TTLFilterWithPhysicalTime) {
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
            // Expired: TTL < current time
            ttl_data[i] = current_physical_us - 1000000;
        } else {
            // Not expired: TTL > current time
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

    uint64_t old_query_ts = base_ts + test_data_count;
    int64_t active_count = segment->get_active_count(old_query_ts);

    // Use ExecuteQueryExpr helper with entity_ttl_physical_time_us
    auto ttl_expr = exec::CreateTTLFieldFilterExpression(
        std::make_shared<milvus::exec::QueryContext>(
            "test",
            segment_internal,
            active_count,
            old_query_ts,
            0,
            0,
            query::PlanOptions(),
            std::make_shared<milvus::exec::QueryConfig>(),
            nullptr,
            std::unordered_map<std::string,
                               std::shared_ptr<milvus::exec::BaseConfig>>(),
            current_physical_us)
            .get());

    ASSERT_NE(ttl_expr, nullptr);

    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, ttl_expr);
    BitsetType bitset = query::ExecuteQueryExpr(plan,
                                                segment_internal,
                                                active_count,
                                                old_query_ts,
                                                current_physical_us);
    BitsetTypeView bitset_view(bitset);

    // Verify: first half expired, second half valid
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

// Test Strong consistency scenario (issue #47413)
TEST(PhysicalTime, StrongConsistencyScenario) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto ttl_fid = schema->AddDebugField("ttl_field", DataType::TIMESTAMPTZ);
    schema->set_primary_field_id(pk_fid);
    schema->set_ttl_field_id(ttl_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto segment_internal =
        dynamic_cast<SegmentInternalInterface*>(segment.get());

    int64_t test_data_count = 100;
    uint64_t insert_ts = 1000000000ULL << 18;

    std::vector<Timestamp> ts_data(test_data_count);
    std::vector<idx_t> row_ids(test_data_count);
    std::vector<int64_t> pk_data(test_data_count);
    std::vector<int64_t> ttl_data(test_data_count);

    int64_t insert_physical_us =
        static_cast<int64_t>(TimestampToPhysicalMs(insert_ts)) * 1000;
    int64_t expire_physical_us = insert_physical_us + 5000000;  // +5 seconds

    for (int i = 0; i < test_data_count; i++) {
        ts_data[i] = insert_ts + i;
        row_ids[i] = i;
        pk_data[i] = i;
        ttl_data[i] = expire_physical_us;  // All expire at same time
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

    uint64_t query_ts = insert_ts + test_data_count;
    int64_t active_count = segment->get_active_count(query_ts);

    // Scenario 1: At insert time (not expired)
    {
        auto ttl_expr = exec::CreateTTLFieldFilterExpression(
            std::make_shared<milvus::exec::QueryContext>(
                "test",
                segment_internal,
                active_count,
                query_ts,
                0,
                0,
                query::PlanOptions(),
                std::make_shared<milvus::exec::QueryConfig>(),
                nullptr,
                std::unordered_map<std::string,
                                   std::shared_ptr<milvus::exec::BaseConfig>>(),
                insert_physical_us)
                .get());

        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           ttl_expr);
        BitsetType bitset = query::ExecuteQueryExpr(
            plan, segment_internal, active_count, query_ts, insert_physical_us);
        BitsetTypeView bitset_view(bitset);

        int valid_count = 0;
        for (int i = 0; i < test_data_count; i++) {
            if (bitset_view[i])
                valid_count++;
        }
        EXPECT_EQ(valid_count, test_data_count)
            << "At insert time, all data should be valid";
    }

    // Scenario 2: After expiration (Strong consistency bug scenario - issue #47413)
    {
        int64_t after_expire_physical_us = expire_physical_us + 1000000;

        auto ttl_expr = exec::CreateTTLFieldFilterExpression(
            std::make_shared<milvus::exec::QueryContext>(
                "test",
                segment_internal,
                active_count,
                query_ts,
                0,
                0,
                query::PlanOptions(),
                std::make_shared<milvus::exec::QueryConfig>(),
                nullptr,
                std::unordered_map<std::string,
                                   std::shared_ptr<milvus::exec::BaseConfig>>(),
                after_expire_physical_us)
                .get());

        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           ttl_expr);
        BitsetType bitset = query::ExecuteQueryExpr(plan,
                                                    segment_internal,
                                                    active_count,
                                                    query_ts,
                                                    after_expire_physical_us);
        BitsetTypeView bitset_view(bitset);

        int valid_count = 0;
        for (int i = 0; i < test_data_count; i++) {
            if (bitset_view[i])
                valid_count++;
        }
        EXPECT_EQ(valid_count, 0)
            << "After expiration, all data should be filtered";
    }
}
