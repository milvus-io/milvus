// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <folly/CancellationToken.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <new>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "common/Schema.h"
#include "common/Utils.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/Utils.h"
#include "storage/ThreadPools.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace std::chrono_literals;

namespace {

class InstrumentedRetrieveSegment : public SegmentGrowingImpl {
 public:
    explicit InstrumentedRetrieveSegment(SchemaPtr schema)
        : SegmentGrowingImpl(std::move(schema),
                             nullptr,
                             SegcoreConfig::default_config(),
                             101) {
    }

    using SegmentGrowingImpl::bulk_subscript;

    std::unique_ptr<DataArray>
    bulk_subscript(milvus::OpContext* op_ctx,
                   FieldId field_id,
                   const int64_t* offsets,
                   int64_t count) const override {
        if (before_fetch) {
            before_fetch(field_id, op_ctx);
        }
        return SegmentGrowingImpl::bulk_subscript(
            op_ctx, field_id, offsets, count);
    }

    void
    bulk_subscript(milvus::OpContext* op_ctx,
                   SystemFieldType system_type,
                   const int64_t* offsets,
                   int64_t count,
                   void* output) const override {
        if (before_system_fetch) {
            before_system_fetch(system_type, op_ctx);
        }
        SegmentGrowingImpl::bulk_subscript(
            op_ctx, system_type, offsets, count, output);
    }

    std::function<void(FieldId, milvus::OpContext*)> before_fetch;
    std::function<void(SystemFieldType, milvus::OpContext*)>
        before_system_fetch;
};

class RetrieveOutputFieldsTest : public ::testing::TestWithParam<DataType> {
 protected:
    void
    SetUp() override {
        auto& pool = ThreadPools::GetThreadPool(ThreadPoolPriority::MIDDLE);
        original_middle_size_ = pool.GetMaxThreadNum();
        if (original_middle_size_ < 2) {
            pool.Resize(2);
        }

        schema = std::make_shared<Schema>();
        pk = schema->AddDebugField("pk", GetParam());
        first = schema->AddDebugField("first", DataType::INT64);
        last = schema->AddDebugField("last", DataType::INT64);
        schema->set_primary_field_id(pk);
        segment = std::make_unique<InstrumentedRetrieveSegment>(schema);
        auto data = DataGen(schema, 4);
        auto reserved = segment->PreInsert(4);
        segment->Insert(reserved,
                        4,
                        data.row_ids_.data(),
                        data.timestamps_.data(),
                        data.raw_);
        plan = std::make_unique<query::RetrievePlan>(schema);
        plan->field_ids_ = {first, pk, last};
    }

    void
    TearDown() override {
        auto& pool = ThreadPools::GetThreadPool(ThreadPoolPriority::MIDDLE);
        pool.Resize(original_middle_size_);
    }

    std::unique_ptr<DataArray>
    ExpectedField(FieldId field_id) {
        return segment->SegmentGrowingImpl::bulk_subscript(
            nullptr, field_id, offsets.data(), offsets.size());
    }

    void
    CheckIDs(const proto::segcore::RetrieveResults& results) {
        auto expected = ExpectedField(pk);
        if (GetParam() == DataType::INT64) {
            EXPECT_EQ(results.ids().int_id().SerializeAsString(),
                      expected->scalars().long_data().SerializeAsString());
        } else {
            EXPECT_EQ(results.ids().str_id().SerializeAsString(),
                      expected->scalars().string_data().SerializeAsString());
        }
    }

    SchemaPtr schema;
    FieldId pk{0};
    FieldId first{0};
    FieldId last{0};
    std::unique_ptr<InstrumentedRetrieveSegment> segment;
    std::unique_ptr<query::RetrievePlan> plan;
    const std::vector<int64_t> offsets{3, 1, 3};
    int original_middle_size_{1};
};

TEST_P(RetrieveOutputFieldsTest, ParallelReadsPreserveOrderIDsAndStorageCost) {
    std::promise<void> last_started;
    auto last_ready = last_started.get_future();
    milvus::OpContext parent_ctx;
    parent_ctx.runtime_load_priority = 1;
    parent_ctx.coload_fields = {first.get(), last.get()};
    parent_ctx.storage_usage.scanned_cold_bytes = 1000;
    parent_ctx.storage_usage.scanned_total_bytes = 2000;
    segment->before_fetch = [&](FieldId field_id, milvus::OpContext* ctx) {
        EXPECT_NE(ctx, &parent_ctx);
        EXPECT_EQ(ctx->runtime_load_priority, parent_ctx.runtime_load_priority);
        EXPECT_EQ(ctx->coload_fields, parent_ctx.coload_fields);
        EXPECT_EQ(ctx->storage_usage.scanned_cold_bytes.load(), 0);
        EXPECT_EQ(ctx->storage_usage.scanned_total_bytes.load(), 0);
        ctx->storage_usage.scanned_cold_bytes += field_id.get();
        ctx->storage_usage.scanned_total_bytes += field_id.get() * 2;
        if (field_id.get() == first.get()) {
            EXPECT_EQ(last_ready.wait_for(5s), std::future_status::ready);
        } else if (field_id.get() == last.get()) {
            last_started.set_value();
        }
    };

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    results->set_scanned_remote_bytes(7);
    results->set_scanned_total_bytes(11);
    segment->FillTargetEntry(nullptr,
                             plan.get(),
                             results,
                             offsets.data(),
                             offsets.size(),
                             false,
                             true,
                             &parent_ctx);

    ASSERT_EQ(results->fields_data_size(), plan->field_ids_.size());
    for (size_t i = 0; i < plan->field_ids_.size(); ++i) {
        EXPECT_EQ(results->fields_data(i).SerializeAsString(),
                  ExpectedField(plan->field_ids_[i])->SerializeAsString());
    }
    CheckIDs(*results);
    auto bytes = first.get() + pk.get() + last.get();
    EXPECT_EQ(results->scanned_remote_bytes(), 7 + bytes);
    EXPECT_EQ(results->scanned_total_bytes(), 11 + bytes * 2);
    EXPECT_EQ(parent_ctx.storage_usage.scanned_cold_bytes.load(), 1000);
    EXPECT_EQ(parent_ctx.storage_usage.scanned_total_bytes.load(), 2000);
}

TEST_P(RetrieveOutputFieldsTest, PKOnlyKeepsTimestampAndSkipsOtherReads) {
    plan->field_ids_ = {first, TimestampFieldID, pk, last};
    std::atomic<int> calls{0};
    segment->before_fetch = [&](FieldId field_id, milvus::OpContext*) {
        EXPECT_EQ(field_id.get(), pk.get());
        ++calls;
    };
    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    segment->FillTargetEntry(nullptr,
                             plan.get(),
                             results,
                             offsets.data(),
                             offsets.size(),
                             true,
                             true);

    EXPECT_EQ(calls.load(), 1);
    CheckIDs(*results);
    ASSERT_EQ(results->fields_data_size(), 1);
    EXPECT_EQ(results->fields_data(0).field_id(), TimestampFieldID.get());
    const auto& timestamps = results->fields_data(0).scalars().long_data();
    ASSERT_EQ(timestamps.data_size(), offsets.size());
    FixedVector<int64_t> expected_timestamps(offsets.size());
    segment->SegmentGrowingImpl::bulk_subscript(nullptr,
                                                SystemFieldType::Timestamp,
                                                offsets.data(),
                                                offsets.size(),
                                                expected_timestamps.data());
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_EQ(timestamps.data(i), expected_timestamps[i]);
    }
}

TEST_P(RetrieveOutputFieldsTest, RetrieveByOffsetsUsesParallelFieldReads) {
    std::promise<void> last_started;
    auto last_ready = last_started.get_future();
    segment->before_fetch = [&](FieldId field_id, milvus::OpContext*) {
        if (field_id.get() == first.get()) {
            EXPECT_EQ(last_ready.wait_for(5s), std::future_status::ready);
        } else if (field_id.get() == last.get()) {
            last_started.set_value();
        }
    };
    auto results = segment->Retrieve(nullptr,
                                     plan.get(),
                                     offsets.data(),
                                     offsets.size(),
                                     folly::CancellationToken());
    ASSERT_EQ(results->fields_data_size(), plan->field_ids_.size());
    for (size_t i = 0; i < plan->field_ids_.size(); ++i) {
        EXPECT_EQ(results->fields_data(i).SerializeAsString(),
                  ExpectedField(plan->field_ids_[i])->SerializeAsString());
    }
    EXPECT_EQ(results->ids().id_field_case(), IdArray::ID_FIELD_NOT_SET);
}

TEST_P(RetrieveOutputFieldsTest, FailureDrainsOtherReadsBeforeReturn) {
    plan->field_ids_ = {last, first};
    std::promise<void> first_started;
    auto first_ready = first_started.get_future().share();
    std::promise<void> release_first;
    auto released = release_first.get_future();
    std::atomic<bool> first_exited{false};
    segment->before_fetch = [&](FieldId field_id, milvus::OpContext*) {
        if (field_id.get() == first.get()) {
            first_started.set_value();
            EXPECT_EQ(released.wait_for(5s), std::future_status::ready);
            first_exited = true;
        } else {
            EXPECT_EQ(first_ready.wait_for(5s), std::future_status::ready);
            throw SegcoreError(ErrorCode::FileReadFailed,
                               "injected read failure");
        }
    };
    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    auto pending = std::async(std::launch::async, [&] {
        segment->FillTargetEntry(nullptr,
                                 plan.get(),
                                 results,
                                 offsets.data(),
                                 offsets.size(),
                                 false,
                                 false);
    });
    EXPECT_EQ(first_ready.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(pending.wait_for(0s), std::future_status::timeout);
    release_first.set_value();
    try {
        pending.get();
        FAIL() << "expected field read failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FileReadFailed);
        EXPECT_STREQ(error.what(), "injected read failure");
    }
    EXPECT_TRUE(first_exited.load());
    EXPECT_EQ(results->fields_data_size(), 0);
    EXPECT_EQ(results->scanned_remote_bytes(), 0);
}

TEST_P(RetrieveOutputFieldsTest, PreCancelledRetrieveDoesNotReadFields) {
    std::atomic<int> calls{0};
    segment->before_fetch = [&](FieldId, milvus::OpContext*) { ++calls; };
    folly::CancellationSource source;
    source.requestCancellation();
    try {
        segment->Retrieve(nullptr,
                          plan.get(),
                          offsets.data(),
                          offsets.size(),
                          source.getToken());
        FAIL() << "expected cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_EQ(calls.load(), 0);
}

TEST_P(RetrieveOutputFieldsTest, CancellationReachesRunningField) {
    plan->field_ids_ = {first};
    folly::CancellationSource source;
    std::promise<void> started;
    auto ready = started.get_future();
    segment->before_fetch = [&](FieldId, milvus::OpContext* ctx) {
        std::promise<void> cancelled;
        auto cancelled_ready = cancelled.get_future();
        folly::CancellationCallback on_cancel(ctx->cancellation_token,
                                              [&] { cancelled.set_value(); });
        started.set_value();
        EXPECT_EQ(cancelled_ready.wait_for(5s), std::future_status::ready);
        CheckCancellation(ctx, segment->get_segment_id(), "test read");
    };
    auto pending = std::async(std::launch::async, [&] {
        return segment->Retrieve(nullptr,
                                 plan.get(),
                                 offsets.data(),
                                 offsets.size(),
                                 source.getToken());
    });
    EXPECT_EQ(ready.wait_for(5s), std::future_status::ready);
    source.requestCancellation();
    try {
        pending.get();
        FAIL() << "expected cancellation during field read";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
}

TEST_P(RetrieveOutputFieldsTest, AllocationFailurePreservesExceptionType) {
    segment->before_fetch = [&](FieldId field_id, milvus::OpContext*) {
        if (field_id.get() == last.get()) {
            throw std::bad_alloc();
        }
    };
    EXPECT_THROW(segment->Retrieve(nullptr,
                                   plan.get(),
                                   offsets.data(),
                                   offsets.size(),
                                   folly::CancellationToken()),
                 std::bad_alloc);
}

TEST_P(RetrieveOutputFieldsTest, DynamicProjectionAndMissingFields) {
    auto local_schema = std::make_shared<Schema>();
    auto local_pk = local_schema->AddDebugField("pk", GetParam());
    local_schema->set_primary_field_id(local_pk);
    auto dynamic = local_schema->AddDebugField("$meta", DataType::JSON);
    local_schema->set_dynamic_field_id(dynamic);
    auto array =
        local_schema->AddDebugArrayField("array", DataType::INT64, false);
    auto local_segment = CreateGrowingSegment(local_schema, nullptr);
    auto data = DataGen(local_schema, 4);
    for (auto& field : *data.raw_->mutable_fields_data()) {
        if (field.field_id() == dynamic.get()) {
            for (auto& json : *field.mutable_scalars()
                                   ->mutable_json_data()
                                   ->mutable_data()) {
                json = R"({"keep":42,"drop":99})";
            }
        }
    }
    auto reserved = local_segment->PreInsert(4);
    local_segment->Insert(
        reserved, 4, data.row_ids_.data(), data.timestamps_.data(), data.raw_);

    auto evolved = std::make_shared<Schema>(*local_schema);
    DefaultValueType default_value;
    default_value.set_long_data(42);
    auto added = evolved->AddDebugFieldWithDefaultValue(
        "added", DataType::INT64, default_value);
    auto missing_array =
        evolved->AddDebugArrayField("missing_array", DataType::VARCHAR, true);
    query::RetrievePlan local_plan(evolved);
    local_plan.field_ids_ = {dynamic, added, array, missing_array, local_pk};
    local_plan.target_dynamic_fields_ = {"keep"};
    auto results = local_segment->Retrieve(nullptr,
                                           &local_plan,
                                           offsets.data(),
                                           offsets.size(),
                                           folly::CancellationToken());
    ASSERT_EQ(results->fields_data_size(), 5);
    const auto& added_validity =
        GetFieldDataRowValidData(results->fields_data(1));
    const auto& array_validity =
        GetFieldDataRowValidData(results->fields_data(3));
    ASSERT_EQ(added_validity.size(), offsets.size());
    ASSERT_EQ(array_validity.size(), offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_EQ(results->fields_data(0).scalars().json_data().data(i),
                  R"({"keep":42})");
        EXPECT_EQ(results->fields_data(1).scalars().long_data().data(i), 42);
        EXPECT_TRUE(added_validity[i]);
        EXPECT_FALSE(array_validity[i]);
    }
    EXPECT_EQ(results->fields_data(2).scalars().array_data().element_type(),
              proto::schema::DataType::Int64);
    EXPECT_EQ(results->fields_data(3).scalars().array_data().element_type(),
              proto::schema::DataType::VarChar);
}

TEST_P(RetrieveOutputFieldsTest, OrderByDeferredFieldsUseParallelReads) {
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->pipeline_field_ids_ = {pk, SegmentOffsetFieldID};
    plan->plan_node_->deferred_field_ids_ = {first, last};
    plan->field_ids_ = {pk, first, last, TimestampFieldID};

    RetrieveResult retrieve_result;
    auto pk_data = ExpectedField(pk);
    retrieve_result.field_data_.push_back(std::move(*pk_data));
    DataArray offset_data;
    offset_data.set_type(proto::schema::DataType::Int64);
    offset_data.mutable_scalars()->mutable_long_data()->mutable_data()->Add(
        offsets.data(), offsets.data() + offsets.size());
    retrieve_result.field_data_.push_back(std::move(offset_data));

    std::promise<void> last_started;
    auto last_ready = last_started.get_future();
    std::promise<void> system_started;
    auto system_ready = system_started.get_future();
    milvus::OpContext parent_ctx;
    parent_ctx.storage_usage.scanned_cold_bytes = 1000;
    segment->before_fetch = [&](FieldId field_id, milvus::OpContext* ctx) {
        EXPECT_NE(ctx, &parent_ctx);
        ctx->storage_usage.scanned_cold_bytes += field_id.get();
        if (field_id.get() == first.get()) {
            EXPECT_EQ(last_ready.wait_for(5s), std::future_status::ready);
            EXPECT_EQ(system_ready.wait_for(5s), std::future_status::ready);
        } else if (field_id.get() == last.get()) {
            last_started.set_value();
        }
    };
    segment->before_system_fetch = [&](SystemFieldType system_type,
                                       milvus::OpContext* ctx) {
        EXPECT_EQ(system_type, SystemFieldType::Timestamp);
        EXPECT_NE(ctx, &parent_ctx);
        system_started.set_value();
    };

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    results->set_scanned_remote_bytes(7);
    segment->FillOrderByResult(
        plan.get(), results, retrieve_result, &parent_ctx);

    ASSERT_EQ(results->fields_data_size(), 4);
    EXPECT_EQ(results->fields_data(0).SerializeAsString(),
              ExpectedField(pk)->SerializeAsString());
    EXPECT_EQ(results->fields_data(1).SerializeAsString(),
              ExpectedField(first)->SerializeAsString());
    EXPECT_EQ(results->fields_data(2).SerializeAsString(),
              ExpectedField(last)->SerializeAsString());
    EXPECT_EQ(results->fields_data(3).field_id(), TimestampFieldID.get());
    FixedVector<int64_t> expected_timestamps(offsets.size());
    segment->SegmentGrowingImpl::bulk_subscript(nullptr,
                                                SystemFieldType::Timestamp,
                                                offsets.data(),
                                                offsets.size(),
                                                expected_timestamps.data());
    const auto& timestamps = results->fields_data(3).scalars().long_data();
    ASSERT_EQ(timestamps.data_size(), offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_EQ(timestamps.data(i), expected_timestamps[i]);
    }
    CheckIDs(*results);
    EXPECT_EQ(results->scanned_remote_bytes(), 7 + first.get() + last.get());
    EXPECT_EQ(parent_ctx.storage_usage.scanned_cold_bytes.load(), 1000);
    EXPECT_TRUE(retrieve_result.field_data_.empty());
}

INSTANTIATE_TEST_SUITE_P(PrimaryKeyTypes,
                         RetrieveOutputFieldsTest,
                         ::testing::Values(DataType::INT64, DataType::VARCHAR));

}  // namespace
