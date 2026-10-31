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

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <folly/CancellationToken.h>
#include <folly/ScopeGuard.h>
#include <folly/system/ThreadName.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <future>
#include <memory>
#include <new>
#include <thread>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "common/Schema.h"
#include "common/Utils.h"
#include "futures/Executor.h"
#include "futures/future_c.h"
#include "query/PlanImpl.h"
#include "query/PlanProto.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/Utils.h"
#include "segcore/retrieve_result_export_c.h"
#include "segcore/segment_c.h"
#include "storage/ThreadPools.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/output_field_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace std::chrono_literals;
using milvus::test::AssertCompletesInChild;

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

    std::shared_ptr<const SegmentReadSnapshot>
    CaptureReadSnapshot() const override {
        ++read_snapshot_captures;
        if (before_snapshot) {
            before_snapshot();
        }
        return SegmentGrowingImpl::CaptureReadSnapshot();
    }

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

    mutable std::atomic<int> read_snapshot_captures{0};
    std::function<void()> before_snapshot;
    std::function<void(FieldId, milvus::OpContext*)> before_fetch;
    std::function<void(SystemFieldType, milvus::OpContext*)>
        before_system_fetch;
};

class RetrieveOutputFieldsTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        auto& pool =
            ThreadPools::GetThreadPool(ThreadPoolPriority::SEARCH_FANOUT);
        original_fanout_size_ = pool.GetMaxThreadNum();
        if (original_fanout_size_ < 2) {
            pool.Resize(2);
        }

        schema = std::make_shared<Schema>();
        pk = schema->AddDebugField("pk", pk_type);
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
                        std::make_shared<InsertRecordProto>(*data.raw_));
        plan = std::make_unique<query::RetrievePlan>(schema);
        plan->field_ids_ = {first, pk, last};
    }

    void
    TearDown() override {
        auto& pool =
            ThreadPools::GetThreadPool(ThreadPoolPriority::SEARCH_FANOUT);
        pool.Resize(original_fanout_size_);
    }

    std::unique_ptr<DataArray>
    ExpectedField(FieldId field_id) {
        return segment->SegmentGrowingImpl::bulk_subscript(
            nullptr, field_id, offsets.data(), offsets.size());
    }

    void
    CheckIDs(const proto::segcore::RetrieveResults& results) {
        auto expected = ExpectedField(pk);
        if (pk_type == DataType::INT64) {
            EXPECT_EQ(results.ids().int_id().SerializeAsString(),
                      expected->scalars().long_data().SerializeAsString());
        } else {
            EXPECT_EQ(results.ids().str_id().SerializeAsString(),
                      expected->scalars().string_data().SerializeAsString());
        }
    }

    DataType pk_type{DataType::INT64};
    SchemaPtr schema;
    FieldId pk{0};
    FieldId first{0};
    FieldId last{0};
    std::unique_ptr<InstrumentedRetrieveSegment> segment;
    std::unique_ptr<query::RetrievePlan> plan;
    const std::vector<int64_t> offsets{3, 1, 3};
    int original_fanout_size_{1};
};

class RetrieveOutputFieldsPKTest
    : public RetrieveOutputFieldsTest,
      public ::testing::WithParamInterface<DataType> {
 protected:
    void
    SetUp() override {
        pk_type = GetParam();
        RetrieveOutputFieldsTest::SetUp();
    }
};

TEST_P(RetrieveOutputFieldsPKTest,
       ParallelReadsPreserveOrderIDsAndStorageCost) {
    segment->read_snapshot_captures = 0;
    std::promise<void> last_started;
    auto last_ready = last_started.get_future();
    milvus::OpContext parent_ctx;
    parent_ctx.runtime_load_priority = 1;
    parent_ctx.coload_fields = {first.get(), last.get()};
    parent_ctx.storage_usage.scanned_cold_bytes = 1000;
    parent_ctx.storage_usage.scanned_total_bytes = 2000;
    segment->before_fetch = [&](FieldId field_id, milvus::OpContext* ctx) {
        EXPECT_TRUE(folly::getCurrentThreadName().value_or("").starts_with(
            "SRCH_FAN_POOL"));
        EXPECT_EQ(segment->read_snapshot_captures.load(), 1);
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
    EXPECT_EQ(segment->read_snapshot_captures.load(), 1);
}

TEST_P(RetrieveOutputFieldsPKTest, PKOnlyKeepsTimestampAndSkipsOtherReads) {
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

TEST_P(RetrieveOutputFieldsPKTest, RetrieveByOffsetsUsesParallelFieldReads) {
    AssertCompletesInChild([&] {
        auto* search = milvus::futures::getSearchCPUExecutor();
        const auto original_search_size = search->numThreads();
        search->setNumThreads(1);
        auto restore_search = folly::makeGuard(
            [&] { search->setNumThreads(original_search_size); });
        segment->read_snapshot_captures = 0;
        segment->before_snapshot = [] {
            EXPECT_TRUE(folly::getCurrentThreadName().value_or("").starts_with(
                "MILVUS_SEARCH_"));
        };
        std::promise<void> last_started;
        auto last_ready = last_started.get_future();
        segment->before_fetch = [&](FieldId field_id, milvus::OpContext*) {
            EXPECT_TRUE(folly::getCurrentThreadName().value_or("").starts_with(
                "SRCH_FAN_POOL"));
            EXPECT_EQ(segment->read_snapshot_captures.load(), 1);
            if (field_id.get() == first.get()) {
                EXPECT_EQ(last_ready.wait_for(5s), std::future_status::ready);
            } else if (field_id.get() == last.get()) {
                last_started.set_value();
            }
        };
        auto mutable_offsets = offsets;
        auto* future = AsyncRetrieveByOffsets({},
                                              segment.get(),
                                              plan.get(),
                                              mutable_offsets.data(),
                                              mutable_offsets.size());
        auto destroy_future = folly::makeGuard([&] { future_destroy(future); });
        void* raw_result = nullptr;
        auto pending = std::async(std::launch::async, [&] {
            while (!future_is_ready(future)) {
                std::this_thread::sleep_for(1ms);
            }
            return future_leak_and_get(future, &raw_result);
        });
        ASSERT_EQ(pending.wait_for(5s), std::future_status::ready);
        auto status = pending.get();
        ASSERT_EQ(status.error_code, 0) << status.error_msg;
        auto* c_result = static_cast<CRetrieveResult*>(raw_result);
        auto destroy_result =
            folly::makeGuard([&] { DeleteRetrieveResult(c_result); });
        auto results = std::make_unique<proto::segcore::RetrieveResults>();
        ASSERT_TRUE(results->ParseFromArray(c_result->proto_blob,
                                            c_result->proto_size));
        ASSERT_EQ(results->fields_data_size(), plan->field_ids_.size());
        for (size_t i = 0; i < plan->field_ids_.size(); ++i) {
            EXPECT_EQ(results->fields_data(i).SerializeAsString(),
                      ExpectedField(plan->field_ids_[i])->SerializeAsString());
        }
        EXPECT_EQ(results->ids().id_field_case(), IdArray::ID_FIELD_NOT_SET);
        EXPECT_EQ(segment->read_snapshot_captures.load(), 1);
    });
}

TEST_F(RetrieveOutputFieldsTest, OrderedArrowOutputUsesBothPoolsWithOneWorker) {
    AssertCompletesInChild([&] {
        auto* search = milvus::futures::getSearchCPUExecutor();
        const auto original_search_size = search->numThreads();
        search->setNumThreads(1);
        auto restore_search = folly::makeGuard(
            [&] { search->setNumThreads(original_search_size); });
        auto& fanout =
            ThreadPools::GetThreadPool(ThreadPoolPriority::SEARCH_FANOUT);
        fanout.Resize(1);
        auto second = std::make_unique<InstrumentedRetrieveSegment>(schema);
        auto second_data = DataGen(schema, 4, /*seed=*/2);
        auto reserved = second->PreInsert(4);
        second->Insert(reserved,
                       4,
                       second_data.row_ids_.data(),
                       second_data.timestamps_.data(),
                       std::make_shared<InsertRecordProto>(*second_data.raw_));
        for (auto* current : {segment.get(), second.get()}) {
            current->read_snapshot_captures = 0;
            current->before_snapshot = [] {
                EXPECT_TRUE(
                    folly::getCurrentThreadName().value_or("").starts_with(
                        "MILVUS_SEARCH_"));
            };
            current->before_fetch = [](FieldId, milvus::OpContext*) {
                EXPECT_TRUE(
                    folly::getCurrentThreadName().value_or("").starts_with(
                        "SRCH_FAN_POOL"));
            };
        }
        plan->field_ids_ = {last, first};
        CSegmentInterface c_segments[] = {segment.get(), second.get()};
        int32_t indices[] = {1, 0, 1};
        int64_t selected_offsets[] = {3, 1, 0};
        ArrowSchema out_schema{};
        ArrowArray out_array{};
        auto cleanup = folly::makeGuard([&] {
            if (out_array.release) {
                out_array.release(&out_array);
            }
            if (out_schema.release) {
                out_schema.release(&out_schema);
            }
        });
        auto pending = std::async(std::launch::async, [&] {
            return FillRetrieveFieldsOrdered(c_segments,
                                             2,
                                             plan.get(),
                                             indices,
                                             selected_offsets,
                                             3,
                                             &out_schema,
                                             &out_array,
                                             nullptr);
        });
        ASSERT_EQ(pending.wait_for(5s), std::future_status::ready);
        auto status = pending.get();
        ASSERT_EQ(status.error_code, 0) << status.error_msg;
        auto batch = arrow::ImportRecordBatch(&out_array, &out_schema);
        ASSERT_TRUE(batch.ok()) << batch.status();
        ASSERT_EQ((*batch)->num_columns(), 2);
        ASSERT_EQ((*batch)->num_rows(), 3);
        for (size_t i = 0; i < plan->field_ids_.size(); ++i) {
            auto actual = std::static_pointer_cast<arrow::Int64Array>(
                (*batch)->column(i));
            for (size_t row = 0; row < 3; ++row) {
                auto* expected_segment =
                    indices[row] == 0 ? segment.get() : second.get();
                auto expected =
                    expected_segment->SegmentGrowingImpl::bulk_subscript(
                        nullptr,
                        plan->field_ids_[i],
                        &selected_offsets[row],
                        1);
                EXPECT_EQ(actual->Value(row),
                          expected->scalars().long_data().data(0));
            }
        }
        EXPECT_EQ(segment->read_snapshot_captures.load(), 1);
        EXPECT_EQ(second->read_snapshot_captures.load(), 1);
    });
}

TEST_F(RetrieveOutputFieldsTest, ConcurrentInsertCompletesDuringOutputReads) {
    schema = std::make_shared<Schema>();
    pk = schema->AddDebugField("pk", DataType::INT64);
    first = schema->AddDebugField("first", DataType::INT64);
    last = schema->AddDebugField("last", DataType::INT64);
    // Insert updates the size statistics for this non-output VARCHAR field.
    schema->AddDebugField("insert_text", DataType::VARCHAR);
    schema->set_primary_field_id(pk);
    segment = std::make_unique<InstrumentedRetrieveSegment>(schema);
    auto initial_data = DataGen(schema, 4);
    auto reserved = segment->PreInsert(4);
    segment->Insert(reserved,
                    4,
                    initial_data.row_ids_.data(),
                    initial_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*initial_data.raw_));

    proto::plan::PlanNode node;
    for (auto field : {pk, first, last, TimestampFieldID}) {
        node.add_output_field_ids(field.get());
    }
    node.mutable_query()->set_limit(4);
    plan = query::ProtoParser(schema).CreateRetrievePlan(node);
    auto insert_data = DataGen(schema, 2, /*seed=*/10);

    for (bool by_offsets : {false, true}) {
        SCOPED_TRACE(by_offsets ? "RetrieveByOffsets" : "Retrieve");
        auto read = [&] {
            if (by_offsets) {
                return segment->Retrieve(nullptr,
                                         plan.get(),
                                         offsets.data(),
                                         offsets.size(),
                                         folly::CancellationToken());
            }
            return segment->Retrieve(nullptr,
                                     plan.get(),
                                     MAX_TIMESTAMP,
                                     DEFAULT_MAX_OUTPUT_SIZE,
                                     false,
                                     folly::CancellationToken(),
                                     0,
                                     0);
        };
        auto expected = read();
        std::promise<void> first_started, release_first;
        auto first_ready = first_started.get_future();
        auto released = release_first.get_future();
        segment->before_fetch = [&](FieldId field_id, milvus::OpContext*) {
            if (field_id == first) {
                first_started.set_value();
                EXPECT_EQ(released.wait_for(15s), std::future_status::ready);
            }
        };
        auto pending = std::async(std::launch::async, read);
        EXPECT_EQ(first_ready.wait_for(5s), std::future_status::ready);
        auto inserting = std::async(std::launch::async, [&] {
            auto offset = segment->PreInsert(2);
            segment->Insert(
                offset,
                2,
                insert_data.row_ids_.data(),
                insert_data.timestamps_.data(),
                std::make_shared<InsertRecordProto>(*insert_data.raw_));
        });
        EXPECT_EQ(inserting.wait_for(5s), std::future_status::ready);
        EXPECT_EQ(pending.wait_for(0ms), std::future_status::timeout);
        release_first.set_value();
        inserting.get();
        auto actual = pending.get();
        segment->before_fetch = {};
        EXPECT_EQ(actual->SerializeAsString(), expected->SerializeAsString());
    }
}

TEST_F(RetrieveOutputFieldsTest, FailureDrainsOtherReadsBeforeReturn) {
    plan->field_ids_ = {last, first};
    std::promise<void> first_started;
    auto first_ready = first_started.get_future().share();
    std::promise<void> last_failed;
    auto failure_ready = last_failed.get_future();
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
            last_failed.set_value();
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
    EXPECT_EQ(failure_ready.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(pending.wait_for(50ms), std::future_status::timeout);
    EXPECT_FALSE(first_exited.load());
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

TEST_F(RetrieveOutputFieldsTest, PreCancelledRetrieveDoesNotReadFields) {
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

TEST_F(RetrieveOutputFieldsTest, CancellationReachesRunningField) {
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

TEST_F(RetrieveOutputFieldsTest, AllocationFailurePreservesExceptionType) {
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

TEST_F(RetrieveOutputFieldsTest, DynamicProjectionAndMissingFields) {
    auto local_schema = std::make_shared<Schema>();
    auto local_pk = local_schema->AddDebugField("pk", pk_type);
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
    local_segment->Insert(reserved,
                          4,
                          data.row_ids_.data(),
                          data.timestamps_.data(),
                          std::make_shared<InsertRecordProto>(*data.raw_));

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

TEST_P(RetrieveOutputFieldsPKTest, OrderByDeferredFieldsUseParallelReads) {
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
                         RetrieveOutputFieldsPKTest,
                         ::testing::Values(DataType::INT64, DataType::VARCHAR));

class InstrumentedSealedRetrieveSegment : public ChunkedSegmentSealedImpl {
 public:
    explicit InstrumentedSealedRetrieveSegment(SchemaPtr schema)
        : ChunkedSegmentSealedImpl(std::move(schema),
                                   nullptr,
                                   SegcoreConfig::default_config(),
                                   kSegmentID) {
    }

    using ChunkedSegmentSealedImpl::bulk_subscript;

    void
    bulk_subscript(milvus::OpContext* ctx,
                   SystemFieldType system_type,
                   const int64_t* offsets,
                   int64_t count,
                   void* output) const override {
        if (before_system_fetch) {
            before_system_fetch(ctx);
        }
        ChunkedSegmentSealedImpl::bulk_subscript(
            ctx, system_type, offsets, count, output);
    }

    std::function<void(milvus::OpContext*)> before_system_fetch;
};

enum class RetrievePath { Normal, ByOffsets, OrderBy };

// Run read-lease/Reopen tests in a child so a deadlock cannot hang all_tests.
class SealedRetrieveOutputFieldsDeathTest : public ::testing::Test {
 protected:
    void
    Init() {
        ThreadPools::GetThreadPool(ThreadPoolPriority::SEARCH_FANOUT).Resize(2);
        schema = std::make_shared<Schema>();
        pk = schema->AddDebugField("pk", DataType::INT64);
        first = schema->AddDebugField("first", DataType::INT64);
        last = schema->AddDebugField("last", DataType::INT64);
        vector = schema->AddDebugField(
            "vector", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
        schema->set_primary_field_id(pk);
        schema->set_schema_version(100);
        segment = std::make_unique<InstrumentedSealedRetrieveSegment>(schema);
        auto data = DataGen(schema, 4);
        LoadGeneratedDataIntoSegment(data, segment.get());
        auto vectors = data.get_col<float>(vector);
        auto reader = GenVecIndexing(
            4, 4, vectors.data(), knowhere::IndexEnum::INDEX_FAISS_IDMAP);
        LoadIndexInfo vector_index{};
        vector_index.field_id = vector.get();
        vector_index.field_type = DataType::VECTOR_FLOAT;
        vector_index.index_params = GenIndexParams(reader.get());
        SetTestIndexMetadata(
            vector_index, *reader, index::families::kVectorMem);
        vector_index.cache_index =
            CreateTestCacheIndex("retrieve-vector", std::move(reader));
        segment->LoadIndex(vector_index);
        ASSERT_TRUE(segment->IndexHasRawData(vector));
        segment->DropFieldData(vector);
        ASSERT_FALSE(segment->HasFieldData(vector));

        proto::plan::PlanNode node;
        for (auto field : {pk, first, last, vector, TimestampFieldID}) {
            node.add_output_field_ids(field.get());
        }
        node.mutable_query()->set_limit(4);
        if (retrieve_path == RetrievePath::OrderBy) {
            auto* order = node.mutable_query()->add_order_by_fields();
            order->set_field_id(first.get());
            order->set_ascending(true);
        }
        plan = query::ProtoParser(schema).CreateRetrievePlan(node);
        plan->take_for_output_allowed_ = false;
        if (retrieve_path == RetrievePath::OrderBy) {
            ASSERT_FALSE(plan->plan_node_->deferred_field_ids_.empty());
        }
        expected = Retrieve(folly::CancellationToken());
    }

    std::unique_ptr<proto::segcore::RetrieveResults>
    Retrieve(const folly::CancellationToken& token) {
        // Match the C API: lease outlives every output task, including errors.
        auto lease = segment->AcquireReadLease(token);
        if (retrieve_path == RetrievePath::ByOffsets) {
            return segment->Retrieve(
                nullptr, plan.get(), offsets.data(), offsets.size(), token);
        }
        return segment->Retrieve(nullptr,
                                 plan.get(),
                                 MAX_TIMESTAMP,
                                 DEFAULT_MAX_OUTPUT_SIZE,
                                 false,
                                 token,
                                 0,
                                 0);
    }

    void
    CheckResult(const proto::segcore::RetrieveResults& result) {
        ASSERT_EQ(result.fields_data_size(), expected->fields_data_size());
        for (int i = 0; i < result.fields_data_size(); ++i) {
            EXPECT_EQ(result.fields_data(i).SerializeAsString(),
                      expected->fields_data(i).SerializeAsString());
        }
        EXPECT_EQ(result.ids().SerializeAsString(),
                  expected->ids().SerializeAsString());
        ASSERT_EQ(result.offset_size(), expected->offset_size());
        for (int i = 0; i < result.offset_size(); ++i) {
            EXPECT_EQ(result.offset(i), expected->offset(i));
        }
    }

    void
    ConcurrentReopen(bool cancel, bool ordered_arrow = false) {
        ASSERT_NO_FATAL_FAILURE(Init());
        auto current = segment->TestGetPublishedStateSnapshot();
        auto next_schema = std::make_shared<Schema>(*schema);
        next_schema->AddDebugField("new_field", DataType::INT64);
        next_schema->set_schema_version(200);
        std::promise<void> entered;
        auto entered_future = entered.get_future();
        std::promise<void> release;
        auto released = release.get_future();
        std::atomic<bool> field_exited{false};
        folly::CancellationSource source;
        segment->before_system_fetch = [&](milvus::OpContext* ctx) {
            entered.set_value();
            released.wait();
            field_exited = true;
            CheckCancellation(
                ctx, segment->get_segment_id(), "test output read");
        };
        auto query = std::async(std::launch::async, [&] {
            if (ordered_arrow) {
                CSegmentInterface c_segment = segment.get();
                int32_t indices[] = {0, 0, 0};
                ArrowSchema out_schema{};
                ArrowArray out_array{};
                auto cleanup = folly::makeGuard([&] {
                    if (out_array.release) {
                        out_array.release(&out_array);
                    }
                    if (out_schema.release) {
                        out_schema.release(&out_schema);
                    }
                });
                auto status = FillRetrieveFieldsOrdered(&c_segment,
                                                        1,
                                                        plan.get(),
                                                        indices,
                                                        offsets.data(),
                                                        offsets.size(),
                                                        &out_schema,
                                                        &out_array,
                                                        &source);
                EXPECT_EQ(status.error_code, ErrorCode::FollyCancel)
                    << status.error_msg;
                EXPECT_EQ(out_array.release, nullptr);
                EXPECT_EQ(out_schema.release, nullptr);
                if (status.error_code != 0) {
                    free(const_cast<char*>(status.error_msg));
                }
            } else if (cancel) {
                try {
                    Retrieve(source.getToken());
                    ADD_FAILURE() << "expected cancellation during output read";
                } catch (const SegcoreError& error) {
                    EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
                }
            } else {
                CheckResult(*Retrieve(source.getToken()));
            }
        });
        entered_future.wait();
        auto reopen = std::async(std::launch::async,
                                 [&] { segment->Reopen(next_schema); });
        while (!segment->TestReadGateWriterPending()) {
            std::this_thread::yield();
        }
        EXPECT_EQ(segment->TestGetPublishedStateSnapshot(), current);
        EXPECT_EQ(reopen.wait_for(0s), std::future_status::timeout);
        if (cancel) {
            source.requestCancellation();
            // Cancellation must not destroy the read lease while a submitted
            // field task still owns pointers to the segment and its snapshot.
            EXPECT_EQ(query.wait_for(50ms), std::future_status::timeout);
            EXPECT_FALSE(field_exited.load());
            EXPECT_EQ(segment->TestGetPublishedStateSnapshot(), current);
        }
        release.set_value();
        query.get();
        EXPECT_TRUE(field_exited.load());
        reopen.get();
        EXPECT_NE(segment->TestGetPublishedStateSnapshot(), current);
        EXPECT_EQ(segment->TestGetSchemaSnapshot()->get_schema_version(), 200);
    }

    RetrievePath retrieve_path{RetrievePath::Normal};
    SchemaPtr schema;
    FieldId pk{0}, first{0}, last{0}, vector{0};
    std::unique_ptr<InstrumentedSealedRetrieveSegment> segment;
    std::unique_ptr<query::RetrievePlan> plan;
    std::unique_ptr<proto::segcore::RetrieveResults> expected;
    const std::vector<int64_t> offsets{3, 1, 3};
};

class SealedRetrievePathsDeathTest
    : public SealedRetrieveOutputFieldsDeathTest,
      public ::testing::WithParamInterface<RetrievePath> {
 protected:
    void
    SetUp() override {
        retrieve_path = GetParam();
    }
};

TEST_P(SealedRetrievePathsDeathTest, ReopenWaitsForOutputReadLease) {
    AssertCompletesInChild([&] { ConcurrentReopen(false); });
}

TEST_P(SealedRetrievePathsDeathTest, CancelDrainsOutputBeforeReopenPublishes) {
    AssertCompletesInChild([&] { ConcurrentReopen(true); });
}

TEST_F(SealedRetrieveOutputFieldsDeathTest,
       ArrowCancellationDrainsReadsBeforeReopenPublishes) {
    AssertCompletesInChild(
        [&] { ConcurrentReopen(/*cancel=*/true, /*ordered_arrow=*/true); });
}

INSTANTIATE_TEST_SUITE_P(RetrievePaths,
                         SealedRetrievePathsDeathTest,
                         ::testing::Values(RetrievePath::Normal,
                                           RetrievePath::ByOffsets,
                                           RetrievePath::OrderBy));

}  // namespace
