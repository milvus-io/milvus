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

#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <folly/CancellationToken.h>
#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>
#include <parquet/arrow/writer.h>

#include <cstdlib>
#include <functional>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/ColumnarArrayChunkBuilder.h"
#include "common/Schema.h"
#include "pb/cgo_msg.pb.h"
#include "pb/plan.pb.h"
#include "query/PlanImpl.h"
#include "query/PlanProto.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/arrow_field_utils.h"
#include "segcore/retrieve_result_export_c.h"
#include "segcore/search_result_export_c.h"

namespace {

using namespace milvus;
using namespace milvus::segcore;

struct ArrowTakeAccessor : ChunkedSegmentSealedImpl {
    using ChunkedSegmentSealedImpl::ArrowToDataArray;
};

void
CheckFailure(CStatus status, ErrorCode expected) {
    EXPECT_EQ(status.error_code, expected) << status.error_msg;
    EXPECT_NE(status.error_msg, nullptr);
    free(const_cast<char*>(status.error_msg));
}

class OutOfMemoryPool final : public arrow::ProxyMemoryPool {
 public:
    OutOfMemoryPool() : arrow::ProxyMemoryPool(arrow::default_memory_pool()) {
    }

    arrow::Status
    Allocate(int64_t, int64_t, uint8_t** out) override {
        *out = nullptr;
        return arrow::Status::OutOfMemory("injected allocator exhaustion");
    }

    arrow::Status
    Reallocate(int64_t, int64_t, int64_t, uint8_t**) override {
        return arrow::Status::OutOfMemory("injected allocator exhaustion");
    }
};

// The real virtual materialization call is the fault injection point. No C ABI
// or Arrow export implementation is mocked, so exception type preservation is
// checked across each public entry point.
class FaultingSegment final : public SegmentGrowingImpl {
 public:
    explicit FaultingSegment(SchemaPtr schema)
        : SegmentGrowingImpl(
              std::move(schema), nullptr, SegcoreConfig::default_config(), 1) {
    }

    std::function<void()> fault = [] {};

    std::unique_ptr<DataArray>
    bulk_subscript(OpContext*,
                   FieldId field_id,
                   const int64_t*,
                   int64_t count) const override {
        fault();
        auto data = std::make_unique<DataArray>();
        data->set_field_id(field_id.get());
        data->set_type(proto::schema::DataType::Int64);
        for (int64_t i = 0; i < count; ++i) {
            data->mutable_scalars()->mutable_long_data()->add_data(7);
        }
        return data;
    }
};

class ArrowExportBoundaryTest : public testing::Test {
 protected:
    void
    SetUp() override {
        schema = std::make_shared<Schema>();
        auto pk = schema->AddDebugField("pk", DataType::INT64);
        schema->set_primary_field_id(pk);
        field = schema->AddDebugField("value", DataType::INT64);
        auto vector =
            schema->AddDebugField("vector", DataType::VECTOR_FLOAT, 4, "L2");
        segment = std::make_unique<FaultingSegment>(schema);

        proto::plan::PlanNode node;
        auto* anns = node.mutable_vector_anns();
        anns->set_field_id(vector.get());
        anns->set_vector_type(proto::plan::VectorType::FloatVector);
        anns->set_placeholder_tag("$0");
        anns->mutable_query_info()->set_topk(1);
        anns->mutable_query_info()->set_metric_type("L2");
        anns->mutable_query_info()->set_search_params("{}");
        auto blob = node.SerializeAsString();
        search_plan =
            query::CreateSearchPlanByExpr(schema, blob.data(), blob.size());
        retrieve_plan = std::make_unique<query::RetrievePlan>(schema);
        retrieve_plan->field_ids_ = {field};

        proto::cgo::FunctionChainInputPlan inputs;
        auto* input = inputs.add_inputs();
        input->set_source_field_id(field.get());
        input->set_target_data_type(proto::schema::DataType::Int64);
        input->set_logical_name("value");
        input_plan = inputs.SerializeAsString();

        result.segment_ = segment.get();
        result.total_nq_ = 1;
        result.unity_topK_ = 1;
        result.pk_type_ = DataType::INT64;
        result.primary_keys_ = {PkType(int64_t(1))};
        result.seg_offsets_ = {0};
        result.distances_ = {1.0f};
        result.topk_per_nq_prefix_sum_ = {0, 1};
    }

    CStatus
    Export(int entry, ArrowSchema* out_schema, ArrowArray* out_array) {
        int32_t index = 0;
        int64_t offset = 0;
        if (entry == 0) {
            CSegmentInterface c_segment = segment.get();
            return FillRetrieveFieldsOrdered(&c_segment,
                                             1,
                                             retrieve_plan.get(),
                                             &index,
                                             &offset,
                                             1,
                                             out_schema,
                                             out_array,
                                             nullptr);
        }
        CSearchResult c_result = &result;
        if (entry == 1) {
            int64_t* chunks = nullptr;
            int64_t num_chunks = 0;
            auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
                c_result,
                search_plan.get(),
                input_plan.data(),
                input_plan.size(),
                out_schema,
                out_array,
                &chunks,
                &num_chunks,
                nullptr);
            free(chunks);
            return status;
        }
        return FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
            &c_result,
            1,
            search_plan.get(),
            input_plan.data(),
            input_plan.size(),
            &index,
            &offset,
            1,
            out_schema,
            out_array,
            nullptr);
    }

    SchemaPtr schema;
    FieldId field{0};
    std::unique_ptr<FaultingSegment> segment;
    std::unique_ptr<query::Plan> search_plan;
    std::unique_ptr<query::RetrievePlan> retrieve_plan;
    std::string input_plan;
    SearchResult result;
};

TEST(ArrowExportError, BuilderAllocationFailureKeepsResourceCode) {
    OutOfMemoryPool pool;
    arrow::Int64Builder builder(&pool);
    auto status = builder.Append(1);
    ASSERT_TRUE(status.IsOutOfMemory());
    CheckFailure(ArrowExportFailure(status), MemAllocateFailed);
}

TEST(ArrowExportError, BinaryOffsetOverflowIsPermanentRangeFailure) {
    arrow::BinaryBuilder builder;
    auto status = builder.ReserveData(
        static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1);
    ASSERT_TRUE(status.IsCapacityError());
    CheckFailure(ArrowExportFailure(status), OutOfRange);
}

TEST(ArrowExportError, UnsupportedPayloadKeepsCapabilityCode) {
    DataArray data;
    data.mutable_vectors()->mutable_vector_array()->set_element_type(
        proto::schema::DataType::Double);
    auto result = FieldDataToArrow("value", data, 0);
    ASSERT_TRUE(result.status().IsNotImplemented());
    CheckFailure(ArrowExportFailure(result.status()), NotImplemented);
}

TEST(ArrowExportNativeList, ScalarProtoArrowTakeRoundTrip) {
    FieldMeta meta(FieldName("nested"),
                   FieldId(101),
                   DataType::ARRAY,
                   DataType::INT64,
                   true,
                   true,
                   std::nullopt);
    DataArray data;
    data.set_type(proto::schema::DataType::Array);
    auto* arrays = data.mutable_scalars()->mutable_array_data();
    arrays->set_element_type(proto::schema::Int64);
    auto* row = arrays->add_data();
    row->mutable_long_data()->add_data(9);
    row->mutable_long_data()->add_data(0);
    row->add_valid_data(true);
    row->add_valid_data(false);
    auto converted = FieldDataToArrow("nested", data, 1, false, nullptr, &meta);
    ASSERT_TRUE(converted.ok()) << converted.status().ToString();
    ASSERT_TRUE(converted->second->type()->Equals(GetArrowDataType(meta)));
    auto list = std::dynamic_pointer_cast<arrow::ListArray>(converted->second);
    ASSERT_NE(list, nullptr);
    auto rows =
        ArrowListToScalarFieldProto(*list, meta.get_array_type_schema());
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(rows[0].SerializeAsString(), row->SerializeAsString());
    auto taken =
        ArrowTakeAccessor::ArrowToDataArray(converted->second, meta, {0}, 1);
    ASSERT_NE(taken, nullptr);
    EXPECT_EQ(taken->scalars().array_data().data(0).SerializeAsString(),
              row->SerializeAsString());
}

TEST(ArrowExportNativeList, NestedNullableListRoundTrip) {
    proto::schema::TypeSchema type;
    type.set_nullable(true);
    auto* nested = type.mutable_array_element();
    nested->set_nullable(true);
    nested->mutable_array_element()->set_leaf_type(proto::schema::Int64);
    nested->mutable_array_element()->set_nullable(true);
    FieldMeta meta(FieldName("nested"),
                   FieldId(103),
                   DataType::ARRAY,
                   DataType::ARRAY,
                   true,
                   true,
                   std::nullopt,
                   "",
                   "raw",
                   type);
    DataArray data;
    data.set_type(proto::schema::DataType::Array);
    auto* arrays = data.mutable_scalars()->mutable_array_data();
    arrays->set_element_type(proto::schema::Array);
    auto* row = arrays->add_data();
    auto* children = row->mutable_array_data();
    children->set_element_type(proto::schema::Int64);
    auto* first = children->add_data();
    first->mutable_long_data()->add_data(5);
    first->mutable_long_data()->add_data(0);
    first->add_valid_data(true);
    first->add_valid_data(false);
    children->add_data()->mutable_long_data();
    row->add_valid_data(true);
    row->add_valid_data(false);
    auto converted = FieldDataToArrow("nested", data, 1, false, nullptr, &meta);
    ASSERT_TRUE(converted.ok()) << converted.status().ToString();
    auto list = std::dynamic_pointer_cast<arrow::ListArray>(converted->second);
    ASSERT_NE(list, nullptr);
    auto inner = std::dynamic_pointer_cast<arrow::ListArray>(list->values());
    ASSERT_NE(inner, nullptr);
    EXPECT_TRUE(inner->IsNull(1));
    EXPECT_EQ(inner->value_length(1), 0);
    auto output = arrow::io::BufferOutputStream::Create();
    ASSERT_TRUE(output.ok()) << output.status().ToString();
    auto table = arrow::Table::Make(
        arrow::schema(
            {arrow::field("nested", converted->second->type(), true)}),
        {converted->second});
    auto parquet_status = parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), *output, 1);
    EXPECT_TRUE(parquet_status.ok()) << parquet_status.ToString();
    auto taken =
        ArrowTakeAccessor::ArrowToDataArray(converted->second, meta, {0}, 1);
    ASSERT_NE(taken, nullptr);
    EXPECT_EQ(taken->scalars().array_data().data(0).SerializeAsString(),
              row->SerializeAsString());
}

TEST(ArrowExportNativeList, VectorProtoArrowTakeRoundTrip) {
    FieldMeta meta(FieldName("embeddings"),
                   FieldId(102),
                   DataType::VECTOR_ARRAY,
                   DataType::VECTOR_FLOAT,
                   2,
                   std::nullopt,
                   true,
                   true);
    DataArray data;
    data.set_type(proto::schema::DataType::ArrayOfVector);
    auto* vectors = data.mutable_vectors();
    vectors->set_dim(2);
    auto* array = vectors->mutable_vector_array();
    array->set_element_type(proto::schema::FloatVector);
    auto* row = array->add_data();
    row->set_dim(2);
    row->add_valid_data(false);
    row->add_valid_data(true);
    row->mutable_float_vector()->add_data(1.0F);
    row->mutable_float_vector()->add_data(2.0F);
    auto converted =
        FieldDataToArrow("embeddings", data, 1, false, nullptr, &meta);
    ASSERT_TRUE(converted.ok()) << converted.status().ToString();
    ASSERT_TRUE(converted->second->type()->Equals(GetArrowDataType(meta)));
    auto list = std::dynamic_pointer_cast<arrow::ListArray>(converted->second);
    ASSERT_NE(list, nullptr);
    EXPECT_TRUE(list->values()->IsNull(0));
    EXPECT_TRUE(list->values()->IsValid(1));
    auto taken =
        ArrowTakeAccessor::ArrowToDataArray(converted->second, meta, {0}, 1);
    ASSERT_NE(taken, nullptr);
    EXPECT_EQ(taken->vectors().vector_array().data(0).SerializeAsString(),
              row->SerializeAsString());
}

TEST(ArrowExportNativeList, LegacyFixedBinaryVectorTake) {
    FieldMeta meta(FieldName("legacy_embeddings"),
                   FieldId(104),
                   DataType::VECTOR_ARRAY,
                   DataType::VECTOR_FLOAT,
                   2,
                   std::nullopt,
                   false,
                   false);
    arrow::ListBuilder builder(
        arrow::default_memory_pool(),
        std::make_shared<arrow::FixedSizeBinaryBuilder>(
            arrow::fixed_size_binary(2 * sizeof(float))));
    auto* values =
        static_cast<arrow::FixedSizeBinaryBuilder*>(builder.value_builder());
    const float vector[] = {4.0F, 5.0F};
    ASSERT_TRUE(builder.Append().ok());
    ASSERT_TRUE(values->Append(reinterpret_cast<const uint8_t*>(vector)).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto taken = ArrowTakeAccessor::ArrowToDataArray(array, meta, {0}, 1);
    ASSERT_NE(taken, nullptr);
    const auto& row = taken->vectors().vector_array().data(0);
    EXPECT_EQ(row.valid_data_size(), 0);
    ASSERT_EQ(row.float_vector().data_size(), 2);
    EXPECT_FLOAT_EQ(row.float_vector().data(0), 4.0F);
}

TEST(ArrowExportError, InternalContractsAreNotPersistedDataCorruption) {
    auto negative_buffer = arrow::AllocateBuffer(-1);
    ASSERT_TRUE(negative_buffer.status().IsInvalid());
    CheckFailure(ArrowExportFailure(negative_buffer.status()), UnexpectedError);
    for (const auto& status : {arrow::Status::TypeError("internal type"),
                               arrow::Status::IndexError("internal offset"),
                               arrow::Status::SerializationError("protobuf"),
                               arrow::Status::UnknownError("internal")}) {
        CheckFailure(ArrowExportFailure(status), UnexpectedError);
    }
    CheckFailure(ArrowExportFailure(arrow::Status::Cancelled("cancelled")),
                 FollyCancel);
}

TEST_F(ArrowExportBoundaryTest, SuccessfulExportsRemainOwnedByCaller) {
    for (int entry = 0; entry < 3; ++entry) {
        SCOPED_TRACE(entry);
        ArrowSchema out_schema{};
        ArrowArray out_array{};
        auto cleanup = folly::makeGuard([&] {
            if (out_array.release != nullptr) {
                out_array.release(&out_array);
            }
            if (out_schema.release != nullptr) {
                out_schema.release(&out_schema);
            }
        });
        auto status = Export(entry, &out_schema, &out_array);
        ASSERT_EQ(status.error_code, 0) << status.error_msg;
        ASSERT_NE(out_schema.release, nullptr);
        ASSERT_NE(out_array.release, nullptr);
        auto imported_schema = arrow::ImportSchema(&out_schema);
        ASSERT_TRUE(imported_schema.ok());
        auto batch = arrow::ImportRecordBatch(&out_array, *imported_schema);
        ASSERT_TRUE(batch.ok()) << batch.status().ToString();
        ASSERT_EQ((*batch)->num_rows(), 1);
        auto values = std::dynamic_pointer_cast<arrow::Int64Array>(
            (*batch)->GetColumnByName("value"));
        ASSERT_NE(values, nullptr);
        EXPECT_EQ(values->Value(0), 7);
    }
}

TEST_F(ArrowExportBoundaryTest, MaterializationExceptionsKeepTheirCategory) {
    const std::vector<std::pair<std::function<void()>, ErrorCode>> cases = {
        {[] { throw std::bad_alloc(); }, MemAllocateFailed},
        {[] { throw folly::FutureCancellation(); }, FollyCancel},
        {[] { throw SegcoreError(StorageTransientError, "storage throttle"); },
         StorageTransientError},
        {[] { throw SegcoreError(DataFormatBroken, "corrupt stored data"); },
         DataFormatBroken},
        {[] { throw std::runtime_error("unclassified internal error"); },
         UnexpectedError},
        {[] { throw 42; }, UnexpectedError},
    };
    for (int entry = 0; entry < 3; ++entry) {
        SCOPED_TRACE(entry);
        for (const auto& [fault, expected] : cases) {
            SCOPED_TRACE(expected);
            segment->fault = fault;
            ArrowSchema out_schema{};
            ArrowArray out_array{};
            CheckFailure(Export(entry, &out_schema, &out_array), expected);
            EXPECT_EQ(out_schema.release, nullptr);
            EXPECT_EQ(out_array.release, nullptr);
        }
    }
}

TEST_F(ArrowExportBoundaryTest, PartialOutputsAreReleasedOnException) {
    for (int entry = 0; entry < 3; ++entry) {
        SCOPED_TRACE(entry);
        int released_arrays = 0;
        int released_schemas = 0;
        ArrowSchema out_schema{};
        ArrowArray out_array{};
        // Model the ownership state of a partially completed C Data export
        // followed by an allocation exception. The guard must cover throws as
        // well as non-OK statuses.
        segment->fault = [&] {
            out_schema.private_data = &released_schemas;
            out_schema.release = [](ArrowSchema* schema) {
                ++*static_cast<int*>(schema->private_data);
                schema->release = nullptr;
            };
            out_array.private_data = &released_arrays;
            out_array.release = [](ArrowArray* array) {
                ++*static_cast<int*>(array->private_data);
                array->release = nullptr;
            };
            throw std::bad_alloc();
        };
        CheckFailure(Export(entry, &out_schema, &out_array), MemAllocateFailed);
        EXPECT_EQ(released_arrays, 1);
        EXPECT_EQ(released_schemas, 1);
        EXPECT_EQ(out_schema.release, nullptr);
        EXPECT_EQ(out_array.release, nullptr);
    }
}

TEST_F(ArrowExportBoundaryTest, EmptyRetrieveHonorsCancellation) {
    folly::CancellationSource cancellation;
    cancellation.requestCancellation();
    CSegmentInterface c_segment = segment.get();
    ArrowSchema out_schema{};
    ArrowArray out_array{};
    CheckFailure(FillRetrieveFieldsOrdered(&c_segment,
                                           1,
                                           retrieve_plan.get(),
                                           nullptr,
                                           nullptr,
                                           0,
                                           &out_schema,
                                           &out_array,
                                           &cancellation),
                 FollyCancel);
    EXPECT_EQ(out_schema.release, nullptr);
    EXPECT_EQ(out_array.release, nullptr);
}

}  // namespace
