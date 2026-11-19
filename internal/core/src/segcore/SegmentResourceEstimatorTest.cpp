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

#include "segcore/SegmentResourceEstimator.h"

#include <cstdlib>
#include <limits>

#include "gtest/gtest.h"
#include "segcore/memory_planner.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "segcore/segment_resource_c.h"
#include "storage/EntryStreamUtils.h"
#include "storage/ThreadPools.h"

namespace milvus::segcore {
namespace {

class SegmentResourceEstimatorTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        old_async_ = storagev2translator::StorageV2AsyncLoadEnabled();
        old_slots_ =
            storage::LoadAdmissionController::GetInstance().CapacitySlots();
        storagev2translator::SetStorageV2AsyncLoadEnabled(false);
        old_budget_ =
            storage::LoadAdmissionController::GetInstance().CapacityBytes();
        storage::LoadAdmissionController::SetLoadTransientBudgetBytes(1);
    }

    void
    TearDown() override {
        storage::LoadAdmissionController::SetLoadTransientBudgetBytes(
            old_budget_);
        storage::LoadAdmissionController::GetInstance().SetCapacitySlots(
            old_slots_);
        storagev2translator::SetStorageV2AsyncLoadEnabled(old_async_);
    }

    proto::cgo::SegmentLoadResources*
    AddFieldSegment(proto::cgo::BatchLoadResourceRequest& request,
                    proto::schema::DataType type,
                    int64_t bytes,
                    int64_t rows = 10,
                    int64_t storage_version = 3) {
        auto* segment = request.add_segments();
        segment->set_storage_version(storage_version);
        auto* field = segment->mutable_schema()->add_fields();
        field->set_fieldid(100);
        field->set_data_type(type);
        auto* group = segment->add_fields();
        group->set_fieldid(100);
        auto* binlog = group->add_binlogs();
        binlog->set_memory_size(bytes);
        binlog->set_entries_num(rows);
        binlog->set_log_path("/must/not/read/field");
        return segment;
    }

    size_t old_budget_;
    size_t old_slots_;
    bool old_async_;
};

TEST_F(SegmentResourceEstimatorTest, BatchSharesOversizedFieldTasks) {
    proto::cgo::BatchLoadResourceRequest request;
    const auto bytes = 2 * FieldDataLoadBatchTargetBytes();
    for (int i = 0; i < 3; ++i) {
        AddFieldSegment(request, proto::schema::Int64, bytes);
    }
    const auto result = EstimateSegmentResources(request);
    ASSERT_EQ(result.segments_size(), 3);
    for (const auto& segment : result.segments()) {
        EXPECT_EQ(segment.final_memory_bytes(), bytes);
        EXPECT_EQ(segment.max_memory_bytes(), 2 * bytes);
    }
    EXPECT_EQ(result.total().final_memory_bytes(), 3 * bytes);
    // The oversized read runs alone, across all three segments.
    EXPECT_EQ(result.total().max_memory_bytes(), 4 * bytes);
}

TEST_F(SegmentResourceEstimatorTest, LegacyFieldReadsStayPrivate) {
    proto::cgo::BatchLoadResourceRequest request;
    for (int i = 0; i < 3; ++i) {
        AddFieldSegment(request, proto::schema::Int64, 1000, 10, 0);
    }
    const auto result = EstimateSegmentResources(request);
    EXPECT_EQ(result.total().final_memory_bytes(), 3000);
    EXPECT_EQ(result.total().max_memory_bytes(), 6000);
}

TEST_F(SegmentResourceEstimatorTest, ExplicitVectorMmapOverridesDefault) {
    for (const auto enabled : {false, true}) {
        proto::cgo::BatchLoadResourceRequest request;
        request.set_mmap_vector_field(!enabled);
        auto* segment =
            AddFieldSegment(request, proto::schema::FloatVector, 1024);
        auto* setting =
            segment->mutable_schema()->mutable_fields(0)->add_type_params();
        setting->set_key("mmap.enabled");
        setting->set_value(enabled ? "true" : "false");
        const auto usage = EstimateSegmentResources(request).total();
        EXPECT_EQ(usage.final_memory_bytes(), enabled ? 0 : 1024);
        EXPECT_EQ(usage.final_disk_bytes(), enabled ? 1024 : 0);
        EXPECT_EQ(usage.mmap_field_count(), enabled ? 1 : 0);
        EXPECT_GE(usage.max_memory_bytes(), usage.final_memory_bytes());
    }
}

TEST_F(SegmentResourceEstimatorTest, OffsetsAreNotCappedByReadBudget) {
    proto::cgo::BatchLoadResourceRequest request;
    AddFieldSegment(request, proto::schema::VarChar, 1000, 10);
    const auto usage = EstimateSegmentResources(request).total();
    EXPECT_EQ(usage.final_memory_bytes(), 1000);
    // 1000 resident + 1000 Arrow read + at least one offsets vector.
    EXPECT_GE(usage.max_memory_bytes(), 2044);
}

TEST_F(SegmentResourceEstimatorTest,
       MixedColumnGroupUsesExplicitFalseOverDefaults) {
    proto::cgo::BatchLoadResourceRequest request;
    request.set_mmap_vector_field(true);
    request.set_mmap_scalar_field(true);
    auto* segment = AddFieldSegment(request, proto::schema::FloatVector, 1024);
    auto* field = segment->mutable_schema()->add_fields();
    field->set_fieldid(101);
    field->set_data_type(proto::schema::Int64);
    auto* setting = field->add_type_params();
    setting->set_key("mmap.enabled");
    setting->set_value("false");
    auto* group = segment->mutable_fields(0);
    group->set_fieldid(500);
    group->add_child_fields(100);
    group->add_child_fields(101);
    const auto usage = EstimateSegmentResources(request).total();
    EXPECT_EQ(usage.final_memory_bytes(), 1024);
    EXPECT_EQ(usage.final_disk_bytes(), 0);
}

TEST_F(SegmentResourceEstimatorTest, CollectionMmapOverridesDefault) {
    for (const auto enabled : {false, true}) {
        proto::cgo::BatchLoadResourceRequest request;
        request.set_mmap_scalar_field(!enabled);
        auto* segment = AddFieldSegment(request, proto::schema::Int64, 1024);
        auto* setting = segment->mutable_schema()->add_properties();
        setting->set_key("mmap.enabled");
        setting->set_value(enabled ? "true" : "false");
        const auto usage = EstimateSegmentResources(request).total();
        EXPECT_EQ(usage.final_memory_bytes(), enabled ? 0 : 1024);
        EXPECT_EQ(usage.final_disk_bytes(), enabled ? 1024 : 0);
    }
}

TEST_F(SegmentResourceEstimatorTest, MixedColumnGroupUsesAnyExplicitMmap) {
    proto::cgo::BatchLoadResourceRequest request;
    auto* segment = AddFieldSegment(request, proto::schema::FloatVector, 1024);
    auto* vector_setting =
        segment->mutable_schema()->mutable_fields(0)->add_type_params();
    vector_setting->set_key("mmap.enabled");
    vector_setting->set_value("false");
    auto* scalar = segment->mutable_schema()->add_fields();
    scalar->set_fieldid(101);
    scalar->set_data_type(proto::schema::Int64);
    auto* scalar_setting = scalar->add_type_params();
    scalar_setting->set_key("mmap.enabled");
    scalar_setting->set_value("true");
    auto* group = segment->mutable_fields(0);
    group->add_child_fields(100);
    group->add_child_fields(101);
    auto usage = EstimateSegmentResources(request).total();
    EXPECT_EQ(usage.final_memory_bytes(), 0);
    EXPECT_EQ(usage.final_disk_bytes(), 1024);

    segment->mutable_schema()->mutable_fields(0)->clear_type_params();
    scalar->clear_type_params();
    request.set_mmap_vector_field(true);
    request.set_mmap_scalar_field(false);
    usage = EstimateSegmentResources(request).total();
    EXPECT_EQ(usage.final_memory_bytes(), 0);
    EXPECT_EQ(usage.final_disk_bytes(), 1024);
}

TEST_F(SegmentResourceEstimatorTest, SparseWriterScratchSurvivesSmallBudget) {
    proto::cgo::BatchLoadResourceRequest request;
    AddFieldSegment(request, proto::schema::SparseFloatVector, 1000, 10);
    const auto usage = EstimateSegmentResources(request).total();
    EXPECT_GE(usage.max_memory_bytes(), 2000 + 24 * 11);
}

TEST_F(SegmentResourceEstimatorTest, VectorArrayScratchUsesElementWidth) {
    proto::cgo::BatchLoadResourceRequest request;
    auto* segment =
        AddFieldSegment(request, proto::schema::ArrayOfVector, 1000, 10);
    auto* field = segment->mutable_schema()->mutable_fields(0);
    field->set_element_type(proto::schema::BinaryVector);
    auto* dim = field->add_type_params();
    dim->set_key("dim");
    dim->set_value("8");
    const auto narrow =
        EstimateSegmentResources(request).total().max_memory_bytes();
    field->set_element_type(proto::schema::FloatVector);
    dim->set_value("128");
    const auto wide =
        EstimateSegmentResources(request).total().max_memory_bytes();
    EXPECT_GT(narrow, wide);
    EXPECT_GE(wide, 2000 + 164 + 32);
}

TEST_F(SegmentResourceEstimatorTest, FinalOnlyPreservesResidentEstimate) {
    proto::cgo::BatchLoadResourceRequest request;
    AddFieldSegment(request, proto::schema::VarChar, 1000);
    request.set_mmap_vector_field(true);
    AddFieldSegment(request, proto::schema::FloatVector, 2048);
    const auto loading = EstimateSegmentResources(request);
    request.set_final_only(true);
    const auto final = EstimateSegmentResources(request);
    EXPECT_EQ(final.total().final_memory_bytes(),
              loading.total().final_memory_bytes());
    EXPECT_EQ(final.total().final_disk_bytes(),
              loading.total().final_disk_bytes());
    EXPECT_EQ(final.total().timestamp_memory_bytes(),
              loading.total().timestamp_memory_bytes());
    EXPECT_EQ(final.segments_size(), loading.segments_size());
}

TEST_F(SegmentResourceEstimatorTest, RawIndexSkipsFieldUnlessRequested) {
    proto::cgo::BatchLoadResourceRequest request;
    auto* segment = AddFieldSegment(request, proto::schema::Int64, 1000);
    auto* index = segment->add_indexes();
    *index->mutable_field() = segment->schema().fields(0);
    index->add_index_files("/must/not/read/index.v3");
    index->set_index_file_size(4096);
    index->set_num_rows(10);
    index->set_current_scalar_index_version(3);
    (*index->mutable_index_params())["index_type"] = "STL_SORT";
    const auto index_only = EstimateSegmentResources(request).total();
    request.set_prefer_field_data_when_index_has_raw_data(true);
    const auto with_field = EstimateSegmentResources(request).total();
    EXPECT_EQ(with_field.final_memory_bytes(),
              index_only.final_memory_bytes() + 1000);
    EXPECT_GE(with_field.max_memory_bytes(),
              index_only.max_memory_bytes() + 1000);
}

TEST_F(SegmentResourceEstimatorTest, UnlimitedBudgetStillUsesExecutorBound) {
    storage::LoadAdmissionController::SetLoadTransientBudgetBytes(0);
    proto::cgo::BatchLoadResourceRequest request;
    const auto bytes = 2 * FieldDataLoadBatchTargetBytes();
    storage::LoadAdmissionController::GetInstance().SetCapacitySlots(0);
    const auto workers = ThreadPools::GetLoadExecutorWorkers();
    const auto count = workers + 2;
    for (int64_t i = 0; i < count; ++i) {
        AddFieldSegment(request, proto::schema::Int64, bytes);
    }
    const auto usage = EstimateSegmentResources(request).total();
    EXPECT_EQ(usage.final_memory_bytes(), count * bytes);
    EXPECT_EQ(usage.max_memory_bytes(), (count + workers) * bytes);
}

TEST_F(SegmentResourceEstimatorTest, AsyncReadsUseAdmissionSlots) {
    storagev2translator::SetStorageV2AsyncLoadEnabled(true);
    auto& admission = storage::LoadAdmissionController::GetInstance();
    admission.SetCapacityBytes(0);
    proto::cgo::BatchLoadResourceRequest request;
    constexpr uint64_t bytes = 1000;
    for (int i = 0; i < 5; ++i) {
        AddFieldSegment(request, proto::schema::Int64, bytes);
    }
    admission.SetCapacitySlots(2);
    EXPECT_EQ(EstimateSegmentResources(request).total().max_memory_bytes(),
              7 * bytes);
    admission.SetCapacitySlots(0);
    EXPECT_EQ(EstimateSegmentResources(request).total().max_memory_bytes(),
              10 * bytes);
}

TEST_F(SegmentResourceEstimatorTest, DroppedFieldsDoNotReserveResources) {
    proto::cgo::BatchLoadResourceRequest request;
    auto* segment = AddFieldSegment(request, proto::schema::Int64, 1000);
    segment->mutable_fields(0)->set_fieldid(101);
    const auto usage = EstimateSegmentResources(request).total();
    EXPECT_EQ(usage.final_memory_bytes(), 0);
    EXPECT_EQ(usage.max_memory_bytes(), 0);
}

TEST_F(SegmentResourceEstimatorTest, TimestampIsReportedSeparately) {
    proto::cgo::BatchLoadResourceRequest request;
    auto* segment = AddFieldSegment(request, proto::schema::Int64, 80, 10);
    segment->mutable_schema()->mutable_fields(0)->set_fieldid(1);
    segment->mutable_fields(0)->set_fieldid(1);
    const auto usage = EstimateSegmentResources(request).total();
    EXPECT_EQ(usage.final_memory_bytes(), 80);
    EXPECT_EQ(usage.timestamp_memory_bytes(), 80);
}

TEST_F(SegmentResourceEstimatorTest, ExternalLazySkipsOnlyEagerFieldCost) {
    proto::cgo::BatchLoadResourceRequest request;
    auto* segment = AddFieldSegment(request, proto::schema::Int64, 1000);
    segment->set_external_collection(true);
    segment->set_external_lazy_load(true);
    const auto usage = EstimateSegmentResources(request).total();
    EXPECT_EQ(usage.final_memory_bytes(), 1000);
    EXPECT_EQ(usage.max_memory_bytes(), 0);
}

TEST_F(SegmentResourceEstimatorTest, MissingMemorySizeFallsBackToLogSize) {
    proto::cgo::BatchLoadResourceRequest request;
    auto* segment = AddFieldSegment(request, proto::schema::Int64, 0);
    segment->mutable_fields(0)->mutable_binlogs(0)->set_log_size(4096);
    const auto usage = EstimateSegmentResources(request).total();
    EXPECT_EQ(usage.final_memory_bytes(), 4096);
    EXPECT_GE(usage.max_memory_bytes(), 4096);
}

TEST_F(SegmentResourceEstimatorTest,
       NegativeMetadataFailsAndLargeSumsSaturate) {
    proto::cgo::BatchLoadResourceRequest request;
    AddFieldSegment(request, proto::schema::Int64, -1);
    EXPECT_ANY_THROW(EstimateSegmentResources(request));
    request.clear_segments();
    for (int i = 0; i < 3; ++i) {
        AddFieldSegment(
            request, proto::schema::Int64, std::numeric_limits<int64_t>::max());
    }
    const auto usage = EstimateSegmentResources(request).total();
    EXPECT_EQ(usage.final_memory_bytes(), std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(usage.max_memory_bytes(), std::numeric_limits<uint64_t>::max());
}

TEST_F(SegmentResourceEstimatorTest, CBoundaryPreservesVectorIndexParameters) {
    for (const auto* type : {"FLAT", "GPU_CAGRA", "GPU_CUVS_CAGRA"}) {
        SCOPED_TRACE(type);
        proto::cgo::BatchLoadResourceRequest request;
        auto* segment =
            AddFieldSegment(request, proto::schema::FloatVector, 160, 10);
        auto* field = segment->mutable_schema()->mutable_fields(0);
        auto* dim = field->add_type_params();
        dim->set_key("dim");
        dim->set_value("4");
        auto* index = segment->add_indexes();
        *index->mutable_field() = *field;
        index->add_index_files("/must/not/read/vector.index");
        index->set_index_file_size(160);
        index->set_num_rows(10);
        (*index->mutable_index_params())["index_type"] = type;
        (*index->mutable_index_params())["metric_type"] = "L2";
        (*index->mutable_index_params())["adapt_for_cpu"] = "true";
        request.set_prefer_field_data_when_index_has_raw_data(true);

        const auto serialized = request.SerializeAsString();
        auto* output = CreateProtoLayout();
        const auto status = EstimateSegmentResourcesFromSerializedInfo(
            reinterpret_cast<const uint8_t*>(serialized.data()),
            serialized.size(),
            output);
        EXPECT_EQ(status.error_code, 0)
            << (status.error_msg == nullptr ? "" : status.error_msg);
        if (status.error_code == 0) {
            proto::cgo::BatchLoadResourceResponse response;
            EXPECT_TRUE(response.ParseFromArray(
                output->blob, static_cast<int>(output->size)));
            EXPECT_EQ(response.total().field_gpu_memory_bytes_size(), 0);
        }
        if (status.error_code != 0) {
            free(const_cast<char*>(status.error_msg));
        }
        ReleaseProtoLayout(output);
    }
}

TEST_F(SegmentResourceEstimatorTest,
       CBoundaryRejectsInvalidSerializedMetadata) {
    auto* output = CreateProtoLayout();
    const uint8_t invalid[] = {0xff};
    const auto status =
        EstimateSegmentResourcesFromSerializedInfo(invalid, 1, output);
    EXPECT_NE(status.error_code, 0);
    free(const_cast<char*>(status.error_msg));
    ReleaseProtoLayout(output);
}

}  // namespace
}  // namespace milvus::segcore
