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

#include <string.h>
#include "common/FastMem.h"
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include "segcore/default_fs.h"

#include "IndexConfigGenerator.h"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/OffsetMapping.h"
#include "common/TypeTraits.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/VectorTrait.h"
#include "common/type_c.h"
#include "fmt/core.h"
#include "folly/FBVector.h"
#include "index/RTreeIndex.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexMarisa.h"
#include "index/VectorMemIndex.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/expected.h"
#include "knowhere/object.h"
#include "knowhere/sparse_utils.h"
#include "knowhere/version.h"
#include "milvus-storage/filesystem/fs.h"
#include "monitor/Monitor.h"
#include "nlohmann/json.hpp"
#include "pb/schema.pb.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/FieldIndexing.h"
#include "storage/ChunkManager.h"
#include "storage/FileManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/ThreadPool.h"

namespace milvus::segcore {
using std::unique_ptr;

std::function<void(VectorFieldIndexing::GrowingBuildPhase)>
    VectorFieldIndexing::growing_build_test_hook_ = nullptr;

namespace {

void
CallBuildHook(VectorFieldIndexing::GrowingBuildPhase phase) {
    if (VectorFieldIndexing::growing_build_test_hook_) {
        VectorFieldIndexing::growing_build_test_hook_(phase);
    }
}

double
ElapsedMs(const std::chrono::steady_clock::time_point& since) {
    return std::chrono::duration<double, std::milli>(
               std::chrono::steady_clock::now() - since)
        .count();
}

// Executor for async growing-index first builds. Sized to the knowhere
// build pool ratio so this layer never out-submits it; tasks spend most
// of their life blocked on the knowhere pool, which is why we do not
// reuse ThreadPools::LOW (that would starve segment load). See spec §4.5.
milvus::ThreadPool&
GrowingIndexBuildPool() {
    static milvus::ThreadPool pool(
        SegcoreConfig::default_config().get_growing_index_build_pool_ratio(),
        "growing_index_build");
    // Queue depth is the saturation signal for this layer: a task waiting here
    // is not yet even blocked on the knowhere build pool. Task counters and
    // duration histograms are deliberately left unwired -- first-build and
    // catch-up latency already have dedicated histograms.
    [[maybe_unused]] static const bool metrics_wired = [] {
        pool.SetMetrics(
            &milvus::monitor::internal_core_growing_index_pool_capacity,
            &milvus::monitor::internal_core_growing_index_pool_active_threads,
            &milvus::monitor::internal_core_growing_index_pool_idle_threads,
            &milvus::monitor::internal_core_growing_index_pool_queue_depth,
            /*submitted=*/nullptr,
            /*completed=*/nullptr,
            /*queue_duration=*/nullptr,
            /*execute_duration=*/nullptr);
        return true;
    }();
    return pool;
}

}  // namespace

void
IndexingRecord::AppendingIndex(int64_t reserved_offset,
                               int64_t size,
                               FieldId fieldId,
                               const DataArray* stream_data,
                               const InsertRecord<false>& record,
                               const FieldMeta& field_meta) {
    // Check if field has indexing created
    auto it = field_indexings_.find(fieldId);
    if (it == field_indexings_.end()) {
        return;
    }

    FieldIndexing* indexing_ptr = it->second.get();
    auto type = indexing_ptr->get_data_type();
    auto field_raw_data = record.get_data_base(fieldId);

    int64_t valid_count = reserved_offset + size;
    if (field_meta.is_nullable() && field_raw_data->is_mapping_storage()) {
        valid_count = field_raw_data->get_valid_count();
    }

    if (type == DataType::VECTOR_FLOAT &&
        valid_count >= indexing_ptr->get_build_threshold()) {
        indexing_ptr->AppendSegmentIndexDense(
            reserved_offset,
            size,
            field_raw_data,
            stream_data->vectors().float_vector().data().data());
    } else if (type == DataType::VECTOR_FLOAT16 &&
               valid_count >= indexing_ptr->get_build_threshold()) {
        indexing_ptr->AppendSegmentIndexDense(
            reserved_offset,
            size,
            field_raw_data,
            stream_data->vectors().float16_vector().data());
    } else if (type == DataType::VECTOR_BFLOAT16 &&
               valid_count >= indexing_ptr->get_build_threshold()) {
        indexing_ptr->AppendSegmentIndexDense(
            reserved_offset,
            size,
            field_raw_data,
            stream_data->vectors().bfloat16_vector().data());
    } else if (type == DataType::VECTOR_SPARSE_U32_F32 &&
               valid_count >= indexing_ptr->get_build_threshold()) {
        auto data = SparseBytesToRows(
            stream_data->vectors().sparse_float_vector().contents());
        indexing_ptr->AppendSegmentIndexSparse(
            reserved_offset,
            size,
            stream_data->vectors().sparse_float_vector().dim(),
            field_raw_data,
            data.get());
    } else if (type == DataType::GEOMETRY) {
        // For geometry fields, append data incrementally to RTree index
        indexing_ptr->AppendSegmentIndex(
            reserved_offset, size, field_raw_data, stream_data);
    }
}

// concurrent, reentrant
void
IndexingRecord::AppendingIndex(int64_t reserved_offset,
                               int64_t size,
                               FieldId fieldId,
                               const FieldDataPtr data,
                               const InsertRecord<false>& record,
                               const FieldMeta& field_meta) {
    // Check if field has indexing created
    auto it = field_indexings_.find(fieldId);
    if (it == field_indexings_.end()) {
        return;
    }

    FieldIndexing* indexing_ptr = it->second.get();
    auto type = indexing_ptr->get_data_type();
    const void* p = data->Data();
    auto vec_base = record.get_data_base(fieldId);

    int64_t valid_count = reserved_offset + size;
    if (field_meta.is_nullable() && vec_base->is_mapping_storage()) {
        valid_count = vec_base->get_valid_count();
    }

    if ((type == DataType::VECTOR_FLOAT || type == DataType::VECTOR_FLOAT16 ||
         type == DataType::VECTOR_BFLOAT16) &&
        valid_count >= indexing_ptr->get_build_threshold()) {
        indexing_ptr->AppendSegmentIndexDense(
            reserved_offset, size, vec_base, p);
    } else if (type == DataType::VECTOR_SPARSE_U32_F32 &&
               valid_count >= indexing_ptr->get_build_threshold()) {
        indexing_ptr->AppendSegmentIndexSparse(
            reserved_offset,
            size,
            std::dynamic_pointer_cast<const FieldData<SparseFloatVector>>(data)
                ->Dim(),
            vec_base,
            p);
    } else if (type == DataType::GEOMETRY) {
        // For geometry fields, append data incrementally to RTree index
        indexing_ptr->AppendSegmentIndex(reserved_offset, size, vec_base, data);
    }
}

VectorFieldIndexing::VectorFieldIndexing(const FieldMeta& field_meta,
                                         const FieldIndexMeta& field_index_meta,
                                         int64_t segment_max_row_count,
                                         const SegcoreConfig& segcore_config,
                                         const VectorBase* field_raw_data)
    : FieldIndexing(field_meta, segcore_config),
      built_(false),
      sync_with_index_(false),
      // Snapshot the async switch exactly once, at construction: this is
      // what makes "a hot toggle only affects growing segments created
      // afterwards" true, and it keeps the per-dispatch read race-free
      // regardless of concurrent config-watcher stores to the atomic source.
      async_build_enabled_(
          segcore_config.get_enable_async_growing_index_build()),
      async_finalize_budget_ms_(
          segcore_config.get_async_growing_index_finalize_budget_ms()),
      async_catchup_deadline_ms_(
          segcore_config.get_async_growing_index_catchup_deadline_ms()),
      config_(std::make_unique<VecIndexConfig>(
          segment_max_row_count,
          field_index_meta,
          segcore_config,
          SegmentType::Growing,
          IsSparseFloatVectorDataType(field_meta.get_data_type()))) {
    recreate_index(field_meta.get_data_type(), field_raw_data);
}

VectorFieldIndexing::~VectorFieldIndexing() {
    // Lifetime precondition: the caller guarantees no Insert (and thus no
    // AppendSegmentIndex*) is concurrently in flight for this field while the
    // segment is being destroyed -- that is why build_ctrl_ (assigned only
    // under append_mutex_ in StartBuildLocked) may be read here without the
    // lock. Every other member access in this class rests on the same
    // implicit contract; SegmentGrowingImpl provides it by fencing reads and
    // writes off through its ptrLock before release.
    //
    // Queued task: abandon it, no wait -- once scheduled it exits after
    // reading only the shared control block (the FinishGuard and TryStart both
    // touch ctrl only, never `this`). Running task: join it; worst case one
    // full first-build (knowhere Build is not interruptible; cancelled is
    // honored at phase boundaries only). Safe to join: IndexingRecord
    // (SegmentGrowingImpl.h) is declared after InsertRecord, so we are
    // destroyed first and field_raw_data outlives this join.
    if (build_ctrl_ == nullptr) {
        return;
    }
    build_ctrl_->cancelled.store(true);
    if (build_ctrl_->TryAbandon()) {
        return;
    }
    // Lost the race: the task body is running (or already finished) and may
    // still dereference `this`. Join on the control block, NOT on build_task_:
    // Submit can throw *after* enqueueing the task, leaving build_task_
    // invalid while the task runs -- waiting on build_task_ would then return
    // immediately and free the object under the running task.
    // Invariant at this point: phase is kRunning, never kAbandoned. The only
    // two call sites that can win the TryAbandon() CAS are this destructor
    // (which returns immediately above, never reaching here) and the
    // Submit-catch block in AppendBuild (which resets build_ctrl_ to nullptr
    // on a win, so the destructor takes the early `build_ctrl_ == nullptr`
    // return instead of getting here). TryAbandon() above already lost, so
    // the running task owns the state machine and phase can only be
    // kRunning.
    AssertInfo(build_ctrl_->phase.load() != BuildTaskCtrl::Phase::kAbandoned,
               "abandoned build ctrl must have been cleared before the "
               "destructor waits");
    build_ctrl_->finished.wait();
}

void
VectorFieldIndexing::recreate_index(DataType data_type,
                                    const VectorBase* field_raw_data) {
    if (IsSparseFloatVectorDataType(data_type)) {
        index_ = std::make_unique<index::VectorMemIndex<sparse_u32_f32>>(
            DataType::NONE,
            config_->GetIndexType(),
            config_->GetMetricType(),
            knowhere::Version::GetCurrentVersion().VersionNumber());
    } else if (data_type == DataType::VECTOR_FLOAT) {
        auto concurrent_fp32_vec =
            reinterpret_cast<const ConcurrentVector<FloatVector>*>(
                field_raw_data);
        AssertInfo(concurrent_fp32_vec != nullptr,
                   "Fail to generate a cocurrent vector when recreate_index in "
                   "growing segment.");
        knowhere::ViewDataOp view_data = [field_raw_data_ptr =
                                              concurrent_fp32_vec](size_t id) {
            return (const void*)field_raw_data_ptr->get_physical_element(id);
        };
        index_ = std::make_unique<index::VectorMemIndex<float>>(
            DataType::NONE,
            config_->GetIndexType(),
            config_->GetMetricType(),
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            view_data);
    } else if (data_type == DataType::VECTOR_FLOAT16) {
        auto concurrent_fp16_vec =
            reinterpret_cast<const ConcurrentVector<Float16Vector>*>(
                field_raw_data);
        AssertInfo(concurrent_fp16_vec != nullptr,
                   "Fail to generate a cocurrent vector when    recreate_index "
                   "in growing segment.");
        knowhere::ViewDataOp view_data = [field_raw_data_ptr =
                                              concurrent_fp16_vec](size_t id) {
            return (const void*)field_raw_data_ptr->get_physical_element(id);
        };
        index_ = std::make_unique<index::VectorMemIndex<float16>>(
            DataType::NONE,
            config_->GetIndexType(),
            config_->GetMetricType(),
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            view_data);
    } else if (data_type == DataType::VECTOR_BFLOAT16) {
        auto concurrent_bf16_vec =
            reinterpret_cast<const ConcurrentVector<BFloat16Vector>*>(
                field_raw_data);
        AssertInfo(concurrent_bf16_vec != nullptr,
                   "Fail to generate a cocurrent vector when    recreate_index "
                   "in growing segment.");
        knowhere::ViewDataOp view_data = [field_raw_data_ptr =
                                              concurrent_bf16_vec](size_t id) {
            return (const void*)field_raw_data_ptr->get_physical_element(id);
        };
        index_ = std::make_unique<index::VectorMemIndex<bfloat16>>(
            DataType::NONE,
            config_->GetIndexType(),
            config_->GetMetricType(),
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            view_data);
    }
}

// for sparse float vector:
//   * element_size is not used
//   * output_raw pooints at a milvus::schema::proto::SparseFloatArray.
void
VectorFieldIndexing::GetDataFromIndex(const int64_t* seg_offsets,
                                      int64_t count,
                                      int64_t element_size,
                                      void* output) {
    auto ids_ds = std::make_shared<knowhere::DataSet>();
    ids_ds->SetRows(count);
    ids_ds->SetDim(1);
    ids_ds->SetIds(seg_offsets);
    ids_ds->SetIsOwner(false);
    if (IsSparseFloatVectorDataType(get_data_type())) {
        auto vector = index_->GetSparseVector(ids_ds);
        SparseRowsToProto(
            [vec_ptr = vector.get()](size_t i) { return vec_ptr + i; },
            count,
            reinterpret_cast<milvus::proto::schema::SparseFloatArray*>(output));
    } else {
        auto vector = index_->GetVector(ids_ds);
        milvus::fastmem::FastMemcpy(
            output, vector.data(), count * element_size);
    }
}

const void*
VectorFieldIndexing::CopyDenseRows(const VectorBase* vec,
                                   int64_t from,
                                   int64_t to,
                                   size_t vec_length,
                                   std::unique_ptr<char[]>& staging) const {
    auto size_per_chunk = vec->get_size_per_chunk();
    int64_t start_chunk = from / size_per_chunk;
    int64_t end_chunk = (to - 1) / size_per_chunk;
    if (start_chunk == end_chunk) {
        auto chunk_data =
            static_cast<const char*>(vec->get_chunk_data(start_chunk));
        return chunk_data + (from - start_chunk * size_per_chunk) * vec_length;
    }
    staging = std::make_unique<char[]>((to - from) * vec_length);
    int64_t copied = 0;
    for (int64_t chunk_id = start_chunk; chunk_id <= end_chunk; ++chunk_id) {
        auto chunk_data =
            static_cast<const char*>(vec->get_chunk_data(chunk_id));
        int64_t copy_start = std::max(from, chunk_id * size_per_chunk);
        int64_t copy_end = std::min(to, (chunk_id + 1) * size_per_chunk);
        int64_t copy_count = copy_end - copy_start;
        auto src =
            chunk_data + (copy_start - chunk_id * size_per_chunk) * vec_length;
        milvus::fastmem::FastMemcpy(
            staging.get() + copied * vec_length, src, copy_count * vec_length);
        copied += copy_count;
    }
    return staging.get();
}

const void*
VectorFieldIndexing::CopySparseRows(
    const VectorBase* vec,
    int64_t from,
    int64_t to,
    std::vector<knowhere::sparse::SparseRow<SparseValueType>>& staging) const {
    using value_type = knowhere::sparse::SparseRow<SparseValueType>;
    auto size_per_chunk = vec->get_size_per_chunk();
    int64_t start_chunk = from / size_per_chunk;
    int64_t end_chunk = (to - 1) / size_per_chunk;
    if (start_chunk == end_chunk) {
        auto chunk_data =
            static_cast<const value_type*>(vec->get_chunk_data(start_chunk));
        return chunk_data + (from - start_chunk * size_per_chunk);
    }
    staging.resize(to - from);
    int64_t copied = 0;
    for (int64_t chunk_id = start_chunk; chunk_id <= end_chunk; ++chunk_id) {
        int64_t copy_start = std::max(from, chunk_id * size_per_chunk);
        int64_t copy_end = std::min(to, (chunk_id + 1) * size_per_chunk);
        int64_t copy_count = copy_end - copy_start;
        // For mapping storage, chunk data is already compactly stored,
        // so we can copy directly from chunk
        auto chunk_data =
            static_cast<const value_type*>(vec->get_chunk_data(chunk_id));
        int64_t chunk_offset = copy_start - chunk_id * size_per_chunk;
        for (int64_t i = 0; i < copy_count; ++i) {
            staging[copied + i] = chunk_data[chunk_offset + i];
        }
        copied += copy_count;
    }
    return staging.data();
}

void
VectorFieldIndexing::BuildFirstIndexDense(const VectorBase* field_raw_data) {
    auto dim = get_dim();
    auto conf = get_build_params(get_data_type());
    auto build_threshold = get_build_threshold();
    bool is_mapping_storage = field_raw_data->is_mapping_storage();
    auto valid_data = field_raw_data->get_valid_data();

    size_t vec_length;
    if (get_data_type() == DataType::VECTOR_FLOAT) {
        vec_length = dim * sizeof(float);
    } else if (get_data_type() == DataType::VECTOR_FLOAT16) {
        vec_length = dim * sizeof(float16);
    } else {
        vec_length = dim * sizeof(bfloat16);
    }

    std::unique_ptr<char[]> staging;
    // Chunk data stores valid vectors compactly for both nullable and non-nullable
    const void* data_ptr =
        CopyDenseRows(field_raw_data, 0, build_threshold, vec_length, staging);

    auto dataset = knowhere::GenDataSet(build_threshold, dim, data_ptr);
    index_->BuildWithDataset(dataset, conf);
    if (is_mapping_storage) {
        auto logical_offset =
            field_raw_data->get_logical_offset(build_threshold - 1);
        auto update_count = logical_offset + 1;
        index_->UpdateValidData(valid_data.data(), update_count);
    }
    built_ = true;
    index_cur_.fetch_add(build_threshold);
}

void
VectorFieldIndexing::BuildFirstIndexSparse(const VectorBase* field_raw_data,
                                           int64_t new_data_dim) {
    using value_type = knowhere::sparse::SparseRow<SparseValueType>;
    auto conf = get_build_params(get_data_type());
    auto dim = new_data_dim;
    auto build_threshold = get_build_threshold();
    bool is_mapping_storage = field_raw_data->is_mapping_storage();
    auto valid_data = field_raw_data->get_valid_data();

    std::vector<value_type> staging;
    const void* data_ptr =
        CopySparseRows(field_raw_data, 0, build_threshold, staging);

    auto dataset = knowhere::GenDataSet(build_threshold, dim, data_ptr);
    dataset->SetIsSparse(true);
    index_->BuildWithDataset(dataset, conf);
    if (is_mapping_storage) {
        auto logical_offset =
            field_raw_data->get_logical_offset(build_threshold - 1);
        auto update_count = logical_offset + 1;
        index_->UpdateValidData(valid_data.data(), update_count);
    }
    built_ = true;
    index_cur_.fetch_add(build_threshold);
}

void
VectorFieldIndexing::AddBatchDense(int64_t reserved_offset,
                                   int64_t size,
                                   const VectorBase* field_raw_data,
                                   const void* data_source) {
    auto dim = get_dim();
    auto conf = get_build_params(get_data_type());
    auto valid_data = field_raw_data->get_valid_data();

    size_t vec_length;
    if (get_data_type() == DataType::VECTOR_FLOAT) {
        vec_length = dim * sizeof(float);
    } else if (get_data_type() == DataType::VECTOR_FLOAT16) {
        vec_length = dim * sizeof(float16);
    } else {
        vec_length = dim * sizeof(bfloat16);
    }

    //append rest data when index has built
    int64_t add_count = 0;
    int64_t total_count = 0;
    if (valid_data.empty()) {
        add_count = reserved_offset + size - index_cur_.load();
        total_count = size;
        if (add_count <= 0) {
            sync_with_index_.store(true);
            return;
        }
        auto data_ptr = static_cast<const char*>(data_source) +
                        (total_count - add_count) * vec_length;
        auto dataset = knowhere::GenDataSet(add_count, dim, data_ptr);
        try {
            index_->AddWithDataset(dataset, conf);
            index_cur_.fetch_add(add_count);
            sync_with_index_.store(true);
        } catch (SegcoreError& error) {
            LOG_ERROR("growing index add error: {}", error.what());
            // Known design defect: after a raw-data-owning interim index has
            // synchronized, its source chunks may have been reclaimed and
            // field_raw_data is no longer a complete recovery source. This
            // catch is reachable when AddWithDataset fails, but recreating the
            // index here is not a valid recovery path and must not exist under
            // single-owner semantics. The failure should instead be propagated
            // and the segment marked unrecoverable.
            recreate_index(get_data_type(), field_raw_data);
        }
    } else {
        // Nullable dense vectors: data_source (proto) contains valid vectors compactly
        auto index_total_count = index_->GetOffsetMapping().GetTotalCount();
        auto add_valid_data_count = reserved_offset + size - index_total_count;
        // Count valid vectors in this batch range
        for (auto i = reserved_offset; i < reserved_offset + size; i++) {
            if (valid_data[i]) {
                total_count++;
                if (i >= index_total_count) {
                    add_count++;
                }
            }
        }
        if (add_count <= 0 && add_valid_data_count <= 0) {
            sync_with_index_.store(true);
            return;
        }
        if (add_count > 0) {
            // data_source contains valid vectors compactly, skip already indexed ones
            auto data_ptr = static_cast<const char*>(data_source) +
                            (total_count - add_count) * vec_length;
            auto dataset = knowhere::GenDataSet(add_count, dim, data_ptr);
            try {
                index_->AddWithDataset(dataset, conf);
            } catch (SegcoreError& error) {
                LOG_ERROR("growing index add error: {}", error.what());
                // Known design defect: after a raw-data-owning interim index
                // has synchronized, its source chunks may have been reclaimed
                // and field_raw_data is no longer a complete recovery source.
                // This catch is reachable when AddWithDataset fails, but
                // recreating the index here is not a valid recovery path and
                // must not exist under single-owner semantics. The failure
                // should instead be propagated and the segment marked
                // unrecoverable.
                recreate_index(get_data_type(), field_raw_data);
            }
        }
        if (add_valid_data_count > 0) {
            index_->UpdateValidData(valid_data.data() + index_total_count,
                                    add_valid_data_count);
        }
        index_cur_.fetch_add(add_count);
        sync_with_index_.store(true);
    }
}

void
VectorFieldIndexing::AddBatchSparse(int64_t reserved_offset,
                                    int64_t size,
                                    int64_t new_data_dim,
                                    const VectorBase* field_raw_data,
                                    const void* data_source) {
    using value_type = knowhere::sparse::SparseRow<SparseValueType>;
    auto conf = get_build_params(get_data_type());
    auto source = static_cast<const value_type*>(data_source);
    auto dim = new_data_dim;
    auto valid_data = field_raw_data->get_valid_data();

    // Append rest data when index has been built
    int64_t add_count = 0;
    int64_t total_count = 0;
    if (valid_data.empty()) {
        // Non-nullable case: add all rows
        add_count = reserved_offset + size - index_cur_.load();
        total_count = size;
        if (add_count <= 0) {
            sync_with_index_.store(true);
            return;
        }
        auto data_ptr = source + (total_count - add_count);
        auto dataset = knowhere::GenDataSet(add_count, dim, data_ptr);
        dataset->SetIsSparse(true);
        try {
            index_->AddWithDataset(dataset, conf);
            index_cur_.fetch_add(add_count);
            sync_with_index_.store(true);
        } catch (SegcoreError& error) {
            LOG_ERROR("growing sparse index add error: {}", error.what());
            // Known design defect: after a raw-data-owning interim index has
            // synchronized, its source chunks may have been reclaimed and
            // field_raw_data is no longer a complete recovery source. This
            // catch is reachable when AddWithDataset fails, but recreating the
            // index here is not a valid recovery path and must not exist under
            // single-owner semantics. The failure should instead be propagated
            // and the segment marked unrecoverable.
            recreate_index(get_data_type(), field_raw_data);
        }
    } else {
        // Nullable case: only add valid rows (matching dense vector approach)
        auto index_total_count = index_->GetOffsetMapping().GetTotalCount();
        auto add_valid_data_count = reserved_offset + size - index_total_count;
        for (auto i = reserved_offset; i < reserved_offset + size; i++) {
            if (valid_data[i]) {
                total_count++;
                if (i >= index_total_count) {
                    add_count++;
                }
            }
        }
        if (add_count <= 0 && add_valid_data_count <= 0) {
            sync_with_index_.store(true);
            return;
        }
        if (add_count > 0) {
            auto data_ptr = source + (total_count - add_count);
            auto dataset = knowhere::GenDataSet(add_count, dim, data_ptr);
            dataset->SetIsSparse(true);
            try {
                index_->AddWithDataset(dataset, conf);
            } catch (SegcoreError& error) {
                LOG_ERROR("growing sparse index add error: {}", error.what());
                // Known design defect: after a raw-data-owning interim index
                // has synchronized, its source chunks may have been reclaimed
                // and field_raw_data is no longer a complete recovery source.
                // This catch is reachable when AddWithDataset fails, but
                // recreating the index here is not a valid recovery path and
                // must not exist under single-owner semantics. The failure
                // should instead be propagated and the segment marked
                // unrecoverable.
                recreate_index(get_data_type(), field_raw_data);
            }
        }
        if (add_valid_data_count > 0) {
            index_->UpdateValidData(valid_data.data() + index_total_count,
                                    add_valid_data_count);
        }
        index_cur_.fetch_add(add_count);
        sync_with_index_.store(true);
    }
}

void
VectorFieldIndexing::AppendSegmentIndexSparse(int64_t reserved_offset,
                                              int64_t size,
                                              int64_t new_data_dim,
                                              const VectorBase* field_raw_data,
                                              const void* data_source) {
    AssertInfo(get_data_type() == DataType::VECTOR_SPARSE_U32_F32,
               "Data type of vector field is not VECTOR_SPARSE_U32_F32");
    auto field_source =
        dynamic_cast<const ConcurrentVector<SparseFloatVector>*>(
            field_raw_data);
    AssertInfo(field_source,
               "field_raw_data can't cast to "
               "ConcurrentVector<SparseFloatVector> type");

    if (!async_build_enabled_) {
        // Legacy synchronous path, byte-for-byte the pre-change behavior.
        if (!built_) {
            try {
                BuildFirstIndexSparse(field_raw_data, new_data_dim);
            } catch (SegcoreError& error) {
                LOG_ERROR("growing sparse index build error: {}", error.what());
                recreate_index(get_data_type(), field_raw_data);
                return;
            }
        }
        AddBatchSparse(
            reserved_offset, size, new_data_dim, field_raw_data, data_source);
        return;
    }

    // Async path. Reading state_ and acting on it must be one critical
    // section: a lock-free read races the background finalizer and can
    // publish sync_with_index_=true while this batch is missing from the
    // index -- later kSynced batches would then compute a negative source
    // offset in AddBatchSparse (out-of-bounds read). See spec §4.2.
    std::lock_guard<std::mutex> lock(append_mutex_);
    switch (state_.load()) {
        case GrowingIndexState::kNotBuilt:
            StartBuildLocked(
                field_raw_data, reserved_offset + size, new_data_dim);
            break;
        case GrowingIndexState::kBuilding:
            // pending_upto_ is written after this batch's set_data_raw
            // completed (SegmentGrowingImpl::Insert step ordering), so it is
            // an exact raw-data watermark for the catch-up task.
            AdvanceWatermarkLocked(reserved_offset + size);
            break;
        case GrowingIndexState::kSynced:
            AddBatchSparse(reserved_offset,
                           size,
                           new_data_dim,
                           field_raw_data,
                           data_source);
            break;
        case GrowingIndexState::kDisabled:
            break;
    }
}

void
VectorFieldIndexing::AppendSegmentIndexDense(int64_t reserved_offset,
                                             int64_t size,
                                             const VectorBase* field_raw_data,
                                             const void* data_source) {
    AssertInfo(get_data_type() == DataType::VECTOR_FLOAT ||
                   get_data_type() == DataType::VECTOR_FLOAT16 ||
                   get_data_type() == DataType::VECTOR_BFLOAT16,
               "Data type of vector field is not in (VECTOR_FLOAT, "
               "VECTOR_FLOAT16,VECTOR_BFLOAT16)");
    AssertInfo(ConcurrentDenseVectorCheck(field_raw_data, get_data_type()),
               "vec_base can't cast to ConcurrentVector type");
    if (!async_build_enabled_) {
        // Legacy synchronous path, byte-for-byte the pre-change behavior.
        if (!built_) {
            try {
                BuildFirstIndexDense(field_raw_data);
            } catch (SegcoreError& error) {
                LOG_ERROR("growing index build error: {}", error.what());
                recreate_index(get_data_type(), field_raw_data);
                return;
            }
        }
        AddBatchDense(reserved_offset, size, field_raw_data, data_source);
        return;
    }

    // Async path; see the note in AppendSegmentIndexSparse for why state_ must
    // be read and acted on inside append_mutex_.
    std::lock_guard<std::mutex> lock(append_mutex_);
    switch (state_.load()) {
        case GrowingIndexState::kNotBuilt:
            StartBuildLocked(
                field_raw_data, reserved_offset + size, /*new_data_dim=*/0);
            break;
        case GrowingIndexState::kBuilding:
            AdvanceWatermarkLocked(reserved_offset + size);
            break;
        case GrowingIndexState::kSynced:
            AddBatchDense(reserved_offset, size, field_raw_data, data_source);
            break;
        case GrowingIndexState::kDisabled:
            break;
    }
}

void
VectorFieldIndexing::StartBuildLocked(const VectorBase* field_raw_data,
                                      int64_t upto,
                                      int64_t new_data_dim) {
    // state_ must be published before Submit: the task may start running on
    // another thread immediately and drive state_ all the way to kSynced, and
    // a later store of kBuilding here would be a back edge.
    state_.store(GrowingIndexState::kBuilding);
    AdvanceWatermarkLocked(upto);
    build_ctrl_ = std::make_shared<BuildTaskCtrl>();
    try {
        build_task_ = GrowingIndexBuildPool().Submit(
            [this, ctrl = build_ctrl_, field_raw_data, new_data_dim] {
                // Fulfils ctrl->finished on every exit path, so the destructor
                // can join even when Submit threw before assigning
                // build_task_. Must outlive everything below.
                BuildTaskCtrl::FinishGuard done{ctrl.get()};
                // CAS on the control block BEFORE touching `this`: if the
                // destructor abandoned us while queued, `this` is gone and we
                // must return reading nothing but ctrl.
                if (!ctrl->TryStart()) {
                    return;
                }
                BuildAsync(field_raw_data, new_data_dim);
            });
    } catch (std::exception& error) {
        // Submit enqueues the task before its own throwable work, so the task
        // may already be running here -- the abandon CAS decides who owns the
        // state machine.
        if (build_ctrl_->TryAbandon()) {
            // We won: the body will never run (if the task was enqueued at
            // all it will lose TryStart and exit touching only ctrl), so
            // nothing else can observe state_ and nothing needs joining.
            // Degrade exactly like a build failure: brute-force stays correct.
            LOG_WARN(
                "failed to submit async growing index build, disabling interim "
                "index for this field: {}",
                error.what());
            build_ctrl_ = nullptr;
            state_.store(GrowingIndexState::kDisabled);
            milvus::monitor::internal_core_growing_index_build_failures
                .Increment();
            return;
        }
        // We lost: the task is running and owns state_ from here on. Storing
        // kDisabled now would be a back edge that the task overwrites with
        // kSynced. Leave state_ == kBuilding and let the running build drive
        // it to kSynced/kDisabled itself -- the throw came from the pool's own
        // bookkeeping after the task was handed off, so the build is still
        // valid. The only casualty is the pool future we never received, which
        // is why the destructor joins on build_ctrl_->finished.
        LOG_WARN(
            "async growing index build submit reported an error after the task "
            "started, letting the running build drive the state machine: {}",
            error.what());
    }
}

void
VectorFieldIndexing::BuildAsync(const VectorBase* field_raw_data,
                                int64_t new_data_dim) {
    // RAII so the inflight gauge is decremented on every exit path,
    // including handle_failure itself throwing (recreate_index's
    // make_unique hitting bad_alloc, or LOG_WARN throwing) -- a plain
    // Increment/Decrement pair around the try block below would leak the
    // gauge in that case, since the throw propagates straight out of the
    // catch clause it originated in.
    struct InflightBuildGuard {
        InflightBuildGuard() {
            milvus::monitor::internal_core_growing_index_inflight_builds
                .Increment();
        }
        ~InflightBuildGuard() {
            milvus::monitor::internal_core_growing_index_inflight_builds
                .Decrement();
        }
        InflightBuildGuard(const InflightBuildGuard&) = delete;
        InflightBuildGuard&
        operator=(const InflightBuildGuard&) = delete;
    } inflight_guard;

    // Shared by both catch clauses below (a non-std exception must not leak
    // the inflight gauge nor strand the field in kBuilding forever).
    auto handle_failure = [&](const char* reason) {
        if (sync_with_index_.load()) {
            // The index is already published: CatchUp flips sync_with_index_
            // and state_ under append_mutex_, and only post-publish
            // bookkeeping (the metric Observe calls) runs after that.
            // Recreating the index here would swap a live, searchable index
            // out from under concurrent readers and hand them an empty one.
            // Nothing to undo: the build itself succeeded.
            LOG_WARN(
                "async growing index build threw after publishing the index; "
                "the index stays live, only post-publish bookkeeping was "
                "lost: {}",
                reason);
            return;
        }
        std::lock_guard<std::mutex> lock(append_mutex_);
        // Terminal state FIRST, and with nothing throwable between the lock
        // and here: everything below (recreate_index's allocations, the log
        // sinks) can throw, and a throw that escaped before this point would
        // strand the field in kBuilding forever -- no kDisabled, no counter,
        // no terminal log, and every later insert silently no-ops into the
        // kBuilding branch.
        built_ = false;
        index_cur_.store(0);
        state_.store(GrowingIndexState::kDisabled);
        milvus::monitor::internal_core_growing_index_build_failures.Increment();
        try {
            LOG_WARN(
                "async growing index build failed, disabling interim index "
                "for this field, falling back to brute-force scan "
                "permanently: {}",
                reason);
            // Legal here because the index has NOT taken raw-data ownership
            // yet: sync_with_index_ was never set, so HasRawData() stayed
            // false and try_remove_chunks never cleared the source chunks
            // (unlike the post-sync recreate_index documented as a design
            // defect in AddBatch*). Recreate only to free the half-built
            // index memory.
            recreate_index(get_data_type(), field_raw_data);
        } catch (...) {
            // Best effort only. The field is already terminally kDisabled and
            // correctness (brute-force scan over the untouched raw chunks) no
            // longer depends on anything below; the sole casualty is that the
            // half-built index's memory stays held until the segment is
            // dropped. Swallowing beats unwinding out of the pool task and
            // losing the terminal state.
            try {
                LOG_WARN(
                    "failed to release the half-built growing index after a "
                    "build failure; the index is disabled and its memory is "
                    "held until the segment is dropped");
            } catch (...) {
            }
        }
    };

    try {
        CallBuildHook(GrowingBuildPhase::kBeforeBuild);
        if (!IsCancelled()) {
            // Nullable growing vector fields always use mapping storage
            // (InsertRecord::append_data passes the two together), and
            // PhysicalTarget's logical->physical conversion depends on it.
            // get_valid_data() is O(1) in this branch: a non-mapping vector
            // holds no validity bitmap pointer at all.
            AssertInfo(field_raw_data->is_mapping_storage() ||
                           field_raw_data->get_valid_data().empty(),
                       "nullable growing vector without mapping storage is not "
                       "supported by async growing index build");
            auto build_start = std::chrono::steady_clock::now();
            // Phase 1: no lock held -- while state_ == kBuilding insert never
            // touches index_, which is the core invariant of the state machine.
            // Rows [0, get_build_threshold()) are guaranteed already written:
            // AppendingIndex only reaches us once the raw watermark crossed the
            // threshold, and set_data_raw precedes AppendingIndex.
            if (IsSparseFloatVectorDataType(get_data_type())) {
                BuildFirstIndexSparse(field_raw_data, new_data_dim);
            } else {
                BuildFirstIndexDense(field_raw_data);
            }
            milvus::monitor::internal_core_growing_index_build_latency.Observe(
                ElapsedMs(build_start));
            CallBuildHook(GrowingBuildPhase::kAfterBuild);
            CatchUp(field_raw_data);
        }
    } catch (const std::exception& error) {
        handle_failure(error.what());
    } catch (...) {
        handle_failure("unknown exception");
    }
}

void
VectorFieldIndexing::CatchUp(const VectorBase* field_raw_data) {
    auto catchup_start = std::chrono::steady_clock::now();
    int64_t catchup_from = static_cast<int64_t>(index_cur_.load());
    int64_t consumed_rows = 0;
    double consumed_ms = 0.0;

    auto consume_rate = [&]() -> double {
        return consumed_ms > 0.0
                   ? static_cast<double>(consumed_rows) / consumed_ms
                   : 0.0;
    };
    auto estimated_finalize_ms = [&](int64_t gap) -> double {
        if (gap <= 0) {
            return 0.0;
        }
        double rate = consume_rate();
        return rate > 0.0 ? static_cast<double>(gap) / rate
                          : std::numeric_limits<double>::infinity();
    };
    auto throw_deadline = [&](int64_t gap) {
        double elapsed_ms = ElapsedMs(catchup_start);
        double rate = consume_rate();
        double estimate_ms = estimated_finalize_ms(gap);
        LOG_WARN(
            "async growing index catch-up deadline exceeded, discarding the "
            "unpublished index and falling back to raw search permanently: "
            "elapsed_ms={}, deadline_ms={}, gap_rows={}, consumed_rows={}, "
            "consumed_ms={}, consume_rows_per_ms={}, "
            "estimated_finalize_ms={}, finalize_budget_ms={}",
            elapsed_ms,
            async_catchup_deadline_ms_,
            gap,
            consumed_rows,
            consumed_ms,
            rate,
            estimate_ms,
            async_finalize_budget_ms_);
        throw std::runtime_error(fmt::format(
            "async growing index catch-up exceeded deadline: elapsed_ms={}, "
            "deadline_ms={}, gap_rows={}, consume_rows_per_ms={}, "
            "estimated_finalize_ms={}, finalize_budget_ms={}",
            elapsed_ms,
            async_catchup_deadline_ms_,
            gap,
            rate,
            estimate_ms,
            async_finalize_budget_ms_));
    };

    for (;;) {
        if (IsCancelled()) {
            // Segment tearing down: stay kBuilding, never publish.
            return;
        }
        int64_t target = PhysicalTarget(field_raw_data, pending_upto_.load());
        int64_t gap = target - static_cast<int64_t>(index_cur_.load());
        if (gap > 0 && ElapsedMs(catchup_start) >= async_catchup_deadline_ms_ &&
            estimated_finalize_ms(gap) > async_finalize_budget_ms_) {
            throw_deadline(gap);
        }
        if (gap > 0) {
            auto round_start = std::chrono::steady_clock::now();
            int64_t round_from = static_cast<int64_t>(index_cur_.load());
            AddRange(field_raw_data, target, /*interruptible=*/true);
            double round_ms = ElapsedMs(round_start);
            int64_t round_rows =
                static_cast<int64_t>(index_cur_.load()) - round_from;
            if (round_rows > 0) {
                consumed_rows += round_rows;
                consumed_ms += round_ms;
            }
            CallBuildHook(GrowingBuildPhase::kAfterCatchupRound);

            int64_t latest_target =
                PhysicalTarget(field_raw_data, pending_upto_.load());
            int64_t latest_gap =
                latest_target - static_cast<int64_t>(index_cur_.load());
            double rate = consume_rate();
            double estimate_ms = estimated_finalize_ms(latest_gap);
            LOG_INFO(
                "async growing index catch-up round completed: "
                "round_rows={}, round_ms={}, consumed_rows={}, "
                "consumed_ms={}, consume_rows_per_ms={}, latest_gap_rows={}, "
                "estimated_finalize_ms={}, finalize_budget_ms={}, "
                "catchup_elapsed_ms={}, catchup_deadline_ms={}, decision={}",
                round_rows,
                round_ms,
                consumed_rows,
                consumed_ms,
                rate,
                latest_gap,
                estimate_ms,
                async_finalize_budget_ms_,
                ElapsedMs(catchup_start),
                async_catchup_deadline_ms_,
                estimate_ms <= async_finalize_budget_ms_ ? "try_finalize"
                                                         : "continue");

            if (estimate_ms > async_finalize_budget_ms_) {
                if (ElapsedMs(catchup_start) >= async_catchup_deadline_ms_) {
                    throw_deadline(latest_gap);
                }
                continue;
            }
        }

        CallBuildHook(GrowingBuildPhase::kBeforeFinalize);
        auto lock_wait_start = std::chrono::steady_clock::now();
        std::unique_lock<std::mutex> lock(append_mutex_);
        double lock_wait_ms = ElapsedMs(lock_wait_start);
        if (IsCancelled()) {
            return;
        }
        // Under append_mutex_ no insert can advance pending_upto_. Recompute
        // the frozen gap because rows may have slipped in while the background
        // task was competing for the lock. Never turn that race into an
        // unbounded locked finalize.
        int64_t pending = pending_upto_.load();
        int64_t final_target = PhysicalTarget(field_raw_data, pending);
        int64_t frozen_gap =
            final_target - static_cast<int64_t>(index_cur_.load());
        double frozen_estimate_ms = estimated_finalize_ms(frozen_gap);
        if (frozen_estimate_ms > async_finalize_budget_ms_) {
            LOG_INFO(
                "async growing index finalize deferred after locking: "
                "frozen_gap_rows={}, estimated_finalize_ms={}, "
                "finalize_budget_ms={}, consume_rows_per_ms={}, "
                "lock_wait_ms={}, catchup_elapsed_ms={}, decision=continue",
                frozen_gap,
                frozen_estimate_ms,
                async_finalize_budget_ms_,
                consume_rate(),
                lock_wait_ms,
                ElapsedMs(catchup_start));
            lock.unlock();
            if (ElapsedMs(catchup_start) >= async_catchup_deadline_ms_) {
                throw_deadline(frozen_gap);
            }
            continue;
        }

        auto lock_hold_start = std::chrono::steady_clock::now();
        AddRange(field_raw_data, final_target, /*interruptible=*/false);
        // get_valid_data() materializes an O(rows) copy, so it is fetched once
        // here and never inside the catch-up loop.
        auto valid_data = field_raw_data->get_valid_data();
        if (!valid_data.empty()) {
            AssertInfo(static_cast<int64_t>(valid_data.size()) >= pending,
                       "validity bitmap ({} rows) shorter than the raw-data "
                       "watermark ({} rows)",
                       valid_data.size(),
                       pending);
            int64_t index_logical = index_->GetOffsetMapping().GetTotalCount();
            if (pending > index_logical) {
                index_->UpdateValidData(valid_data.data() + index_logical,
                                        pending - index_logical);
            }
        }
        // Atomic with the final AddRange under the same lock: readers that
        // observe sync_with_index_==true are guaranteed the index covers
        // every row whose AppendingIndex call has returned.
        sync_with_index_.store(true);
        state_.store(GrowingIndexState::kSynced);
        double lock_hold_ms = ElapsedMs(lock_hold_start);
        LOG_INFO(
            "async growing index finalized and published: frozen_gap_rows={}, "
            "estimated_finalize_ms={}, finalize_budget_ms={}, "
            "consume_rows_per_ms={}, lock_wait_ms={}, lock_hold_ms={}, "
            "catchup_elapsed_ms={}, decision=publish",
            frozen_gap,
            frozen_estimate_ms,
            async_finalize_budget_ms_,
            consume_rate(),
            lock_wait_ms,
            lock_hold_ms,
            ElapsedMs(catchup_start));
        milvus::monitor::internal_core_growing_index_catchup_latency.Observe(
            ElapsedMs(catchup_start));
        milvus::monitor::internal_core_growing_index_catchup_rows.Observe(
            static_cast<double>(static_cast<int64_t>(index_cur_.load()) -
                                catchup_from));
        return;
    }
}

int64_t
VectorFieldIndexing::PhysicalTarget(const VectorBase* vec,
                                    int64_t logical_upto) const {
    if (!vec->is_mapping_storage()) {
        return logical_upto;
    }
    // get_logical_offset is monotone in the physical offset (rows are appended
    // in order), so the first physical offset whose logical offset reaches
    // logical_upto is exactly the number of valid rows below the watermark.
    int64_t lo = 0;
    int64_t hi = vec->get_valid_count();
    while (lo < hi) {
        int64_t mid = lo + (hi - lo) / 2;
        if (vec->get_logical_offset(mid) >= logical_upto) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return lo;
}

void
VectorFieldIndexing::AddRange(const VectorBase* field_raw_data,
                              int64_t target,
                              bool interruptible) {
    auto conf = get_build_params(get_data_type());
    if (IsSparseFloatVectorDataType(get_data_type())) {
        using value_type = knowhere::sparse::SparseRow<SparseValueType>;
        while (static_cast<int64_t>(index_cur_.load()) < target) {
            if (interruptible && IsCancelled()) {
                return;
            }
            int64_t from = static_cast<int64_t>(index_cur_.load());
            int64_t to = std::min(target, from + kCatchupSparseRows);
            std::vector<value_type> staging;
            auto rows = static_cast<const value_type*>(
                CopySparseRows(field_raw_data, from, to, staging));
            // Dim semantics must match today's Add path, which passes the
            // triggering proto batch's dim (its max index + 1). knowhere only
            // uses it to widen its running max_dim_, so the max over this
            // slice is the value-identical reconstruction for chunk-sourced
            // slices that do not align with batch boundaries.
            int64_t slice_dim = 0;
            for (int64_t i = 0; i < to - from; ++i) {
                slice_dim = std::max(slice_dim, rows[i].dim());
            }
            auto dataset = knowhere::GenDataSet(to - from, slice_dim, rows);
            dataset->SetIsSparse(true);
            index_->AddWithDataset(dataset, conf);
            index_cur_.fetch_add(to - from);
        }
        return;
    }
    size_t vec_length;
    if (get_data_type() == DataType::VECTOR_FLOAT) {
        vec_length = get_dim() * sizeof(float);
    } else if (get_data_type() == DataType::VECTOR_FLOAT16) {
        vec_length = get_dim() * sizeof(float16);
    } else {
        vec_length = get_dim() * sizeof(bfloat16);
    }
    int64_t budget_rows = std::max<int64_t>(
        1, kCatchupStagingBytes / static_cast<int64_t>(vec_length));
    while (static_cast<int64_t>(index_cur_.load()) < target) {
        if (interruptible && IsCancelled()) {
            return;
        }
        int64_t from = static_cast<int64_t>(index_cur_.load());
        int64_t to = std::min(target, from + budget_rows);
        std::unique_ptr<char[]> staging;
        auto data_ptr =
            CopyDenseRows(field_raw_data, from, to, vec_length, staging);
        auto dataset = knowhere::GenDataSet(to - from, get_dim(), data_ptr);
        index_->AddWithDataset(dataset, conf);
        index_cur_.fetch_add(to - from);
    }
}

knowhere::Json
VectorFieldIndexing::get_build_params(DataType data_type) const {
    auto config = config_->GetBuildBaseParams(data_type);
    if (!IsSparseFloatVectorDataType(get_data_type())) {
        config[knowhere::meta::DIM] = std::to_string(get_dim());
    }
    config[knowhere::meta::NUM_BUILD_THREAD] = std::to_string(1);
    // for sparse float vector: drop_ratio_build config is not allowed to be set
    // on growing segment index.
    return config;
}

SearchInfo
VectorFieldIndexing::get_search_params(const SearchInfo& searchInfo) const {
    auto conf = config_->GetSearchConf(searchInfo);
    return conf;
}

bool
VectorFieldIndexing::sync_data_with_index() const {
    return sync_with_index_.load();
}

bool
VectorFieldIndexing::has_raw_data() const {
    return index_->HasRawData();
}

template <typename T>
ScalarFieldIndexing<T>::ScalarFieldIndexing(
    const FieldMeta& field_meta,
    const FieldIndexMeta& field_index_meta,
    int64_t segment_max_row_count,
    const SegcoreConfig& segcore_config,
    const VectorBase* field_raw_data)
    : FieldIndexing(field_meta, segcore_config),
      built_(false),
      sync_with_index_(false),
      config_(std::make_unique<FieldIndexMeta>(field_index_meta)) {
    recreate_index(field_meta, field_raw_data);
}

template <typename T>
void
ScalarFieldIndexing<T>::recreate_index(const FieldMeta& field_meta,
                                       const VectorBase* field_raw_data) {
    if constexpr (std::is_same_v<T, std::string>) {
        if (field_meta.get_data_type() == DataType::GEOMETRY) {
            // Create chunk manager for file operations
            auto chunk_manager =
                milvus::storage::LocalChunkManagerSingleton::GetInstance()
                    .GetChunkManager();
            auto fs = milvus::segcore::GetDefaultArrowFileSystem();

            // Create FieldDataMeta for RTree index
            storage::FieldDataMeta field_data_meta;
            field_data_meta.field_id = field_meta.get_id().get();

            // Create a minimal field schema from FieldMeta
            field_data_meta.field_schema.set_fieldid(field_meta.get_id().get());
            field_data_meta.field_schema.set_name(field_meta.get_name().get());
            field_data_meta.field_schema.set_data_type(
                static_cast<proto::schema::DataType>(
                    field_meta.get_data_type()));
            field_data_meta.field_schema.set_nullable(field_meta.is_nullable());

            // Create IndexMeta for RTree index
            storage::IndexMeta index_meta;
            index_meta.segment_id = 0;
            index_meta.field_id = field_meta.get_id().get();
            index_meta.build_id = 0;
            index_meta.index_version = 1;
            index_meta.key = "rtree_index";
            index_meta.field_name = field_meta.get_name().get();
            index_meta.field_type = field_meta.get_data_type();
            index_meta.index_non_encoding = false;

            // Create FileManagerContext with all required components
            storage::FileManagerContext ctx(
                field_data_meta, index_meta, chunk_manager, fs);

            index_ = std::make_unique<index::RTreeIndex<std::string>>(ctx);
            built_ = false;
            sync_with_index_ = false;
            index_cur_ = 0;
            LOG_INFO(
                "Created R-Tree index for geometry fieldID: {} with "
                "FileManagerContext",
                field_meta.get_id().get());
            return;
        }
        index_ = index::CreateStringIndexMarisa();
    } else {
        index_ = index::CreateScalarIndexSort<T>();
    }

    built_ = false;
    sync_with_index_ = false;
    index_cur_ = 0;

    LOG_INFO("Created scalar index for data type: {}",
             field_meta.get_data_type());
}

template <typename T>
void
ScalarFieldIndexing<T>::AppendSegmentIndex(int64_t reserved_offset,
                                           int64_t size,
                                           const VectorBase* vec_base,
                                           const DataArray* stream_data) {
    // Special handling for geometry fields (stored as std::string)
    if constexpr (std::is_same_v<T, std::string>) {
        if (get_data_type() == DataType::GEOMETRY) {
            // Extract geometry data from stream_data
            if (stream_data->has_scalars() &&
                stream_data->scalars().has_geometry_data()) {
                const auto& geometry_array =
                    stream_data->scalars().geometry_data();
                const auto& valid_data = stream_data->valid_data();

                // Create accessor for DataArray
                auto accessor = [&geometry_array, &valid_data](
                                    int64_t i) -> std::pair<std::string, bool> {
                    bool is_valid = valid_data.empty() || valid_data[i];
                    if (is_valid && i < geometry_array.data_size()) {
                        return {geometry_array.data(i), true};
                    }
                    return {"", false};
                };

                process_geometry_data(
                    reserved_offset, size, vec_base, accessor, "DataArray");
            }
            return;
        }
    }

    // For other scalar fields, not implemented yet
    ThrowInfo(Unsupported,
              "ScalarFieldIndexing::AppendSegmentIndex from DataArray not "
              "implemented for non-geometry scalar fields. Type: {}",
              get_data_type());
}

template <typename T>
void
ScalarFieldIndexing<T>::AppendSegmentIndex(int64_t reserved_offset,
                                           int64_t size,
                                           const VectorBase* vec_base,
                                           const FieldDataPtr& field_data) {
    // Special handling for geometry fields (stored as std::string)
    if constexpr (std::is_same_v<T, std::string>) {
        if (get_data_type() == DataType::GEOMETRY) {
            // Extract geometry data from field_data
            const void* raw_data = field_data->Data();
            if (raw_data) {
                const auto* string_array =
                    static_cast<const std::string*>(raw_data);

                // Create accessor for FieldDataPtr
                auto accessor = [field_data, string_array](
                                    int64_t i) -> std::pair<std::string, bool> {
                    bool is_valid = field_data->is_valid(i);
                    if (is_valid) {
                        return {string_array[i], true};
                    }
                    return {"", false};
                };

                process_geometry_data(
                    reserved_offset, size, vec_base, accessor, "FieldData");
            }
            return;
        }
    }

    // For other scalar fields, not implemented yet
    ThrowInfo(Unsupported,
              "ScalarFieldIndexing::AppendSegmentIndex from FieldDataPtr not "
              "implemented for non-geometry scalar fields. Type: {}",
              get_data_type());
}

template <typename T>
template <typename GeometryDataAccessor>
void
ScalarFieldIndexing<T>::process_geometry_data(int64_t reserved_offset,
                                              int64_t size,
                                              const VectorBase* vec_base,
                                              GeometryDataAccessor&& accessor,
                                              const std::string& log_source) {
    // Special handling for geometry fields (stored as std::string)
    if constexpr (std::is_same_v<T, std::string>) {
        if (get_data_type() == DataType::GEOMETRY) {
            // Cast to R-Tree index for geometry data
            auto* rtree_index =
                dynamic_cast<index::RTreeIndex<std::string>*>(index_.get());
            if (!rtree_index) {
                ThrowInfo(UnexpectedError,
                          "Failed to cast to R-Tree index for geometry field");
            }

            // Initialize R-Tree index on first data arrival (no threshold waiting)
            if (!built_) {
                try {
                    // Initialize R-Tree for building immediately when first data arrives
                    rtree_index->InitForBuildIndex(true);
                    built_ = true;
                    sync_with_index_ = true;
                    LOG_INFO(
                        "Initialized R-Tree index for immediate incremental "
                        "building from {}",
                        log_source);
                } catch (std::exception& error) {
                    ThrowInfo(UnexpectedError,
                              "R-Tree index initialization error: {}",
                              error.what());
                }
            }

            // Always add geometries incrementally (no bulk build phase)
            int64_t added_count = 0;
            for (int64_t i = 0; i < size; ++i) {
                int64_t global_offset = reserved_offset + i;

                // Use the accessor to get geometry data and validity
                auto [wkb_data, is_valid] = accessor(i);

                try {
                    rtree_index->AddGeometry(wkb_data, global_offset);
                    added_count++;
                } catch (std::exception& error) {
                    ThrowInfo(UnexpectedError,
                              "Failed to add geometry at offset {}: {}",
                              global_offset,
                              error.what());
                }
            }

            // Update statistics
            index_cur_.fetch_add(added_count);
            sync_with_index_.store(true);

            LOG_INFO("Added {} geometries to R-Tree index immediately from {}",
                     added_count,
                     log_source);
        }
    }
}

std::unique_ptr<FieldIndexing>
CreateIndex(const FieldMeta& field_meta,
            const FieldIndexMeta& field_index_meta,
            int64_t segment_max_row_count,
            const SegcoreConfig& segcore_config,
            const VectorBase* field_raw_data) {
    if (field_meta.is_vector()) {
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT ||
            field_meta.get_data_type() == DataType::VECTOR_FLOAT16 ||
            field_meta.get_data_type() == DataType::VECTOR_BFLOAT16 ||
            field_meta.get_data_type() == DataType::VECTOR_INT8 ||
            field_meta.get_data_type() == DataType::VECTOR_SPARSE_U32_F32) {
            return std::make_unique<VectorFieldIndexing>(field_meta,
                                                         field_index_meta,
                                                         segment_max_row_count,
                                                         segcore_config,
                                                         field_raw_data);
        } else {
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported vector type in index: {}",
                                  field_meta.get_data_type()));
        }
    }
    switch (field_meta.get_data_type()) {
        case DataType::BOOL:
            return std::make_unique<ScalarFieldIndexing<bool>>(field_meta,
                                                               segcore_config);
        case DataType::INT8:
            return std::make_unique<ScalarFieldIndexing<int8_t>>(
                field_meta, segcore_config);
        case DataType::INT16:
            return std::make_unique<ScalarFieldIndexing<int16_t>>(
                field_meta, segcore_config);
        case DataType::INT32:
            return std::make_unique<ScalarFieldIndexing<int32_t>>(
                field_meta, segcore_config);
        case DataType::INT64:
            return std::make_unique<ScalarFieldIndexing<int64_t>>(
                field_meta, segcore_config);
        case DataType::FLOAT:
            return std::make_unique<ScalarFieldIndexing<float>>(field_meta,
                                                                segcore_config);
        case DataType::DOUBLE:
            return std::make_unique<ScalarFieldIndexing<double>>(
                field_meta, segcore_config);
        case DataType::TIMESTAMPTZ:
            return std::make_unique<ScalarFieldIndexing<int64_t>>(
                field_meta, segcore_config);
        case DataType::VARCHAR:
            return std::make_unique<ScalarFieldIndexing<std::string>>(
                field_meta, segcore_config);
        case DataType::GEOMETRY:
            return std::make_unique<ScalarFieldIndexing<std::string>>(
                field_meta,
                field_index_meta,
                segment_max_row_count,
                segcore_config,
                field_raw_data);
        default:
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported scalar type in index: {}",
                                  field_meta.get_data_type()));
    }
}

// Explicit template instantiation for ScalarFieldIndexing
template class ScalarFieldIndexing<std::string>;

}  // namespace milvus::segcore
