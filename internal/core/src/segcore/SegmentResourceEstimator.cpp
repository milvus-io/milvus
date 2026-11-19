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

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "index/LoadResource.h"
#include "knowhere/index/index_factory.h"
#include "segcore/memory_planner.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/EntryStreamUtils.h"
#include "storage/FileWriter.h"
#include "storage/ThreadPools.h"

namespace milvus::segcore {
namespace {

using Field = proto::schema::FieldSchema;
using Usage = proto::cgo::CoreResourceUsage;

uint64_t
Add(uint64_t left, uint64_t right) {
    return left > std::numeric_limits<uint64_t>::max() - right
               ? std::numeric_limits<uint64_t>::max()
               : left + right;
}

uint64_t
Multiply(uint64_t value, uint64_t count) {
    return milvus::SaturatingMultiply(value, count);
}

uint64_t
Nonnegative(int64_t value) {
    AssertInfo(value >= 0, "negative field resource metadata: {}", value);
    return static_cast<uint64_t>(value);
}

bool
IsVector(const Field& field) {
    return IsVectorDataType(static_cast<DataType>(field.data_type()));
}

bool
HasOffsets(const Field& field) {
    const auto type = static_cast<DataType>(field.data_type());
    return type == DataType::STRING || type == DataType::VARCHAR ||
           type == DataType::JSON || type == DataType::TEXT ||
           type == DataType::GEOMETRY;
}

bool
SupportsInterimIndex(const Field& field) {
    const auto type = static_cast<DataType>(field.data_type());
    return type == DataType::VECTOR_FLOAT ||
           type == DataType::VECTOR_SPARSE_U32_F32 ||
           type == DataType::VECTOR_FLOAT16 ||
           type == DataType::VECTOR_BFLOAT16;
}

bool
IndexRequiresGpu(const std::string& type, std::string_view adapt_for_cpu) {
    if (type == "GPU_CAGRA" || type == "GPU_CUVS_CAGRA") {
        if (!adapt_for_cpu.empty()) {
            std::string value(adapt_for_cpu);
            std::transform(
                value.begin(), value.end(), value.begin(), [](unsigned char c) {
                    return std::tolower(c);
                });
            if (value == "true" || value == "1" || value == "t") {
                return false;
            }
        }
        return true;
    }
    // Keep this feature-only Knowhere boundary non-throwing.
    static_assert(noexcept(knowhere::IndexFactory::Instance().FeatureCheck(
        type, knowhere::feature::GPU)));
    return knowhere::IndexFactory::Instance().FeatureCheck(
        type, knowhere::feature::GPU);
}

// Pointer capacity is at most twice the number of physical vectors. With
// missing dimension/type metadata retain the one-byte-vector upper estimate.
uint64_t
VectorArrayElementWidth(const Field& field) {
    uint64_t width = 1;
    const auto dim = GetStringFromRepeatedKVs(field.type_params(), "dim");
    if (dim) {
        uint64_t value = 0;
        const auto parsed =
            std::from_chars(dim->data(), dim->data() + dim->size(), value);
        if (parsed.ec == std::errc{} &&
            parsed.ptr == dim->data() + dim->size() && value > 0) {
            switch (static_cast<DataType>(field.element_type())) {
                case DataType::VECTOR_BINARY:
                    width = Add(value, 7) / 8;
                    break;
                case DataType::VECTOR_FLOAT:
                    width = Multiply(value, 4);
                    break;
                case DataType::VECTOR_FLOAT16:
                case DataType::VECTOR_BFLOAT16:
                    width = Multiply(value, 2);
                    break;
                case DataType::VECTOR_INT8:
                    width = value;
                    break;
                default:
                    break;
            }
        }
    }
    return width;
}

uint64_t
VectorArrayPointerBytes(uint64_t bytes, uint64_t width) {
    const auto numerator =
        static_cast<unsigned __int128>(bytes) * (2 * sizeof(const uint8_t*));
    const auto rounded = numerator / width + (numerator % width != 0);
    return static_cast<uint64_t>(std::min<unsigned __int128>(
        rounded, std::numeric_limits<uint64_t>::max()));
}

// A task may exceed the budget, but then runs alone to guarantee progress.
struct RuntimeLimits {
    uint64_t workers;
    uint64_t budget;
    uint64_t field_batch_target;
    uint64_t writer_buffer;
    bool async_load;
    uint64_t read_window;
};

struct SharedScratch {
    uint64_t total{0};
    uint64_t largest_task{0};

    void
    Include(uint64_t bytes, uint64_t task_bytes) {
        total = Add(total, bytes);
        largest_task = std::max(largest_task, std::min(bytes, task_bytes));
    }

    uint64_t
    Peak(const RuntimeLimits& limits, bool budgeted) const {
        auto peak = std::min(total, Multiply(limits.workers, largest_task));
        if (budgeted && limits.budget != 0) {
            peak = std::min(peak, std::max(limits.budget, largest_task));
        }
        return peak;
    }
};

// Only worker-owned buffers go in the shared accumulators. V1 channels and
// index-family allocations survive worker completion and remain private.
struct SegmentEstimate {
    Usage usage;
    SharedScratch read_scratch;
    SharedScratch offset_scratch;
    uint64_t mmap_writer_tasks{0};

    uint64_t
    LoadingMemory(const RuntimeLimits& limits) const {
        auto bytes =
            Add(usage.max_memory_bytes(), read_scratch.Peak(limits, true));
        bytes = Add(bytes, offset_scratch.Peak(limits, false));
        bytes = Add(bytes,
                    Multiply(std::min(limits.workers, mmap_writer_tasks),
                             limits.writer_buffer));
        return bytes;
    }
};

// Includes struct-array children without creating a second schema representation.
std::unordered_map<int64_t, const Field*>
FieldsById(const proto::schema::CollectionSchema& schema) {
    std::unordered_map<int64_t, const Field*> fields;
    size_t count = schema.fields_size();
    for (const auto& group : schema.struct_array_fields()) {
        count += group.fields_size();
    }
    fields.reserve(count);
    auto add = [&](const Field& field) {
        AssertInfo(fields.emplace(field.fieldid(), &field).second,
                   "duplicate field id in resource estimate: {}",
                   field.fieldid());
    };
    for (const auto& field : schema.fields()) {
        add(field);
    }
    for (const auto& group : schema.struct_array_fields()) {
        for (const auto& field : group.fields()) {
            add(field);
        }
    }
    return fields;
}

SegmentEstimate
EstimateSegment(const proto::cgo::SegmentLoadResources& input,
                const proto::cgo::BatchLoadResourceRequest& request,
                const RuntimeLimits& limits) {
    AssertInfo(input.has_schema(), "resource estimate schema is missing");
    const auto fields = FieldsById(input.schema());
    const auto collection_mmap =
        GetBoolFromRepeatedKVs(input.schema().properties(), "mmap.enabled");
    std::unordered_set<int64_t> indexed_fields;
    std::unordered_set<int64_t> skip_fields;
    indexed_fields.reserve(input.indexes_size());
    skip_fields.reserve(input.indexes_size());
    SegmentEstimate result;
    auto& usage = result.usage;

    for (const auto& info : input.indexes()) {
        if (info.index_files().empty() ||
            fields.find(info.field().fieldid()) == fields.end()) {
            continue;
        }
        const auto estimate = index::IndexLoadResource(info);
        const auto& resource = estimate.resource;
        AssertInfo(estimate.shared_memory_bytes <= resource.max_memory_cost,
                   "shared index scratch exceeds index loading estimate");
        usage.set_final_memory_bytes(
            Add(usage.final_memory_bytes(), resource.final_memory_cost));
        usage.set_final_disk_bytes(
            Add(usage.final_disk_bytes(), resource.final_disk_cost));
        usage.set_max_memory_bytes(
            Add(usage.max_memory_bytes(),
                resource.max_memory_cost - estimate.shared_memory_bytes));
        usage.set_max_disk_bytes(
            Add(usage.max_disk_bytes(), resource.max_disk_cost));
        result.read_scratch.Include(estimate.shared_memory_bytes,
                                    estimate.max_shared_task_bytes);
        const auto field_id = info.field().fieldid();
        indexed_fields.insert(field_id);
        const std::string* index_type = nullptr;
        std::string_view metric_type;
        std::string_view adapt_for_cpu;
        // Protobuf map hashing may use different Abseil seeds across shared libraries.
        for (const auto& [key, value] : info.index_params()) {
            if (key == "index_type") {
                index_type = &value;
            } else if (key == "metric_type") {
                metric_type = value;
            } else if (key == "adapt_for_cpu") {
                adapt_for_cpu = value;
            }
        }
        AssertInfo(index_type != nullptr, "index type is missing");
        if (IndexRequiresGpu(*index_type, adapt_for_cpu)) {
            usage.add_field_gpu_memory_bytes(resource.max_memory_cost);
        }
        if (resource.has_raw_data &&
            !request.prefer_field_data_when_index_has_raw_data()) {
            skip_fields.insert(field_id);
        } else if (IsVector(info.field()) && metric_type == "BM25") {
            skip_fields.insert(field_id);
        }
    }

    for (const auto& group : input.fields()) {
        uint64_t bytes = 0;
        uint64_t rows = 0;
        uint64_t max_file_bytes = 0;
        uint64_t max_file_rows = 0;
        for (const auto& log : group.binlogs()) {
            const auto memory = Nonnegative(log.memory_size());
            const auto size =
                memory != 0 ? memory : Nonnegative(log.log_size());
            const auto file_rows = Nonnegative(log.entries_num());
            bytes = Add(bytes, size);
            rows = Add(rows, file_rows);
            max_file_bytes = std::max(max_file_bytes, size);
            max_file_rows = std::max(max_file_rows, file_rows);
        }
        bool has_live_field = false;
        bool all_skipped = true;
        bool all_indexed = true;
        bool has_mmap_setting = false;
        bool mmap_enabled = false;
        bool has_vector = false;
        bool has_timestamp = false;
        bool supports_interim = false;
        bool has_array = false;
        uint64_t offset_bytes_per_row = 0;
        bool has_sparse = false;
        uint64_t vector_array_fields = 0;
        uint64_t vector_array_min_width = std::numeric_limits<uint64_t>::max();
        bool vector_array_nullable = false;
        bool external_double_memory = false;
        auto inspect = [&](int64_t field_id) {
            const auto it = fields.find(field_id);
            if (it == fields.end()) {
                return;
            }
            const auto& field = *it->second;
            has_live_field = true;
            all_skipped = all_skipped && skip_fields.count(field_id) != 0;
            all_indexed = all_indexed && indexed_fields.count(field_id) != 0;
            auto field_mmap =
                GetBoolFromRepeatedKVs(field.type_params(), "mmap.enabled");
            if (!field_mmap.first) {
                field_mmap = collection_mmap;
            }
            has_mmap_setting = has_mmap_setting || field_mmap.first;
            mmap_enabled = mmap_enabled || field_mmap.second;
            has_vector = has_vector || IsVector(field);
            has_timestamp = has_timestamp || field_id == 1;
            supports_interim = supports_interim || SupportsInterimIndex(field);
            has_array = has_array || static_cast<DataType>(field.data_type()) ==
                                         DataType::ARRAY;
            const auto type = static_cast<DataType>(field.data_type());
            const bool sparse = type == DataType::VECTOR_SPARSE_U32_F32;
            has_sparse = has_sparse || sparse;
            if (type == DataType::VECTOR_ARRAY) {
                ++vector_array_fields;
                vector_array_min_width = std::min(
                    vector_array_min_width, VectorArrayElementWidth(field));
                vector_array_nullable =
                    vector_array_nullable || field.element_nullable();
            }
            offset_bytes_per_row +=
                HasOffsets(field)
                    ? sizeof(uint32_t)
                    : (sparse ? sizeof(std::string_view) + sizeof(uint64_t)
                              : 0);
            external_double_memory =
                external_double_memory || type == DataType::STRING ||
                type == DataType::VARCHAR || type == DataType::JSON;
        };
        if (group.child_fields().empty()) {
            inspect(group.fieldid());
        } else {
            for (const auto field_id : group.child_fields()) {
                inspect(field_id);
            }
        }
        if (!has_live_field || all_skipped) {
            continue;
        }
        if (has_timestamp) {
            usage.set_timestamp_memory_bytes(
                Add(usage.timestamp_memory_bytes(), Multiply(rows, 8)));
        }
        if (!all_indexed && supports_interim && !input.external_lazy_load()) {
            usage.set_interim_index_bytes(
                Add(usage.interim_index_bytes(), bytes));
        }
        // Match the loader: one explicit true maps the whole column group.
        // With no explicit setting, any vector selects the vector default.
        const bool mmap = has_mmap_setting
                              ? mmap_enabled
                              : (has_vector ? request.mmap_vector_field()
                                            : request.mmap_scalar_field());
        const bool external = input.external_collection();
        // External sampling retains the previous conservative string/JSON factor.
        const auto resident =
            Multiply(bytes, external && external_double_memory ? 2 : 1);
        if (mmap) {
            usage.set_final_disk_bytes(Add(usage.final_disk_bytes(), bytes));
            usage.set_mmap_field_count(Add(usage.mmap_field_count(), 1));
        } else {
            usage.set_final_memory_bytes(
                Add(usage.final_memory_bytes(), resident));
        }
        if (external && input.external_lazy_load()) {
            continue;
        }
        if (mmap) {
            usage.set_max_disk_bytes(Add(usage.max_disk_bytes(), bytes));
        } else {
            usage.set_max_memory_bytes(Add(usage.max_memory_bytes(), resident));
        }
        if (external || bytes == 0) {
            continue;
        }
        const auto version = input.storage_version();
        if (version != 2 && version != 3) {
            // V1 completed Arrow reads can queue outside the executor/budget.
            usage.set_max_memory_bytes(
                Add(usage.max_memory_bytes(), Multiply(bytes, mmap ? 2 : 1)));
            continue;
        }
        const auto overhead_factor = has_array ? 2 : 1;
        const auto max_cell_bytes = version == 2 ? max_file_bytes : bytes;
        result.read_scratch.Include(
            Multiply(bytes, overhead_factor),
            version == 3 && limits.async_load
                ? Multiply(std::max(limits.read_window, max_cell_bytes),
                           overhead_factor)
                : std::max(limits.field_batch_target,
                           Multiply(max_cell_bytes, overhead_factor)));
        uint64_t chunk_scratch_total = 0;
        uint64_t chunk_scratch_task = 0;
        if (offset_bytes_per_row != 0) {
            // Every cell adds a sentinel; without cell metadata, at most one
            // nonempty cell per row bounds the aggregate offset-vector bytes.
            const auto fallback = Multiply(bytes, has_sparse ? 6 : 1);
            const auto total_offsets =
                rows != 0 ? Multiply(Multiply(rows, 2), offset_bytes_per_row)
                          : fallback;
            const auto max_cell_rows = version == 2 ? max_file_rows : rows;
            const auto max_offsets =
                max_cell_rows != 0
                    ? Multiply(Add(max_cell_rows, 1), offset_bytes_per_row)
                    : fallback;
            chunk_scratch_total = total_offsets;
            chunk_scratch_task = max_offsets;
        }
        if (vector_array_fields != 0) {
            // Pointer bytes follow vector width. Row offsets, logical lengths
            // and their capacities need at most 20 bytes per row including
            // per-cell sentinels. Nullable child offsets in Arrow account for
            // the extra validity scratch with another source-size reservation.
            const auto row_total =
                rows != 0 ? Multiply(Multiply(rows, 20), vector_array_fields)
                          : Multiply(bytes, 5);
            const auto cell_rows = version == 2 ? max_file_rows : rows;
            const auto row_task =
                cell_rows != 0 ? Multiply(Add(Multiply(cell_rows, 16), 4),
                                          vector_array_fields)
                               : Multiply(max_cell_bytes, 5);
            chunk_scratch_total = Add(
                chunk_scratch_total,
                Add(row_total,
                    Add(VectorArrayPointerBytes(bytes, vector_array_min_width),
                        vector_array_nullable ? bytes : 0)));
            chunk_scratch_task =
                Add(chunk_scratch_task,
                    Add(row_task,
                        Add(VectorArrayPointerBytes(max_cell_bytes,
                                                    vector_array_min_width),
                            vector_array_nullable ? max_cell_bytes : 0)));
        }
        result.offset_scratch.Include(chunk_scratch_total, chunk_scratch_task);
        // Small per-Arrow-array descriptors and allocator overhead remain
        // approximate: metadata does not carry the Arrow array count. This is
        // admission accounting, not an exact upper bound on process RSS.
        if (mmap) {
            // Nonempty cells have positive rows and bytes; either bounds their count.
            result.mmap_writer_tasks =
                Add(result.mmap_writer_tasks, rows != 0 ? rows : bytes);
        }
    }
    return result;
}

void
Accumulate(const SegmentEstimate& source, SegmentEstimate& total) {
    const auto& usage = source.usage;
    auto& target = total.usage;
    target.set_final_memory_bytes(
        Add(target.final_memory_bytes(), usage.final_memory_bytes()));
    target.set_final_disk_bytes(
        Add(target.final_disk_bytes(), usage.final_disk_bytes()));
    target.set_max_memory_bytes(
        Add(target.max_memory_bytes(), usage.max_memory_bytes()));
    target.set_max_disk_bytes(
        Add(target.max_disk_bytes(), usage.max_disk_bytes()));
    target.set_timestamp_memory_bytes(
        Add(target.timestamp_memory_bytes(), usage.timestamp_memory_bytes()));
    target.set_interim_index_bytes(
        Add(target.interim_index_bytes(), usage.interim_index_bytes()));
    target.set_mmap_field_count(
        Add(target.mmap_field_count(), usage.mmap_field_count()));
    for (const auto bytes : usage.field_gpu_memory_bytes()) {
        target.add_field_gpu_memory_bytes(bytes);
    }
    total.read_scratch.Include(source.read_scratch.total,
                               source.read_scratch.largest_task);
    total.offset_scratch.Include(source.offset_scratch.total,
                                 source.offset_scratch.largest_task);
    total.mmap_writer_tasks =
        Add(total.mmap_writer_tasks, source.mmap_writer_tasks);
}

}  // namespace

proto::cgo::BatchLoadResourceResponse
EstimateSegmentResources(const proto::cgo::BatchLoadResourceRequest& request) {
    proto::cgo::BatchLoadResourceResponse response;
    if (request.segments().empty()) {
        return response;
    }
    RuntimeLimits limits{};
    if (!request.final_only()) {
        auto& admission = storage::LoadAdmissionController::GetInstance();
        const bool async_load =
            storagev2translator::StorageV2AsyncLoadEnabled();
        const auto slots = admission.CapacitySlots();
        auto workers = async_load
                           ? std::numeric_limits<uint64_t>::max()
                           : static_cast<uint64_t>(std::max<int64_t>(
                                 1, ThreadPools::GetLoadExecutorWorkers()));
        if (slots != 0) {
            workers = std::min<uint64_t>(workers, slots);
        }
        limits = {
            workers,
            admission.CapacityBytes(),
            static_cast<uint64_t>(FieldDataLoadBatchTargetBytes()),
            storage::FileWriter::GetBufferSize(),
            async_load,
            static_cast<uint64_t>(
                storagev2translator::StorageV2AsyncLoadReadWindowSizeBytes())};
    }
    response.mutable_segments()->Reserve(request.segments_size());
    SegmentEstimate total;
    for (const auto& input : request.segments()) {
        auto estimate = EstimateSegment(input, request, limits);
        Accumulate(estimate, total);
        estimate.usage.set_max_memory_bytes(estimate.LoadingMemory(limits));
        *response.add_segments() = std::move(estimate.usage);
    }
    total.usage.set_max_memory_bytes(total.LoadingMemory(limits));
    *response.mutable_total() = std::move(total.usage);
    return response;
}

}  // namespace milvus::segcore
