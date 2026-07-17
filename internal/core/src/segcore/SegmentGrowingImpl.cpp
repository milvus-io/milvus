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

#include <boost/iterator/counting_iterator.hpp>
#include <folly/ScopeGuard.h>
#include "common/FastMem.h"
#include <cxxabi.h>
#include <algorithm>
#include <cstring>
#include <exception>
#include <future>
#include <iosfwd>
#include <map>
#include <filesystem>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>
#include "segcore/default_fs.h"

#include "NamedType/named_type_impl.hpp"
#include "arrow/api.h"
#include "bitset/bitset.h"
#include "boost/iterator/iterator_facade.hpp"
#include "cachinglayer/CacheSlot.h"
#include "common/Array.h"
#include "common/ArrayOffsets.h"
#include "common/ArrowDataWrapper.h"
#include "common/Channel.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/LoadInfo.h"
#include "common/Schema.h"
#include "common/Span.h"
#include "common/Types.h"
#include "common/VectorArray.h"
#include "glog/logging.h"
#include "index/Index.h"
#include "index/TextMatchIndex.h"
#include "index/Utils.h"
#include "index/VectorIndex.h"
#include "knowhere/comp/index_param.h"
#include "log/Log.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "milvus-storage/manifest.h"
#include "mmap/Types.h"
#include "pb/schema.pb.h"
#include "pb/segcore.pb.h"
#include "query/SearchOnGrowing.h"
#include "segcore/AckResponder.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/DeletedRecord.h"
#include "segcore/FieldIndexing.h"
#include "segcore/InsertRecord.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/Utils.h"
#include "segcore/memory_planner.h"
#include "storage/KeyRetriever.h"
#include "storage/ThreadPool.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/loon_ffi/util.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/lob_column/lob_column_reader.h"
#include "segcore/TextColumnCache.h"

namespace milvus::segcore {

using namespace milvus::cachinglayer;

namespace {

int64_t
GetLoadedFieldRows(
    const std::vector<std::unordered_map<FieldId, std::vector<FieldDataPtr>>>&
        column_group_results,
    FieldId field_id) {
    int64_t rows = 0;
    bool found = false;
    for (const auto& column_group_result : column_group_results) {
        auto it = column_group_result.find(field_id);
        if (it == column_group_result.end()) {
            continue;
        }
        found = true;
        for (const auto& field_data : it->second) {
            rows += field_data->get_num_rows();
        }
    }
    return found ? rows : -1;
}

void
AssertLoadedFieldRows(
    const std::vector<std::unordered_map<FieldId, std::vector<FieldDataPtr>>>&
        column_group_results,
    FieldId field_id,
    int64_t expected_rows,
    const char* field_name) {
    if (expected_rows == 0) {
        return;
    }
    auto rows = GetLoadedFieldRows(column_group_results, field_id);
    AssertInfo(rows == expected_rows,
               "growing segment StorageV3 manifest loads {} rows for {} "
               "field {}, but SegmentLoadInfo expects {} rows",
               rows,
               field_name,
               field_id.get(),
               expected_rows);
}

void
AssertAllLoadedFieldRows(
    const std::vector<std::unordered_map<FieldId, std::vector<FieldDataPtr>>>&
        column_group_results,
    int64_t expected_rows) {
    if (expected_rows == 0) {
        return;
    }

    std::unordered_map<FieldId, int64_t> loaded_rows;
    for (const auto& column_group_result : column_group_results) {
        for (const auto& [field_id, field_data] : column_group_result) {
            auto& rows = loaded_rows[field_id];
            for (const auto& data : field_data) {
                rows += data->get_num_rows();
            }
        }
    }

    for (const auto& [field_id, rows] : loaded_rows) {
        AssertInfo(rows == expected_rows,
                   "growing segment StorageV3 manifest loads {} rows for "
                   "field {}, but SegmentLoadInfo expects {} rows",
                   rows,
                   field_id.get(),
                   expected_rows);
    }
}

int32_t
GetVectorArrayLength(const proto::schema::VectorField& vec_field,
                     DataType element_type,
                     int64_t dim) {
    switch (element_type) {
        case DataType::VECTOR_FLOAT:
            return vec_field.float_vector().data_size() / dim;
        case DataType::VECTOR_FLOAT16:
            return vec_field.float16_vector().size() / (dim * 2);
        case DataType::VECTOR_BFLOAT16:
            return vec_field.bfloat16_vector().size() / (dim * 2);
        case DataType::VECTOR_BINARY:
            return vec_field.binary_vector().size() / (dim / 8);
        case DataType::VECTOR_INT8:
            return vec_field.int8_vector().size() / dim;
        default:
            ThrowInfo(ErrorCode::UnexpectedError,
                      "Unexpected VECTOR_ARRAY element type: {}",
                      element_type);
    }
    return 0;
}

void
ExtractArrayLengthsFromFieldData(const std::vector<FieldDataPtr>& field_data,
                                 const FieldMeta& field_meta,
                                 int32_t* array_lengths) {
    auto data_type = field_meta.get_data_type();
    int64_t offset = 0;

    for (const auto& data : field_data) {
        auto num_rows = data->get_num_rows();

        if (data_type == DataType::VECTOR_ARRAY) {
            auto* raw_data = static_cast<const VectorArray*>(data->Data());
            int64_t physical_row = 0;
            for (int64_t i = 0; i < num_rows; ++i) {
                if (data->IsNullable() && !data->is_valid(i)) {
                    array_lengths[offset + i] = 0;
                    continue;
                }
                auto source_index = data->IsNullable() ? physical_row++ : i;
                array_lengths[offset + i] = raw_data[source_index].length();
            }
        } else {
            // For regular array types (INT32, FLOAT, etc.)
            auto* raw_data = static_cast<const ArrayView*>(data->Data());
            for (int64_t i = 0; i < num_rows; ++i) {
                array_lengths[offset + i] = raw_data[i].length();
            }
        }
        offset += num_rows;
    }
}

void
ExtractArrayLengths(const proto::schema::FieldData& field_data,
                    const FieldMeta& field_meta,
                    int64_t num_rows,
                    int32_t* array_lengths) {
    auto data_type = field_meta.get_data_type();
    if (data_type == DataType::VECTOR_ARRAY) {
        const auto& vector_array = field_data.vectors().vector_array();
        int64_t dim = field_meta.get_dim();
        auto element_type = field_meta.get_element_type();
        bool compact_nullable = false;
        if (field_meta.is_nullable()) {
            int64_t valid_count = 0;
            if (field_data.valid_data_size() == num_rows) {
                for (int64_t i = 0; i < num_rows; ++i) {
                    if (field_data.valid_data(i)) {
                        ++valid_count;
                    }
                }
            }
            auto dense_aligned = vector_array.data_size() == num_rows;
            compact_nullable = field_data.valid_data_size() == num_rows &&
                               vector_array.data_size() == valid_count;
            AssertInfo(
                dense_aligned || compact_nullable,
                "nullable VECTOR_ARRAY supports only dense-aligned data "
                "(data_size == num_rows) or compact data "
                "(valid_data_size == num_rows and data_size == valid_count), "
                "got data_size {}, valid_data_size {}, num_rows {}, "
                "valid_count {}",
                vector_array.data_size(),
                field_data.valid_data_size(),
                num_rows,
                valid_count);
            compact_nullable = compact_nullable && !dense_aligned;
        } else {
            AssertInfo(vector_array.data_size() == num_rows,
                       "non-nullable VECTOR_ARRAY data_size {} must match "
                       "num_rows {}",
                       vector_array.data_size(),
                       num_rows);
        }
        int64_t physical_row = 0;
        auto has_valid_data = field_data.valid_data_size() == num_rows;

        for (int i = 0; i < num_rows; ++i) {
            if (field_meta.is_nullable() && has_valid_data &&
                !field_data.valid_data(i)) {
                array_lengths[i] = 0;
                continue;
            }

            auto source_index = compact_nullable ? physical_row++ : i;
            AssertInfo(source_index < vector_array.data_size(),
                       "VECTOR_ARRAY row {} is missing from field data",
                       source_index);
            array_lengths[i] = GetVectorArrayLength(
                vector_array.data(source_index), element_type, dim);
        }
    } else {
        // ARRAY: extract from scalars().array_data().data(i)
        const auto& array_data = field_data.scalars().array_data();
        auto element_type = field_meta.get_element_type();

        for (int i = 0; i < num_rows; ++i) {
            int32_t array_len = 0;

            switch (element_type) {
                case DataType::BOOL:
                    array_len = array_data.data(i).bool_data().data_size();
                    break;
                case DataType::INT8:
                case DataType::INT16:
                case DataType::INT32:
                    array_len = array_data.data(i).int_data().data_size();
                    break;
                case DataType::INT64:
                    array_len = array_data.data(i).long_data().data_size();
                    break;
                case DataType::FLOAT:
                    array_len = array_data.data(i).float_data().data_size();
                    break;
                case DataType::DOUBLE:
                    array_len = array_data.data(i).double_data().data_size();
                    break;
                case DataType::STRING:
                case DataType::VARCHAR:
                    array_len = array_data.data(i).string_data().data_size();
                    break;
                default:
                    ThrowInfo(ErrorCode::UnexpectedError,
                              "Unexpected array type: {}",
                              element_type);
            }

            array_lengths[i] = array_len;
        }
    }

    // Handle nullable fields
    if (field_meta.is_nullable() && field_data.valid_data_size() > 0) {
        const auto& valid_data = field_data.valid_data();
        for (int i = 0; i < num_rows; ++i) {
            if (!valid_data[i]) {
                array_lengths[i] = 0;  // null → empty array
            }
        }
    }
}

bool
SchemaHasTextField(const Schema& schema) {
    return std::any_of(schema.get_fields().begin(),
                       schema.get_fields().end(),
                       [](const auto& field) {
                           return field.second.get_data_type() ==
                                  DataType::TEXT;
                       });
}

}  // anonymous namespace

void
SegmentGrowingImpl::InitializeArrayOffsets() {
    // Group fields by struct_name
    std::unordered_map<std::string, std::vector<FieldId>> struct_fields;

    for (const auto& [field_id, field_meta] : schema_->get_fields()) {
        auto struct_name = GetStructNameForArrayField(field_meta);
        if (struct_name.has_value()) {
            struct_fields[*struct_name].push_back(field_id);
        }
    }

    // Create one ArrayOffsetsGrowing per struct, shared by all its fields
    for (const auto& [struct_name, field_ids] : struct_fields) {
        auto array_offsets = std::make_shared<ArrayOffsetsGrowing>();

        // Pick the first field as representative (any field works since array lengths are identical)
        FieldId representative_field = field_ids[0];

        // Map all field_ids from this struct to the same ArrayOffsetsGrowing
        for (auto field_id : field_ids) {
            array_offsets_map_[field_id] = array_offsets;
        }

        // Record representative field for Insert-time updates
        struct_representative_fields_.insert(representative_field);

        LOG_INFO(
            "Created ArrayOffsetsGrowing for struct '{}' with {} fields, "
            "representative field_id={}",
            struct_name,
            field_ids.size(),
            representative_field.get());
    }
}

int64_t
SegmentGrowingImpl::PreInsert(int64_t size) {
    auto reserved_begin = insert_record_.reserved.fetch_add(size);
    return reserved_begin;
}

void
SegmentGrowingImpl::mask_with_delete(BitsetTypeView& bitset,
                                     int64_t ins_barrier,
                                     Timestamp timestamp) const {
    deleted_record_.Query(bitset, ins_barrier, timestamp);
}

void
SegmentGrowingImpl::try_remove_chunks(FieldId fieldId) {
    if (retain_insert_record_chunks_for_flush_) {
        // StorageV3 TEXT and growing-source flush persist growing segments
        // through milvus-storage. Interim indexes may also contain raw vector
        // data, but FlushGrowingSegmentData reads directly from insert_record_
        // chunks. Keep raw chunks until the flush path can reliably export from
        // indexes too.
        return;
    }

    //remove the chunk data to reduce memory consumption
    auto& field_meta = schema_->operator[](fieldId);
    auto data_type = field_meta.get_data_type();
    if (IsVectorDataType(data_type)) {
        if (indexing_record_.HasRawData(fieldId)) {
            auto vec_data_base = insert_record_.get_data_base(fieldId);
            if (vec_data_base && vec_data_base->num_chunk() > 0 &&
                chunk_mutex_.try_lock()) {
                vec_data_base->clear();
                chunk_mutex_.unlock();
            }
        }
    }
}

ResourceUsage
SegmentGrowingImpl::EstimateSegmentResourceUsage() const {
    int64_t num_rows = get_row_count();
    if (num_rows == 0) {
        return ResourceUsage{0, 0};
    }

    bool growing_mmap_enabled = storage::MmapManager::GetInstance()
                                    .GetMmapConfig()
                                    .GetEnableGrowingMmap();

    int64_t memory_bytes = 0;
    int64_t disk_bytes = 0;

    // 1. Timestamps: always in memory for now
    memory_bytes += num_rows * sizeof(Timestamp);
    // RowID is a system column kept for V3 manifest flush.
    memory_bytes += num_rows * sizeof(int64_t);

    // 2. pk2offset_ map: always in memory
    // Use actual allocated memory from the tracking allocator
    memory_bytes += insert_record_.pk2offset_->memory_size();

    // 3. Field data and interim index
    // For vector fields with interim index:
    //   - IVF_FLAT_CC: index stores raw data, so count index_size = raw_size * memExpansionRate (memory)
    //   - SCANN_DVR: index doesn't store raw data, so count raw_size (memory or mmap)
    // For other fields: count raw_size based on mmap setting
    bool interim_index_enabled =
        segcore_config_.get_enable_interim_segment_index();
    bool is_ivf_flat_cc =
        segcore_config_.get_dense_vector_intermin_index_type() ==
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC;

    for (const auto& [field_id, field_meta] : schema_->get_fields()) {
        if (field_id.get() < START_USER_FIELDID) {
            continue;
        }

        int64_t field_bytes = 0;
        auto data_type = field_meta.get_data_type();

        if (field_meta.is_vector()) {
            // Calculate raw vector size
            // Note: get_dim() cannot be called on sparse vectors, so handle that case separately
            if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
                field_bytes =
                    num_rows *
                    SegmentInternalInterface::get_field_avg_size(field_id);
            } else {
                int64_t dim = field_meta.get_dim();
                switch (data_type) {
                    case DataType::VECTOR_FLOAT:
                        field_bytes = num_rows * dim * sizeof(float);
                        break;
                    case DataType::VECTOR_FLOAT16:
                        field_bytes = num_rows * dim * sizeof(float16);
                        break;
                    case DataType::VECTOR_BFLOAT16:
                        field_bytes = num_rows * dim * sizeof(bfloat16);
                        break;
                    case DataType::VECTOR_BINARY:
                        field_bytes = num_rows * (dim / 8);
                        break;
                    case DataType::VECTOR_INT8:
                        field_bytes = num_rows * dim * sizeof(int8_t);
                        break;
                    default:
                        break;
                }
            }

            // Check if this field has interim index
            bool has_interim_index =
                interim_index_enabled && indexing_record_.is_in(field_id);

            if (has_interim_index) {
                if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
                    // sparse vector interim index does not support file mmap
                    // index memory + raw data memory ~ 2x raw data memory
                    memory_bytes += field_bytes * 2;
                } else {
                    // Dense vector interim index estimation based on index type
                    if (is_ivf_flat_cc) {
                        // IVF_FLAT_CC: index stores raw data
                        // Index memory = raw_size * memExpansionRate
                        memory_bytes += static_cast<int64_t>(
                            field_bytes *
                            segcore_config_
                                .get_interim_index_mem_expansion_rate());
                    } else {
                        // SCANN_DVR or no interim index
                        if (growing_mmap_enabled) {
                            disk_bytes += field_bytes;
                        } else {
                            memory_bytes += field_bytes;
                        }
                    }
                }
            } else {
                if (growing_mmap_enabled) {
                    disk_bytes += field_bytes;
                } else {
                    memory_bytes += field_bytes;
                }
            }
        } else {
            // Scalar fields
            switch (data_type) {
                case DataType::BOOL:
                    field_bytes = num_rows * sizeof(bool);
                    break;
                case DataType::INT8:
                    field_bytes = num_rows * sizeof(int8_t);
                    break;
                case DataType::INT16:
                    field_bytes = num_rows * sizeof(int16_t);
                    break;
                case DataType::INT32:
                    field_bytes = num_rows * sizeof(int32_t);
                    break;
                case DataType::INT64:
                case DataType::TIMESTAMPTZ:
                    field_bytes = num_rows * sizeof(int64_t);
                    break;
                case DataType::FLOAT:
                    field_bytes = num_rows * sizeof(float);
                    break;
                case DataType::DOUBLE:
                    field_bytes = num_rows * sizeof(double);
                    break;
                case DataType::VARCHAR:
                case DataType::TEXT:
                case DataType::GEOMETRY: {
                    auto avg_size =
                        SegmentInternalInterface::get_field_avg_size(field_id);
                    field_bytes = num_rows * avg_size;
                    break;
                }
                case DataType::JSON: {
                    auto avg_size =
                        SegmentInternalInterface::get_field_avg_size(field_id);
                    field_bytes = num_rows * avg_size;
                    break;
                }
                case DataType::ARRAY: {
                    auto avg_size =
                        SegmentInternalInterface::get_field_avg_size(field_id);
                    field_bytes = num_rows * avg_size;
                    break;
                }
                default:
                    break;
            }

            // Scalar fields: memory or disk based on mmap setting
            if (growing_mmap_enabled) {
                disk_bytes += field_bytes;
            } else {
                memory_bytes += field_bytes;
            }
        }
    }

    // 4. Text index (Tantivy)
    {
        std::shared_lock lock(mutex_);
        for (const auto& [field_id, index_variant] : text_indexes_) {
            if (auto* ptr = std::get_if<std::unique_ptr<index::TextMatchIndex>>(
                    &index_variant)) {
                memory_bytes += (*ptr)->ByteSize();
            }
        }
    }

    // 4b. TEXT LOB spillover disk usage
    for (const auto& [field_id, spillover] : text_lob_spillovers_) {
        if (spillover) {
            disk_bytes += static_cast<int64_t>(spillover->GetDiskUsage());
        }
    }

    // 5. Deleted records overhead
    memory_bytes += deleted_record_.mem_size();

    // Apply safety margin
    constexpr double kResourceSafetyMargin = 1.2;
    memory_bytes = static_cast<int64_t>(memory_bytes * kResourceSafetyMargin);
    disk_bytes = static_cast<int64_t>(disk_bytes * kResourceSafetyMargin);

    return ResourceUsage{memory_bytes, disk_bytes};
}

void
SegmentGrowingImpl::UpdateResourceTracking() {
    auto new_resource = EstimateSegmentResourceUsage();

    // Lock to ensure refund-then-charge is atomic
    std::lock_guard<std::mutex> lock(resource_tracking_mutex_);

    auto old_resource = tracked_resource_;

    if (old_resource.AnyGTZero()) {
        Manager::GetInstance().RefundLoadedResource(
            old_resource, fmt::format("growing_segment_{}_refund", id_));
    }

    if (new_resource.AnyGTZero()) {
        Manager::GetInstance().ChargeLoadedResource(
            new_resource, fmt::format("growing_segment_{}_charge", id_));
    }

    tracked_resource_ = new_resource;
}

void
SegmentGrowingImpl::Insert(int64_t reserved_offset,
                           int64_t num_rows,
                           const int64_t* row_ids,
                           const Timestamp* timestamps_raw,
                           InsertRecordProto* insert_record_proto) {
    AssertInfo(insert_record_proto->num_rows() == num_rows,
               "Entities_raw count not equal to insert size");
    // protect schema being changed during insert
    // schema change cannot happends during insertion,
    // otherwise, there might be some data not following new schema
    std::shared_lock lck(sch_mutex_);

    // step 1: check insert data if valid
    std::unordered_map<FieldId, int64_t> field_id_to_offset;
    int64_t field_offset = 0;

    for (const auto& field : insert_record_proto->fields_data()) {
        auto field_id = FieldId(field.field_id());
        AssertInfo(!field_id_to_offset.count(field_id), "duplicate field data");
        field_id_to_offset.emplace(field_id, field_offset++);
        AssertInfo(insert_record_.is_data_exist(field_id),
                   "unexpected new field in growing segment {}, field id {}",
                   id_,
                   field.field_id());
    }

    // segment have latest schema while insert used old one
    // need to fill insert data with field_meta
    for (auto& [field_id, field_meta] : schema_->get_fields()) {
        if (field_id.get() < START_USER_FIELDID) {
            continue;
        }
        if (field_id_to_offset.count(field_id) > 0) {
            continue;
        }
        if (schema_->is_function_output(field_id)) {
            LOG_INFO(
                "schema newer than insert data found for segment {}, skip "
                "bulk fill for function output field {}",
                id_,
                field_id.get());
            continue;
        }
        LOG_INFO(
            "schema newer than insert data found for segment {}, attach empty "
            "field data"
            "not exist field {}, data type {}",
            id_,
            field_id.get(),
            field_meta.get_data_type());
        auto data = bulk_subscript_not_exist_field(field_meta, num_rows);
        insert_record_proto->add_fields_data()->CopyFrom(*data);
        field_id_to_offset.emplace(field_id, field_offset++);
    }

    // step 2: sort timestamp
    // query node already guarantees that the timestamp is ordered, avoid field data copy in c++

    // step 3: fill into Segment.ConcurrentVector, no mmap_descriptor is used for timestamps
    insert_record_.timestamps_.set_data_raw(
        reserved_offset, timestamps_raw, num_rows);
    stats_.mem_size += num_rows * sizeof(Timestamp);

    AssertInfo(row_ids != nullptr, "row ids should not be null");
    insert_record_.row_ids_.set_data_raw(reserved_offset, row_ids, num_rows);
    stats_.mem_size += num_rows * sizeof(int64_t);

    for (auto& [field_id, field_meta] : schema_->get_fields()) {
        if (field_id.get() < START_USER_FIELDID) {
            continue;
        }
        if (field_id_to_offset.count(field_id) == 0) {
            AssertInfo(schema_->is_function_output(field_id),
                       "field {} missing from insert without bulk fill",
                       field_id.get());
            continue;
        }
        auto data_offset = field_id_to_offset[field_id];
        if (field_meta.is_nullable()) {
            insert_record_.get_valid_data(field_id)->set_data_raw(
                num_rows,
                &insert_record_proto->fields_data(data_offset),
                field_meta);
        }
        // Growing-source flush reads raw data from insert_record_. Keep it
        // populated even when the interim index can also serve raw vector data.
        // Otherwise later inserts after index sync would be visible by row count
        // but missing from field chunks during FlushGrowingSegmentData.
        if (field_meta.get_data_type() == DataType::TEXT) {
            auto spillover = GetTextLobSpillover(field_id);
            AssertInfo(spillover != nullptr, "TEXT field must have spillover");
            const auto& field_data =
                insert_record_proto->fields_data(data_offset);
            const auto& string_data = field_data.scalars().string_data();

            std::vector<std::string> ref_strings(num_rows);
            for (int64_t i = 0; i < num_rows; i++) {
                const auto& text = string_data.data(i);
                ref_strings[i] = spillover->WriteAndEncode(text);
            }

            auto* vec_base = insert_record_.get_data_base(field_id);
            auto* string_vec =
                dynamic_cast<ConcurrentVector<std::string>*>(vec_base);
            AssertInfo(string_vec != nullptr,
                       "TEXT field must use ConcurrentVector<std::string>");
            string_vec->set_data_raw(
                reserved_offset, ref_strings.data(), num_rows);
        } else {
            insert_record_.get_data_base(field_id)->set_data_raw(
                reserved_offset,
                num_rows,
                &insert_record_proto->fields_data(data_offset),
                field_meta);
        }

        //insert vector data into index
        if (segcore_config_.get_enable_interim_segment_index()) {
            indexing_record_.AppendingIndex(
                reserved_offset,
                num_rows,
                field_id,
                &insert_record_proto->fields_data(data_offset),
                insert_record_,
                field_meta);
        }

        // update ArrayOffsetsGrowing for struct fields
        if (struct_representative_fields_.count(field_id) > 0) {
            const auto& field_data =
                insert_record_proto->fields_data(data_offset);

            std::vector<int32_t> array_lengths(num_rows);
            ExtractArrayLengths(
                field_data, field_meta, num_rows, array_lengths.data());

            auto offsets_it = array_offsets_map_.find(field_id);
            if (offsets_it != array_offsets_map_.end()) {
                offsets_it->second->Insert(
                    reserved_offset, array_lengths.data(), num_rows);
            }
        }

        // index text.
        if (field_meta.enable_match()) {
            // TODO: iterate texts and call `AddText` instead of `AddTexts`. This may cost much more memory.
            std::vector<std::string> texts(
                insert_record_proto->fields_data(data_offset)
                    .scalars()
                    .string_data()
                    .data()
                    .begin(),
                insert_record_proto->fields_data(data_offset)
                    .scalars()
                    .string_data()
                    .data()
                    .end());
            FixedVector<bool> texts_valid_data(
                insert_record_proto->fields_data(data_offset)
                    .valid_data()
                    .begin(),
                insert_record_proto->fields_data(data_offset)
                    .valid_data()
                    .end());
            AddTexts(field_id,
                     texts.data(),
                     texts_valid_data.data(),
                     num_rows,
                     reserved_offset);
        }

        // update average row data size
        auto field_data_size = GetRawDataSizeOfDataArray(
            &insert_record_proto->fields_data(data_offset),
            field_meta,
            num_rows);
        if (IsVariableDataType(field_meta.get_data_type())) {
            SegmentInternalInterface::set_field_avg_size(
                field_id, num_rows, field_data_size);
        }

        // Build geometry cache for GEOMETRY fields
        if (field_meta.get_data_type() == DataType::GEOMETRY &&
            segcore_config_.get_enable_geometry_cache()) {
            BuildGeometryCacheForInsert(
                field_id,
                &insert_record_proto->fields_data(data_offset),
                num_rows);
        }

        stats_.mem_size += field_data_size;

        try_remove_chunks(field_id);
    }

    // step 4: set pks to offset
    auto field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(field_id.get() != INVALID_FIELD_ID, "Primary key is -1");
    std::vector<PkType> pks(num_rows);
    ParsePksFromFieldData(pks,
                          *insert_record_proto->mutable_fields_data(
                              field_id_to_offset[field_id]));
    for (int i = 0; i < num_rows; ++i) {
        insert_record_.insert_pk(pks[i], reserved_offset + i);
    }

    // step 5: update the resource usage
    UpdateResourceTracking();

    // step 6: update small indexes
    insert_record_.ack_responder_.AddSegment(reserved_offset,
                                             reserved_offset + num_rows);
}

void
SegmentGrowingImpl::LoadFieldData(const LoadFieldDataInfo& infos,
                                  milvus::OpContext* op_ctx) {
    // Note: op_ctx is currently unused in growing segments but kept for interface consistency
    (void)op_ctx;
    switch (infos.storage_version) {
        case STORAGE_V2:
            load_column_group_data_internal(infos);
            break;
        default:
            load_field_data_internal(infos);
            break;
    }
}

void
SegmentGrowingImpl::load_field_data_internal(const LoadFieldDataInfo& infos) {
    AssertInfo(infos.field_infos.find(TimestampFieldID.get()) !=
                   infos.field_infos.end(),
               "timestamps field data should be included");
    AssertInfo(
        infos.field_infos.find(RowFieldID.get()) != infos.field_infos.end(),
        "rowID field data should be included");
    auto primary_field_id =
        schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(primary_field_id.get() != INVALID_FIELD_ID, "Primary key is -1");
    AssertInfo(infos.field_infos.find(primary_field_id.get()) !=
                   infos.field_infos.end(),
               "primary field data should be included");

    size_t num_rows = storage::GetNumRowsForLoadInfo(infos);
    auto reserved_offset = PreInsert(num_rows);
    for (auto& [id, info] : infos.field_infos) {
        auto field_id = FieldId(id);

        // Skip fields that have been dropped from schema (except system fields)
        if (!SystemProperty::Instance().IsSystem(field_id) &&
            !schema_->has_field(field_id)) {
            LOG_INFO("growing segment skips dropped field {} during load",
                     field_id.get());
            continue;
        }

        auto insert_files = info.insert_files;
        storage::SortByPath(insert_files);

        auto channel = std::make_shared<FieldDataChannel>();
        auto& pool =
            ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);

        int total = 0;
        for (int num : info.entries_nums) {
            total += num;
        }
        if (total != info.row_count) {
            AssertInfo(total <= info.row_count,
                       "binlog number should less than or equal row_count");
            auto field_meta = get_schema()[field_id];
            AssertInfo(field_meta.is_nullable(),
                       "nullable must be true when lack rows");
            auto lack_num = info.row_count - total;
            auto field_data =
                storage::CreateFieldData(field_meta.get_data_type(),
                                         field_meta.get_element_type(),
                                         true,
                                         1,
                                         lack_num);
            field_data->FillFieldData(field_meta.default_value(), lack_num);
            channel->push(field_data);
        }

        LOG_INFO("segment {} loads field {} with num_rows {}",
                 this->get_segment_id(),
                 field_id.get(),
                 num_rows);
        auto load_future = pool.Submit(LoadFieldDatasFromRemote,
                                       insert_files,
                                       channel,
                                       infos.load_priority);

        LOG_INFO("segment {} submits load field {} task to thread pool",
                 this->get_segment_id(),
                 field_id.get());
        auto field_data = storage::CollectFieldDataChannel(channel);
        load_field_data_common(
            field_id, reserved_offset, field_data, primary_field_id, num_rows);
    }

    // step 5: update small indexes
    insert_record_.ack_responder_.AddSegment(reserved_offset,
                                             reserved_offset + num_rows);
}

void
SegmentGrowingImpl::load_field_data_common(
    FieldId field_id,
    size_t reserved_offset,
    const std::vector<FieldDataPtr>& field_data,
    FieldId primary_field_id,
    size_t num_rows) {
    if (field_id == TimestampFieldID) {
        // step 2: sort timestamp
        // query node already guarantees that the timestamp is ordered, avoid field data copy in c++

        // step 3: fill into Segment.ConcurrentVector
        insert_record_.timestamps_.set_data_raw(reserved_offset, field_data);
        return;
    }

    if (field_id == RowFieldID) {
        insert_record_.row_ids_.set_data_raw(reserved_offset, field_data);
        stats_.mem_size += storage::GetByteSizeOfFieldDatas(field_data);
        return;
    }

    // Skip if field has been dropped from schema
    if (!schema_->has_field(field_id)) {
        LOG_INFO(
            "growing segment skips dropped field {} in load_field_data_common",
            field_id.get());
        return;
    }

    auto field_meta = (*schema_)[field_id];

    // Growing-source flush reads raw data from insert_record_. Keep it
    // populated even when an interim index can provide raw vector data.
    if (insert_record_.is_valid_data_exist(field_id)) {
        insert_record_.get_valid_data(field_id)->set_data_raw(field_data);
    }
    insert_record_.get_data_base(field_id)->set_data_raw(reserved_offset,
                                                         field_data);
    if (segcore_config_.get_enable_interim_segment_index()) {
        auto offset = reserved_offset;
        for (auto& data : field_data) {
            auto row_count = data->get_num_rows();
            indexing_record_.AppendingIndex(
                offset, row_count, field_id, data, insert_record_, field_meta);
            offset += row_count;
        }
    }
    try_remove_chunks(field_id);

    if (field_id == primary_field_id) {
        insert_record_.insert_pks(field_data);
    }

    // update average row data size
    if (IsVariableDataType(field_meta.get_data_type())) {
        SegmentInternalInterface::set_field_avg_size(
            field_id, num_rows, storage::GetByteSizeOfFieldDatas(field_data));
    }

    // build text match index
    if (field_meta.enable_match()) {
        if (field_meta.get_data_type() == DataType::TEXT &&
            HasTextLobPath(field_id)) {
            BuildTextIndexFromTextLobRefs(
                field_id, field_data, reserved_offset, field_meta);
        } else {
            auto pinned = GetTextIndex(nullptr, field_id);
            auto index = pinned.get();
            index->BuildIndexFromFieldData(field_data,
                                           field_meta.is_nullable());
            index->Commit();
            // Reload reader so that the index can be read immediately
            index->Reload();
        }
    }

    // update ArrayOffsetsGrowing for struct fields
    if (struct_representative_fields_.count(field_id) > 0) {
        std::vector<int32_t> array_lengths(num_rows);
        ExtractArrayLengthsFromFieldData(
            field_data, field_meta, array_lengths.data());

        auto offsets_it = array_offsets_map_.find(field_id);
        if (offsets_it != array_offsets_map_.end()) {
            offsets_it->second->Insert(
                reserved_offset, array_lengths.data(), num_rows);
        }

        LOG_INFO("Updated ArrayOffsetsGrowing for field {} with {} rows",
                 field_id.get(),
                 num_rows);
    }

    // update the mem size
    stats_.mem_size += storage::GetByteSizeOfFieldDatas(field_data);

    LOG_INFO("segment {} loads field {} done",
             this->get_segment_id(),
             field_id.get());
}

void
SegmentGrowingImpl::load_column_group_data_internal(
    const LoadFieldDataInfo& infos) {
    AssertInfo(!SchemaHasTextField(*schema_),
               "TEXT growing segment cannot be loaded from StorageV2 column "
               "groups; StorageV3 manifest is required");

    auto primary_field_id =
        schema_->get_primary_field_id().value_or(FieldId(-1));

    size_t num_rows = storage::GetNumRowsForLoadInfo(infos);
    auto reserved_offset = PreInsert(num_rows);
    text_loaded_row_count_ = num_rows;
    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    ArrowSchemaPtr arrow_schema = schema_->ConvertToArrowSchema();

    for (auto& [id, info] : infos.field_infos) {
        auto column_group_id = FieldId(id);
        auto insert_files = info.insert_files;
        storage::SortByPath(insert_files);
        auto fs = milvus::segcore::GetDefaultArrowFileSystem();
        auto column_group_info = FieldDataInfo(
            column_group_id.get(), num_rows, "", false, infos.shard);
        column_group_info.arrow_reader_channel->set_capacity(parallel_degree);

        LOG_INFO(
            "[StorageV2] segment {} loads column group {} with num_rows {}",
            this->get_segment_id(),
            column_group_id.get(),
            num_rows);

        auto& pool =
            ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);

        // Get all row groups for each file
        std::vector<std::vector<int64_t>> row_group_lists;
        row_group_lists.reserve(insert_files.size());
        for (const auto& file : insert_files) {
            auto result = milvus_storage::FileRowGroupReader::Make(
                fs,
                file,
                milvus_storage::DEFAULT_READ_BUFFER_SIZE,
                storage::GetReaderProperties(),
                storage::GetArrowReaderProperties());
            AssertInfo(result.ok(),
                       "[StorageV2] Failed to create file row group reader: " +
                           result.status().ToString());
            auto reader = result.ValueOrDie();
            auto row_group_num =
                reader->file_metadata()->GetRowGroupMetadataVector().size();
            std::vector<int64_t> all_row_groups(row_group_num);
            std::iota(all_row_groups.begin(), all_row_groups.end(), 0);
            row_group_lists.push_back(all_row_groups);
            auto status = reader->Close();
            AssertInfo(
                status.ok(),
                "[StorageV2] failed to close file reader when get row group "
                "metadata from file {} with error {}",
                file,
                status.ToString());
        }

        // create parallel degree split strategy
        auto strategy =
            std::make_unique<ParallelDegreeSplitStrategy>(parallel_degree);

        auto load_future = pool.Submit([&]() {
            return LoadWithStrategy(insert_files,
                                    column_group_info.arrow_reader_channel,
                                    DEFAULT_FIELD_MAX_MEMORY_LIMIT,
                                    std::move(strategy),
                                    row_group_lists,
                                    fs,
                                    nullptr,
                                    infos.load_priority);
        });

        LOG_INFO(
            "[StorageV2] segment {} submits load column group {} task to "
            "thread pool",
            this->get_segment_id(),
            column_group_id.get());

        // The load task captures this frame by reference and may be blocked
        // pushing into the bounded channel. If the consumer loop below
        // throws, unblock the task and wait for it before unwinding
        // (see #46958).
        auto drain_on_unwind = folly::makeGuard([&]() {
            try {
                std::shared_ptr<milvus::ArrowDataWrapper> discard;
                while (column_group_info.arrow_reader_channel->pop(discard)) {
                }
            } catch (...) {
            }
            storage::DrainFuture(load_future);
        });

        std::shared_ptr<milvus::ArrowDataWrapper> r;

        std::unordered_map<FieldId, std::vector<FieldDataPtr>> field_data_map;
        while (column_group_info.arrow_reader_channel->pop(r)) {
            for (const auto& table_info : r->arrow_tables) {
                size_t batch_num_rows = table_info.table->num_rows();
                for (int i = 0; i < table_info.table->schema()->num_fields();
                     ++i) {
                    AssertInfo(
                        table_info.table->schema()
                            ->field(i)
                            ->metadata()
                            ->Contains(milvus_storage::ARROW_FIELD_ID_KEY),
                        "[StorageV2] field id not found in metadata for field "
                        "{}",
                        table_info.table->schema()->field(i)->name());
                    auto field_id =
                        std::stoll(table_info.table->schema()
                                       ->field(i)
                                       ->metadata()
                                       ->Get(milvus_storage::ARROW_FIELD_ID_KEY)
                                       ->data());

                    // Skip if field has been dropped from schema
                    if (!schema_->has_field(FieldId(field_id))) {
                        LOG_INFO(
                            "growing segment skips dropped field {} in column "
                            "group",
                            field_id);
                        continue;
                    }

                    for (auto& field : schema_->get_fields()) {
                        if (field.second.get_id().get() != field_id) {
                            continue;
                        }
                        auto data_type = field.second.get_data_type();
                        auto field_data = storage::CreateFieldData(
                            data_type,
                            field.second.get_element_type(),
                            field.second.is_nullable(),
                            IsVectorDataType(data_type) &&
                                    !IsSparseFloatVectorDataType(data_type)
                                ? field.second.get_dim()
                                : 1,
                            batch_num_rows);
                        field_data->FillFieldData(table_info.table->column(i));
                        field_data_map[FieldId(field_id)].push_back(field_data);
                    }
                }
            }
        }
        // access underlying feature to get exception if any
        load_future.get();
        drain_on_unwind.dismiss();

        for (auto& [field_id, field_data] : field_data_map) {
            load_field_data_common(field_id,
                                   reserved_offset,
                                   field_data,
                                   primary_field_id,
                                   num_rows);
            // Build geometry cache for GEOMETRY fields
            if (schema_->operator[](field_id).get_data_type() ==
                    DataType::GEOMETRY &&
                segcore_config_.get_enable_geometry_cache()) {
                BuildGeometryCacheForLoad(field_id, field_data);
            }
        }
    }

    // step 5: update small indexes
    insert_record_.ack_responder_.AddSegment(reserved_offset,
                                             reserved_offset + num_rows);
}

SegcoreError
SegmentGrowingImpl::Delete(int64_t size,
                           const IdArray* ids,
                           const Timestamp* timestamps_raw) {
    auto field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(field_id.get() != -1, "Primary key is -1");
    auto& field_meta = schema_->operator[](field_id);
    std::vector<PkType> pks(size);
    ParsePksFromIDs(pks, field_meta.get_data_type(), *ids);

    // filter out the deletions that the primary key not exists
    std::vector<std::tuple<Timestamp, PkType>> ordering(size);
    for (int i = 0; i < size; i++) {
        ordering[i] = std::make_tuple(timestamps_raw[i], pks[i]);
    }
    auto end =
        std::remove_if(ordering.begin(),
                       ordering.end(),
                       [&](const std::tuple<Timestamp, PkType>& record) {
                           return !insert_record_.contain(std::get<1>(record));
                       });
    size = end - ordering.begin();
    ordering.resize(size);
    if (size == 0) {
        return SegcoreError::success();
    }

    // step 1: sort timestamp
    std::sort(ordering.begin(), ordering.end());
    std::vector<PkType> sort_pks(size);
    std::vector<Timestamp> sort_timestamps(size);

    for (int i = 0; i < size; i++) {
        auto [t, pk] = ordering[i];
        sort_timestamps[i] = t;
        sort_pks[i] = pk;
    }

    // step 2: fill delete record
    deleted_record_.StreamPush(sort_pks, sort_timestamps.data());

    // step 3: update resource tracking
    UpdateResourceTracking();

    return SegcoreError::success();
}

void
SegmentGrowingImpl::LoadDeletedRecord(const LoadDeletedRecordInfo& info) {
    AssertInfo(info.row_count > 0, "The row count of deleted record is 0");
    AssertInfo(info.primary_keys, "Deleted primary keys is null");
    AssertInfo(info.timestamps, "Deleted timestamps is null");
    // step 1: get pks and timestamps
    auto field_id =
        schema_->get_primary_field_id().value_or(FieldId(INVALID_FIELD_ID));
    AssertInfo(field_id.get() != INVALID_FIELD_ID,
               "Primary key has invalid field id");
    auto& field_meta = schema_->operator[](field_id);
    int64_t size = info.row_count;
    std::vector<PkType> pks(size);
    ParsePksFromIDs(pks, field_meta.get_data_type(), *info.primary_keys);
    auto timestamps = reinterpret_cast<const Timestamp*>(info.timestamps);

    // step 2: push delete info to delete_record
    deleted_record_.LoadPush(pks, timestamps);
}

PinWrapper<SpanBase>
SegmentGrowingImpl::chunk_data_impl(milvus::OpContext* op_ctx,
                                    FieldId field_id,
                                    int64_t chunk_id) const {
    return PinWrapper<SpanBase>(
        get_insert_record().get_span_base(field_id, chunk_id));
}

void
SegmentGrowingImpl::ApplyFieldValidData(milvus::OpContext* op_ctx,
                                        FieldId field_id,
                                        int64_t chunk_id,
                                        int64_t offset,
                                        int64_t size,
                                        TargetBitmapView valid_result) const {
    (void)op_ctx;
    if (size == 0) {
        return;
    }
    auto& field_meta = schema_->operator[](field_id);
    if (!field_meta.is_nullable()) {
        return;
    }

    auto valid_vec_ptr = insert_record_.get_valid_data(field_id);
    auto data = insert_record_.get_data_base(field_id);
    auto row_offset = data->get_size_per_chunk() * chunk_id + offset;
    auto valid_data = valid_vec_ptr->get_chunk_data(row_offset);
    for (int64_t i = 0; i < size; ++i) {
        if (!valid_data[i]) {
            valid_result[i] = false;
        }
    }
}

void
SegmentGrowingImpl::ApplyFieldValidDataByOffsets(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const int64_t* offsets,
    int64_t count,
    TargetBitmapView valid_result) const {
    (void)op_ctx;
    if (count == 0) {
        return;
    }
    auto& field_meta = schema_->operator[](field_id);
    if (!field_meta.is_nullable()) {
        return;
    }

    auto valid_vec_ptr = insert_record_.get_valid_data(field_id);
    for (int64_t i = 0; i < count; ++i) {
        if (!valid_vec_ptr->is_valid(offsets[i])) {
            valid_result[i] = false;
        }
    }
}

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
SegmentGrowingImpl::chunk_string_view_impl(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "chunk string view impl not implement for growing segment");
}

PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
SegmentGrowingImpl::chunk_array_view_impl(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "chunk array view impl not implement for growing segment");
}

PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
SegmentGrowingImpl::chunk_vector_array_view_impl(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    (void)op_ctx;

    auto& field_meta = schema_->operator[](field_id);
    AssertInfo(field_meta.get_data_type() == DataType::VECTOR_ARRAY,
               "chunk_vector_array_view_impl only supports VECTOR_ARRAY field");

    auto vector_data = insert_record_.get_data<VectorArray>(field_id);
    auto size_per_chunk = vector_data->get_size_per_chunk();
    auto active_count = insert_record_.ack_responder_.GetAck();
    auto start_offset = int64_t{0};
    auto len = size_per_chunk;
    if (offset_len.has_value()) {
        start_offset = offset_len->first;
        len = offset_len->second;
    }

    AssertInfo(start_offset >= 0 && start_offset < size_per_chunk,
               "Retrieve vector array views with out-of-bound offset:{}, "
               "len:{}, wrong",
               start_offset,
               len);
    AssertInfo(len >= 0 && len <= size_per_chunk,
               "Retrieve vector array views with out-of-bound offset:{}, "
               "len:{}, wrong",
               start_offset,
               len);
    AssertInfo(start_offset + len <= size_per_chunk,
               "Retrieve vector array views with out-of-bound offset:{}, "
               "len:{}, wrong",
               start_offset,
               len);

    auto logical_start = chunk_id * size_per_chunk + start_offset;
    AssertInfo(logical_start >= 0 && logical_start <= active_count,
               "Retrieve vector array views with out-of-bound offset:{}, "
               "len:{}, wrong",
               start_offset,
               len);
    if (offset_len.has_value()) {
        AssertInfo(logical_start + len <= active_count,
                   "Retrieve vector array views with out-of-bound offset:{}, "
                   "len:{}, wrong",
                   start_offset,
                   len);
    } else {
        len = std::min(len, active_count - logical_start);
    }

    std::vector<VectorArrayView> views;
    views.reserve(len);
    FixedVector<bool> valid_data;
    ThreadSafeValidDataPtr valid_vec_ptr = nullptr;
    if (field_meta.is_nullable()) {
        valid_vec_ptr = insert_record_.get_valid_data(field_id);
        valid_data.reserve(len);
    }

    for (int64_t i = 0; i < len; ++i) {
        auto logical_offset = logical_start + i;
        if (field_meta.is_nullable()) {
            auto valid = valid_vec_ptr->is_valid(logical_offset);
            valid_data.push_back(valid);
            if (!valid) {
                views.emplace_back();
                continue;
            }
        }

        auto vector_array = vector_data->get_element(logical_offset);
        AssertInfo(vector_array != nullptr,
                   "Cannot find VECTOR_ARRAY data at segment offset {}",
                   logical_offset);
        views.emplace_back(const_cast<char*>(vector_array->data()),
                           vector_array->dim(),
                           vector_array->length(),
                           vector_array->byte_size(),
                           vector_array->get_element_type());
    }

    std::pair<std::vector<VectorArrayView>, FixedVector<bool>> content{
        std::move(views), std::move(valid_data)};
    return PinWrapper<
        std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>(
        std::move(content));
}

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
SegmentGrowingImpl::chunk_string_views_by_offsets(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    const FixedVector<int32_t>& offsets) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "chunk view by offsets not implemented for growing segment");
}

PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
SegmentGrowingImpl::chunk_array_views_by_offsets(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    const FixedVector<int32_t>& offsets) const {
    ThrowInfo(
        ErrorCode::NotImplemented,
        "chunk array views by offsets not implemented for growing segment");
}

int64_t
SegmentGrowingImpl::num_chunk(FieldId field_id) const {
    auto size = get_insert_record().ack_responder_.GetAck();
    return upper_div(size, segcore_config_.get_chunk_rows());
}

DataType
SegmentGrowingImpl::GetFieldDataType(milvus::FieldId field_id) const {
    auto& field_meta = schema_->operator[](field_id);
    return field_meta.get_data_type();
}

void
SegmentGrowingImpl::search_batch_pks(
    const std::vector<PkType>& pks,
    const Timestamp* timestamps,
    bool include_same_ts,
    const std::function<void(const SegOffset offset, const Timestamp ts)>&
        callback) const {
    for (size_t i = 0; i < pks.size(); ++i) {
        auto timestamp = timestamps[i];
        auto offsets =
            insert_record_.search_pk(pks[i], timestamp, include_same_ts);
        for (auto offset : offsets) {
            callback(offset, timestamp);
        }
    }
}

void
SegmentGrowingImpl::vector_search(SearchInfo& search_info,
                                  const void* query_data,
                                  const size_t* query_offsets,
                                  int64_t query_count,
                                  Timestamp timestamp,
                                  const BitsetView& bitset,
                                  milvus::OpContext* op_context,
                                  SearchResult& output) const {
    query::SearchOnGrowing(*this,
                           search_info,
                           query_data,
                           query_offsets,
                           query_count,
                           timestamp,
                           bitset,
                           op_context,
                           output);
}

template <typename SetOutput>
void
SegmentGrowingImpl::bulk_subscript_text_impl(FieldId field_id,
                                             const VectorBase* vec_ptr,
                                             const int64_t* seg_offsets,
                                             int64_t count,
                                             SetOutput set_output) const {
    auto vec = dynamic_cast<const ConcurrentVector<std::string>*>(vec_ptr);
    AssertInfo(vec != nullptr,
               "TEXT field must use ConcurrentVector<std::string>");
    auto& src = *vec;
    auto spillover = GetTextLobSpillover(field_id);
    AssertInfo(spillover != nullptr, "TEXT field must have spillover");

    std::vector<int64_t> loaded_indices;
    std::vector<milvus_storage::lob_column::EncodedRef> encoded_refs;
    std::vector<int64_t> spillover_indices;
    std::vector<std::string_view> spillover_refs;

    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        if (offset == INVALID_SEG_OFFSET) {
            set_output(i, std::string());
            continue;
        }
        if (offset < text_loaded_row_count_) {
            auto ref_str = src.view_element(offset);
            auto ptr = reinterpret_cast<const uint8_t*>(ref_str.data());
            if (milvus_storage::lob_column::IsInlineData(ptr)) {
                set_output(i,
                           milvus_storage::lob_column::DecodeInlineText(
                               ptr, ref_str.size()));
            } else {
                encoded_refs.push_back({ptr, ref_str.size()});
                loaded_indices.push_back(i);
            }
        } else {
            auto ref_str = src.view_element(offset);
            spillover_refs.push_back(ref_str);
            spillover_indices.push_back(i);
        }
    }

    if (!spillover_refs.empty()) {
        auto texts = spillover->DecodeAndReadBatch(spillover_refs);
        for (size_t j = 0; j < spillover_indices.size(); ++j) {
            set_output(spillover_indices[j], std::move(texts[j]));
        }
    }

    if (!encoded_refs.empty()) {
        auto properties =
            milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
                .GetProperties();
        auto fs = milvus::segcore::GetDefaultArrowFileSystem();
        google::protobuf::RepeatedPtrField<std::string> resolved;
        resolved.Reserve(encoded_refs.size());
        for (size_t j = 0; j < encoded_refs.size(); ++j) {
            resolved.Add();
        }
        auto& cache = GetGlobalTextColumnCache();
        cache.ReadBatchInto(text_lob_paths_.at(field_id),
                            fs,
                            *properties,
                            encoded_refs,
                            &resolved);
        for (size_t j = 0; j < loaded_indices.size(); ++j) {
            set_output(loaded_indices[j], std::move(*resolved.Mutable(j)));
        }
    }
}

std::unique_ptr<DataArray>
SegmentGrowingImpl::bulk_subscript(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const int64_t* seg_offsets,
    int64_t count,
    const std::vector<std::string>& dynamic_field_names) const {
    Assert(!dynamic_field_names.empty());
    auto& field_meta = schema_->operator[](field_id);
    auto vec_ptr = insert_record_.get_data_base(field_id);
    auto result = CreateEmptyScalarDataArray(count, field_meta);
    if (field_meta.is_nullable()) {
        auto valid_data_ptr = insert_record_.get_valid_data(field_id);
        auto res = result->mutable_valid_data()->mutable_data();
        for (int64_t i = 0; i < count; ++i) {
            auto offset = seg_offsets[i];
            res[i] = valid_data_ptr->is_valid(offset);
        }
    }
    auto vec = dynamic_cast<const ConcurrentVector<Json>*>(vec_ptr);
    auto dst = result->mutable_scalars()->mutable_json_data()->mutable_data();
    auto& src = *vec;
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        dst->at(i) =
            ExtractSubJson(std::string_view(src[offset]), dynamic_field_names);
    }
    return result;
}

std::unique_ptr<DataArray>
SegmentGrowingImpl::bulk_subscript(milvus::OpContext* op_ctx,
                                   FieldId field_id,
                                   const int64_t* seg_offsets,
                                   int64_t count) const {
    auto& field_meta = schema_->operator[](field_id);
    auto vec_ptr = insert_record_.get_data_base(field_id);
    if (field_meta.is_vector()) {
        int64_t valid_count = count;
        const bool* valid_data = nullptr;
        const int64_t* valid_offsets = seg_offsets;
        ValidResult filter_result;

        if (field_meta.is_nullable()) {
            filter_result =
                FilterVectorValidOffsets(op_ctx, field_id, seg_offsets, count);
            valid_count = filter_result.valid_count;
            valid_data = filter_result.valid_data.get();
            valid_offsets = filter_result.valid_offsets.data();
        }

        auto result = CreateEmptyVectorDataArray(
            count, valid_count, valid_data, field_meta);
        if (valid_count == 0) {
            return result;
        }
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            bulk_subscript_impl<FloatVector>(op_ctx,
                                             field_id,
                                             field_meta.get_sizeof(),
                                             vec_ptr,
                                             valid_offsets,
                                             valid_count,
                                             result->mutable_vectors()
                                                 ->mutable_float_vector()
                                                 ->mutable_data()
                                                 ->mutable_data());
        } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
            bulk_subscript_impl<BinaryVector>(
                op_ctx,
                field_id,
                field_meta.get_sizeof(),
                vec_ptr,
                valid_offsets,
                valid_count,
                result->mutable_vectors()->mutable_binary_vector()->data());
        } else if (field_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
            bulk_subscript_impl<Float16Vector>(
                op_ctx,
                field_id,
                field_meta.get_sizeof(),
                vec_ptr,
                valid_offsets,
                valid_count,
                result->mutable_vectors()->mutable_float16_vector()->data());
        } else if (field_meta.get_data_type() == DataType::VECTOR_BFLOAT16) {
            bulk_subscript_impl<BFloat16Vector>(
                op_ctx,
                field_id,
                field_meta.get_sizeof(),
                vec_ptr,
                valid_offsets,
                valid_count,
                result->mutable_vectors()->mutable_bfloat16_vector()->data());
        } else if (field_meta.get_data_type() ==
                   DataType::VECTOR_SPARSE_U32_F32) {
            bulk_subscript_sparse_float_vector_impl(
                op_ctx,
                field_id,
                (const ConcurrentVector<SparseFloatVector>*)vec_ptr,
                valid_offsets,
                valid_count,
                result->mutable_vectors()->mutable_sparse_float_vector());
            result->mutable_vectors()->set_dim(
                result->vectors().sparse_float_vector().dim());
        } else if (field_meta.get_data_type() == DataType::VECTOR_INT8) {
            bulk_subscript_impl<Int8Vector>(
                op_ctx,
                field_id,
                field_meta.get_sizeof(),
                vec_ptr,
                valid_offsets,
                valid_count,
                result->mutable_vectors()->mutable_int8_vector()->data());
        } else if (field_meta.get_data_type() == DataType::VECTOR_ARRAY) {
            bulk_subscript_vector_array_impl(op_ctx,
                                             *vec_ptr,
                                             seg_offsets,
                                             count,
                                             valid_data,
                                             result->mutable_vectors()
                                                 ->mutable_vector_array()
                                                 ->mutable_data());
        } else {
            ThrowInfo(DataTypeInvalid, "logical error");
        }
        return result;
    }

    AssertInfo(!field_meta.is_vector(),
               "Scalar field meta type is vector type");

    auto result = CreateEmptyScalarDataArray(count, field_meta);
    if (field_meta.is_nullable()) {
        auto valid_data_ptr = insert_record_.get_valid_data(field_id);
        auto res = result->mutable_valid_data()->mutable_data();
        for (int64_t i = 0; i < count; ++i) {
            auto offset = seg_offsets[i];
            res[i] = valid_data_ptr->is_valid(offset);
        }
    }
    switch (field_meta.get_data_type()) {
        case DataType::BOOL: {
            bulk_subscript_impl<bool>(op_ctx,
                                      vec_ptr,
                                      seg_offsets,
                                      count,
                                      result->mutable_scalars()
                                          ->mutable_bool_data()
                                          ->mutable_data()
                                          ->mutable_data());
            break;
        }
        case DataType::INT8: {
            bulk_subscript_impl<int8_t>(op_ctx,
                                        vec_ptr,
                                        seg_offsets,
                                        count,
                                        result->mutable_scalars()
                                            ->mutable_int_data()
                                            ->mutable_data()
                                            ->mutable_data());
            break;
        }
        case DataType::INT16: {
            bulk_subscript_impl<int16_t>(op_ctx,
                                         vec_ptr,
                                         seg_offsets,
                                         count,
                                         result->mutable_scalars()
                                             ->mutable_int_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::INT32: {
            bulk_subscript_impl<int32_t>(op_ctx,
                                         vec_ptr,
                                         seg_offsets,
                                         count,
                                         result->mutable_scalars()
                                             ->mutable_int_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::INT64: {
            bulk_subscript_impl<int64_t>(op_ctx,
                                         vec_ptr,
                                         seg_offsets,
                                         count,
                                         result->mutable_scalars()
                                             ->mutable_long_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::FLOAT: {
            bulk_subscript_impl<float>(op_ctx,
                                       vec_ptr,
                                       seg_offsets,
                                       count,
                                       result->mutable_scalars()
                                           ->mutable_float_data()
                                           ->mutable_data()
                                           ->mutable_data());
            break;
        }
        case DataType::DOUBLE: {
            bulk_subscript_impl<double>(op_ctx,
                                        vec_ptr,
                                        seg_offsets,
                                        count,
                                        result->mutable_scalars()
                                            ->mutable_double_data()
                                            ->mutable_data()
                                            ->mutable_data());
            break;
        }
        case DataType::TIMESTAMPTZ: {
            bulk_subscript_impl<int64_t>(op_ctx,
                                         vec_ptr,
                                         seg_offsets,
                                         count,
                                         result->mutable_scalars()
                                             ->mutable_timestamptz_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::VARCHAR: {
            bulk_subscript_ptr_impl<std::string>(op_ctx,
                                                 vec_ptr,
                                                 seg_offsets,
                                                 count,
                                                 result->mutable_scalars()
                                                     ->mutable_string_data()
                                                     ->mutable_data());
            break;
        }
        case DataType::TEXT: {
            auto dst = result->mutable_scalars()
                           ->mutable_string_data()
                           ->mutable_data();
            bulk_subscript_text_impl(field_id,
                                     vec_ptr,
                                     seg_offsets,
                                     count,
                                     [dst](int64_t i, std::string val) {
                                         dst->at(i) = std::move(val);
                                     });
            break;
        }
        case DataType::JSON: {
            bulk_subscript_ptr_impl<Json>(
                op_ctx,
                vec_ptr,
                seg_offsets,
                count,
                result->mutable_scalars()->mutable_json_data()->mutable_data());
            break;
        }
        case DataType::GEOMETRY: {
            bulk_subscript_ptr_impl<std::string>(op_ctx,
                                                 vec_ptr,
                                                 seg_offsets,
                                                 count,
                                                 result->mutable_scalars()
                                                     ->mutable_geometry_data()
                                                     ->mutable_data());
            break;
        }
        case DataType::ARRAY: {
            // element
            bulk_subscript_array_impl(op_ctx,
                                      *vec_ptr,
                                      seg_offsets,
                                      count,
                                      result->mutable_scalars()
                                          ->mutable_array_data()
                                          ->mutable_data());
            break;
        }
        default: {
            ThrowInfo(
                DataTypeInvalid,
                fmt::format("unsupported type {}", field_meta.get_data_type()));
        }
    }
    return result;
}

void
SegmentGrowingImpl::bulk_subscript_sparse_float_vector_impl(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const ConcurrentVector<SparseFloatVector>* vec_raw,
    const int64_t* seg_offsets,
    int64_t count,
    milvus::proto::schema::SparseFloatArray* output) const {
    AssertInfo(HasRawData(field_id.get()), "Growing segment loss raw data");

    // if index has finished building, grab from index without any
    // synchronization operations.
    if (indexing_record_.SyncDataWithIndex(field_id)) {
        indexing_record_.GetDataFromIndex(
            field_id, seg_offsets, count, 0, output);
        return;
    }
    {
        std::lock_guard<std::shared_mutex> guard(chunk_mutex_);
        // check again after lock to make sure: if index has finished building
        // after the above check but before we grabbed the lock, we should grab
        // from index as the data in chunk may have been removed in
        // try_remove_chunks.
        if (!indexing_record_.SyncDataWithIndex(field_id)) {
            // copy from raw data
            SparseRowsToProto(
                [&](size_t i) {
                    auto offset = seg_offsets[i];
                    return offset != INVALID_SEG_OFFSET
                               ? vec_raw->get_physical_element(offset)
                               : nullptr;
                },
                count,
                output);
            return;
        }
        // else: release lock and copy from index
    }
    indexing_record_.GetDataFromIndex(field_id, seg_offsets, count, 0, output);
}

template <typename S>
void
SegmentGrowingImpl::bulk_subscript_ptr_impl(
    milvus::OpContext* op_ctx,
    const VectorBase* vec_raw,
    const int64_t* seg_offsets,
    int64_t count,
    google::protobuf::RepeatedPtrField<std::string>* dst) const {
    auto vec = dynamic_cast<const ConcurrentVector<S>*>(vec_raw);
    auto& src = *vec;
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        auto view = src.view_element(offset);
        dst->at(i).assign(view.data(), view.size());
    }
}

template <typename S, typename T>
void
SegmentGrowingImpl::bulk_subscript_ptr_impl(const VectorBase* vec_raw,
                                            const int64_t* seg_offsets,
                                            int64_t count,
                                            T* dst) const {
    auto vec = dynamic_cast<const ConcurrentVector<S>*>(vec_raw);
    auto& src = *vec;
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        if (offset != INVALID_SEG_OFFSET) {
            dst[i] = T(src.view_element(offset));
        } else {
            dst[i] = T();  // Default-initialize for invalid offsets
        }
    }
}

template <typename T>
void
SegmentGrowingImpl::bulk_subscript_impl(milvus::OpContext* op_ctx,
                                        FieldId field_id,
                                        int64_t element_sizeof,
                                        const VectorBase* vec_raw,
                                        const int64_t* seg_offsets,
                                        int64_t count,
                                        void* output_raw) const {
    static_assert(IsVector<T>);
    auto vec_ptr = dynamic_cast<const ConcurrentVector<T>*>(vec_raw);
    AssertInfo(vec_ptr, "Pointer of vec_raw is nullptr");
    auto& vec = *vec_ptr;

    // HasRawData interface guarantees that data can be fetched from growing segment
    AssertInfo(HasRawData(field_id.get()), "Growing segment loss raw data");

    // if index has finished building, grab from index without any
    // synchronization operations.
    if (indexing_record_.HasRawData(field_id)) {
        indexing_record_.GetDataFromIndex(
            field_id, seg_offsets, count, element_sizeof, output_raw);
        return;
    }
    {
        std::lock_guard<std::shared_mutex> guard(chunk_mutex_);
        // check again after lock to make sure: if index has finished building
        // after the above check but before we grabbed the lock, we should grab
        // from index as the data in chunk may have been removed in
        // try_remove_chunks.
        if (!indexing_record_.HasRawData(field_id)) {
            auto output_base = reinterpret_cast<char*>(output_raw);
            for (int i = 0; i < count; ++i) {
                auto dst = output_base + i * element_sizeof;
                auto offset = seg_offsets[i];
                auto src = (const uint8_t*)vec.get_physical_element(offset);
                milvus::fastmem::FastMemcpy(dst, src, element_sizeof);
            }
            return;
        }
        // else: release lock and copy from index
    }
    indexing_record_.GetDataFromIndex(
        field_id, seg_offsets, count, element_sizeof, output_raw);
}

template <typename S, typename T>
void
SegmentGrowingImpl::bulk_subscript_impl(milvus::OpContext* op_ctx,
                                        const VectorBase* vec_raw,
                                        const int64_t* seg_offsets,
                                        int64_t count,
                                        T* output,
                                        bool small_int_raw_type) const {
    static_assert(IsScalar<S>);
    auto vec_ptr = dynamic_cast<const ConcurrentVector<S>*>(vec_raw);
    AssertInfo(vec_ptr, "Pointer of vec_raw is nullptr");
    auto& vec = *vec_ptr;
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        output[i] = vec[offset];
    }
}

template <typename T>
void
SegmentGrowingImpl::bulk_subscript_array_impl(
    milvus::OpContext* op_ctx,
    const VectorBase& vec_raw,
    const int64_t* seg_offsets,
    int64_t count,
    google::protobuf::RepeatedPtrField<T>* dst) const {
    auto vec_ptr = dynamic_cast<const ConcurrentVector<Array>*>(&vec_raw);
    AssertInfo(vec_ptr, "Pointer of vec_raw is nullptr");
    auto& vec = *vec_ptr;
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        if (offset != INVALID_SEG_OFFSET) {
            dst->at(i) = vec[offset].output_data();
        }
    }
}

template <typename T>
void
SegmentGrowingImpl::bulk_subscript_vector_array_impl(
    milvus::OpContext* op_ctx,
    const VectorBase& vec_raw,
    const int64_t* seg_offsets,
    int64_t count,
    const bool* valid_data,
    google::protobuf::RepeatedPtrField<T>* dst) const {
    auto vec_ptr = dynamic_cast<const ConcurrentVector<VectorArray>*>(&vec_raw);
    AssertInfo(vec_ptr, "Pointer of vec_raw is nullptr");
    auto& vec = *vec_ptr;
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        if (offset == INVALID_SEG_OFFSET ||
            (valid_data != nullptr && !valid_data[i])) {
            continue;
        }
        auto value = vec.get_element(offset);
        AssertInfo(value != nullptr,
                   "Cannot find VECTOR_ARRAY data at segment offset {}",
                   offset);
        dst->at(i) = value->output_data();
    }
}

void
SegmentGrowingImpl::bulk_subscript(milvus::OpContext* op_ctx,
                                   SystemFieldType system_type,
                                   const int64_t* seg_offsets,
                                   int64_t count,
                                   void* output) const {
    switch (system_type) {
        case SystemFieldType::Timestamp:
            bulk_subscript_impl<Timestamp>(op_ctx,
                                           &this->insert_record_.timestamps_,
                                           seg_offsets,
                                           count,
                                           static_cast<Timestamp*>(output));
            break;
        case SystemFieldType::RowId:
            ThrowInfo(ErrorCode::Unsupported,
                      "RowId retrieve is not supported");
            break;
        default:
            ThrowInfo(DataTypeInvalid, "unknown subscript fields");
    }
}

void
SegmentGrowingImpl::bulk_subscript(milvus::OpContext* op_ctx,
                                   FieldId field_id,
                                   DataType data_type,
                                   const int64_t* seg_offsets,
                                   int64_t count,
                                   void* data,
                                   TargetBitmap& valid_map,
                                   bool small_int_raw_type) const {
    auto vec_ptr = insert_record_.get_data_base(field_id);
    auto& field_meta = schema_->operator[](field_id);
    valid_map.set();
    if (field_meta.is_nullable()) {
        auto valid_vec_ptr = insert_record_.get_valid_data(field_id);
        for (auto i = 0; i < count; i++) {
            valid_map.set(i, valid_vec_ptr->is_valid(seg_offsets[i]));
        }
    }

    switch (field_meta.get_data_type()) {
        case DataType::BOOL: {
            bulk_subscript_impl<bool>(
                op_ctx, vec_ptr, seg_offsets, count, static_cast<bool*>(data));
            break;
        }
        case DataType::INT8: {
            if (small_int_raw_type) {
                bulk_subscript_impl<int8_t>(op_ctx,
                                            vec_ptr,
                                            seg_offsets,
                                            count,
                                            static_cast<int8_t*>(data));
            } else {
                bulk_subscript_impl<int8_t, int32_t>(
                    op_ctx,
                    vec_ptr,
                    seg_offsets,
                    count,
                    static_cast<int32_t*>(data));
            }
            break;
        }
        case DataType::INT16: {
            if (small_int_raw_type) {
                bulk_subscript_impl<int16_t>(op_ctx,
                                             vec_ptr,
                                             seg_offsets,
                                             count,
                                             static_cast<int16_t*>(data));
            } else {
                bulk_subscript_impl<int16_t, int32_t>(
                    op_ctx,
                    vec_ptr,
                    seg_offsets,
                    count,
                    static_cast<int32_t*>(data));
            }
            break;
        }
        case DataType::INT32: {
            bulk_subscript_impl<int32_t>(op_ctx,
                                         vec_ptr,
                                         seg_offsets,
                                         count,
                                         static_cast<int32_t*>(data));
            break;
        }
        case DataType::TIMESTAMPTZ:
        case DataType::INT64: {
            bulk_subscript_impl<int64_t>(op_ctx,
                                         vec_ptr,
                                         seg_offsets,
                                         count,
                                         static_cast<int64_t*>(data));
            break;
        }
        case DataType::FLOAT: {
            bulk_subscript_impl<float>(
                op_ctx, vec_ptr, seg_offsets, count, static_cast<float*>(data));
            break;
        }
        case DataType::DOUBLE: {
            bulk_subscript_impl<double>(op_ctx,
                                        vec_ptr,
                                        seg_offsets,
                                        count,
                                        static_cast<double*>(data));
            break;
        }
        case DataType::VARCHAR: {
            bulk_subscript_ptr_impl<std::string>(
                vec_ptr, seg_offsets, count, static_cast<std::string*>(data));
            break;
        }
        case DataType::TEXT: {
            auto dst = static_cast<std::string*>(data);
            bulk_subscript_text_impl(
                field_id,
                vec_ptr,
                seg_offsets,
                count,
                [dst](int64_t i, std::string val) { dst[i] = std::move(val); });
            break;
        }
        case DataType::JSON: {
            bulk_subscript_ptr_impl<Json>(
                vec_ptr, seg_offsets, count, static_cast<Json*>(data));
            break;
        }
        case DataType::GEOMETRY: {
            bulk_subscript_ptr_impl<std::string>(
                vec_ptr, seg_offsets, count, static_cast<std::string*>(data));
            break;
        }
        case DataType::ARRAY: {
            auto vec = dynamic_cast<const ConcurrentVector<Array>*>(vec_ptr);
            AssertInfo(vec, "Pointer of vec_ptr is nullptr for ARRAY type");
            auto& src = *vec;
            auto dst = static_cast<Array*>(data);
            for (int64_t i = 0; i < count; ++i) {
                auto offset = seg_offsets[i];
                if (offset != INVALID_SEG_OFFSET) {
                    dst[i] = src[offset];
                } else {
                    dst[i] =
                        Array();  // Default-construct empty Array for invalid offsets
                }
            }
            break;
        }
        default: {
            ThrowInfo(
                DataTypeInvalid,
                fmt::format("unsupported type {}", field_meta.get_data_type()));
        }
    }
}

void
SegmentGrowingImpl::search_ids(BitsetType& bitset,
                               const IdArray& id_array) const {
    auto field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(field_id.get() != -1, "Primary key is -1");
    auto& field_meta = schema_->operator[](field_id);
    auto data_type = field_meta.get_data_type();
    auto ids_size = GetSizeOfIdArray(id_array);
    std::vector<PkType> pks(ids_size);
    ParsePksFromIDs(pks, data_type, id_array);

    BitsetTypeView bitset_view(bitset);
    for (auto& pk : pks) {
        insert_record_.search_pk_range(
            pk, proto::plan::OpType::Equal, bitset_view);
    }
}

int64_t
SegmentGrowingImpl::get_active_count(Timestamp ts) const {
    auto row_count = this->get_row_count();
    auto& ts_vec = this->get_insert_record().timestamps_;
    auto iter = std::upper_bound(
        boost::make_counting_iterator(static_cast<int64_t>(0)),
        boost::make_counting_iterator(row_count),
        ts,
        [&](Timestamp ts, int64_t index) { return ts < ts_vec[index]; });
    return *iter;
}

void
SegmentGrowingImpl::mask_with_timestamps(BitsetTypeView& bitset_chunk,
                                         Timestamp timestamp,
                                         Timestamp collection_ttl) const {
    if (collection_ttl > 0) {
        auto& timestamps = get_timestamps();
        auto size = bitset_chunk.size();
        if (timestamps[size - 1] <= collection_ttl) {
            bitset_chunk.set();
            return;
        }
        auto pilot = upper_bound(timestamps, 0, size, collection_ttl);
        BitsetType bitset;
        bitset.reserve(size);
        bitset.resize(pilot, true);
        bitset.resize(size, false);
        bitset_chunk |= bitset;
    }
}

void
SegmentGrowingImpl::BuildTextIndexFromTextLobRefs(
    FieldId field_id,
    const std::vector<FieldDataPtr>& field_data,
    size_t reserved_offset,
    const FieldMeta& field_meta) {
    AssertInfo(field_meta.get_data_type() == DataType::TEXT,
               "field {} is not TEXT",
               field_id.get());
    AssertInfo(HasTextLobPath(field_id),
               "TEXT field {} has no LOB path",
               field_id.get());

    auto properties = milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
                          .GetProperties();
    AssertInfo(properties != nullptr,
               "Loon FFI properties is not initialized for TEXT field {}",
               field_id.get());
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    auto& cache = GetGlobalTextColumnCache();
    const auto& lob_base_path = text_lob_paths_.at(field_id);

    auto pinned = GetTextIndex(nullptr, field_id);
    auto index = pinned.get();
    int64_t offset = reserved_offset;

    for (const auto& data : field_data) {
        auto n = data->get_num_rows();
        if (n == 0) {
            continue;
        }

        auto raw_refs = static_cast<const std::string*>(data->Data());
        for (int64_t batch_start = 0; batch_start < n;
             batch_start += kTextLobIndexBuildBatchSize) {
            auto batch_n = std::min<int64_t>(
                static_cast<int64_t>(kTextLobIndexBuildBatchSize),
                n - batch_start);
            FixedVector<std::string> decoded_texts(batch_n);
            FixedVector<bool> valid_data(batch_n, true);
            std::vector<int64_t> pending_indices;
            std::vector<milvus_storage::lob_column::EncodedRef> encoded_refs;
            pending_indices.reserve(batch_n);
            encoded_refs.reserve(batch_n);

            for (int64_t i = 0; i < batch_n; ++i) {
                auto row = batch_start + i;
                auto valid = !field_meta.is_nullable() || data->is_valid(row);
                valid_data[i] = valid;
                if (!valid) {
                    continue;
                }

                const auto& ref = raw_refs[row];
                encoded_refs.push_back(
                    MakeTextLobEncodedRef(ref.data(), ref.size()));
                pending_indices.push_back(i);
            }

            if (!encoded_refs.empty()) {
                auto texts = cache.ReadBatch(
                    lob_base_path, fs, *properties, encoded_refs);
                AssertInfo(
                    texts.size() == pending_indices.size(),
                    "TEXT LOB batch read returned inconsistent result size, "
                    "field {}, expected {}, actual {}",
                    field_id.get(),
                    pending_indices.size(),
                    texts.size());
                for (size_t i = 0; i < pending_indices.size(); ++i) {
                    decoded_texts[pending_indices[i]] = std::move(texts[i]);
                }
            }

            index->AddTextsGrowing(
                batch_n,
                decoded_texts.data(),
                field_meta.is_nullable() ? valid_data.data() : nullptr,
                offset + batch_start);
        }
        offset += n;
    }

    index->Commit();
    // Reload reader so that the index can be read immediately
    index->Reload();
}

void
SegmentGrowingImpl::CreateTextIndex(FieldId field_id,
                                    milvus::OpContext* op_ctx) {
    // Check for cancellation before starting
    CheckCancellation(
        op_ctx, id_, field_id.get(), "SegmentGrowingImpl::CreateTextIndex()");

    auto index = BuildTextIndexForMeta(schema_->operator[](field_id));
    std::unique_lock lock(mutex_);
    text_indexes_[field_id] = std::move(index);
}

// Builds a standalone index without publishing it into text_indexes_; the
// meta is passed in because Reopen runs before the field is in schema_.
std::unique_ptr<index::TextMatchIndex>
SegmentGrowingImpl::BuildTextIndexForMeta(const FieldMeta& field_meta) {
    AssertInfo(IsStringDataType(field_meta.get_data_type()),
               "cannot create text index on non-string type");
    std::string unique_id = GetUniqueFieldId(field_meta.get_id().get());
    // todo: make this(200) configurable.
    auto index = std::make_unique<index::TextMatchIndex>(
        200,
        unique_id.c_str(),
        "milvus_tokenizer",
        field_meta.get_analyzer_params().c_str(),
        /*enable_background_merge=*/true);
    index->Commit();
    index->CreateReader(milvus::index::SetBitsetGrowing);
    index->RegisterAnalyzer("milvus_tokenizer",
                            field_meta.get_analyzer_params().c_str());
    return index;
}

void
SegmentGrowingImpl::CreateTextIndexes() {
    for (const auto& [field_id, field_meta] : schema_->get_fields()) {
        if (IsStringDataType(field_meta.get_data_type()) &&
            field_meta.enable_match()) {
            CreateTextIndex(FieldId(field_id));
        }
    }
}

void
SegmentGrowingImpl::InitializeTextLobSpillovers() {
    // get base path from MmapManager config
    std::string base_path;

    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
    base_path = mmap_config.GetMmapPath();
    AssertInfo(!base_path.empty(), "Mmap path is empty");

    // create spillover for each TEXT field
    for (auto& [field_id, field_meta] : schema_->get_fields()) {
        if (field_meta.get_data_type() == DataType::TEXT) {
            text_lob_spillovers_[field_id] =
                std::make_unique<TextLobSpillover>(id_, field_id, base_path);
            LOG_INFO("Created TEXT LOB spillover for segment {} field {} at {}",
                     id_,
                     field_id.get(),
                     text_lob_spillovers_[field_id]->GetPath());
        }
    }
}

void
SegmentGrowingImpl::InitTextLobPaths(const std::string& manifest_path) {
    std::vector<FieldId> text_field_ids;
    for (auto& [field_id, field_meta] : schema_->get_fields()) {
        if (field_meta.get_data_type() == DataType::TEXT) {
            text_field_ids.push_back(field_id);
        }
    }

    if (text_field_ids.empty()) {
        return;
    }

    std::string segment_base_path;
    try {
        nlohmann::json j = nlohmann::json::parse(manifest_path);
        segment_base_path = j.at("base_path").get<std::string>();
    } catch (const std::exception& e) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Failed to parse manifest path for TEXT columns: {}",
                  e.what());
    }

    // segment_base_path format: {root}/{collectionID}/{partitionID}/{segmentID}
    // lob_base_path format: {root}/{collectionID}/{partitionID}/lobs/{field_id}
    std::filesystem::path segment_fs_path(segment_base_path);
    std::filesystem::path partition_path = segment_fs_path.parent_path();

    for (auto field_id : text_field_ids) {
        std::filesystem::path lob_base_path =
            partition_path / "lobs" / std::to_string(field_id.get());
        text_lob_paths_[field_id] = lob_base_path.string();
        LOG_INFO(
            "Initialized TEXT LOB path for growing segment {} field {}: {}",
            id_,
            field_id.get(),
            lob_base_path.string());
    }
}

void
SegmentGrowingImpl::AddTexts(milvus::FieldId field_id,
                             const std::string* texts,
                             const bool* texts_valid_data,
                             size_t n,
                             int64_t offset_begin) {
    std::unique_lock lock(mutex_);
    auto iter = text_indexes_.find(field_id);
    if (iter == text_indexes_.end()) {
        ThrowInfo(ErrorCode::TextIndexNotFound,
                  "text index not found for field {}",
                  field_id.get());
    }
    // only unique_ptr is supported for growing segment
    if (auto p = std::get_if<std::unique_ptr<milvus::index::TextMatchIndex>>(
            &iter->second)) {
        (*p)->AddTextsGrowing(n, texts, texts_valid_data, offset_begin);
    } else {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "text index of growing segment is not a unique_ptr for field "
                  "{}",
                  field_id.get());
    }
}

void
SegmentGrowingImpl::BulkGetJsonData(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const std::function<void(milvus::Json, size_t, bool)>& fn,
    const int64_t* offsets,
    int64_t count) const {
    auto vec_ptr = dynamic_cast<const ConcurrentVector<Json>*>(
        insert_record_.get_data_base(field_id));
    auto& src = *vec_ptr;
    auto& field_meta = schema_->operator[](field_id);
    if (field_meta.is_nullable()) {
        auto valid_data_ptr = insert_record_.get_valid_data(field_id);
        for (int64_t i = 0; i < count; ++i) {
            auto offset = offsets[i];
            fn(src[offset], i, valid_data_ptr->is_valid(offset));
        }
    } else {
        for (int64_t i = 0; i < count; ++i) {
            auto offset = offsets[i];
            fn(src[offset], i, true);
        }
    }
}

void
SegmentGrowingImpl::LazyCheckSchema(SchemaPtr sch, milvus::OpContext* op_ctx) {
    (void)op_ctx;
    if (sch->get_schema_version() > schema_->get_schema_version()) {
        LOG_INFO(
            "lazy check schema segment {} found newer schema version, "
            "current "
            "schema version {}, new schema version {}",
            id_,
            schema_->get_schema_version(),
            sch->get_schema_version());
        Reopen(sch);
    }
}

void
SegmentGrowingImpl::Reopen(SchemaPtr sch) {
    std::unique_lock lck(sch_mutex_);

    // double check condition, avoid multiple assignment
    if (sch->get_schema_version() > schema_->get_schema_version()) {
        auto absent_fields = sch->AbsentFields(*schema_);

        for (const auto& field_meta : *absent_fields) {
            if (sch->is_function_output(field_meta.get_id())) {
                continue;
            }
            fill_empty_field(field_meta);
        }

        auto row_count = insert_record_.row_count();
        // #50484: build text indexes for new enable_match fields BEFORE
        // publishing schema_ -- readers skip Reopen once the new schema_ is
        // visible. Backfill every pre-existing row (default value or nulls,
        // same as fill_empty_field): doc offsets must stay dense and aligned
        // with insert_record_ rows. Commit+Reload so the triggering query
        // sees the backfill. Stage all fields locally and publish together
        // only after every one succeeds: a present text_indexes_ entry is
        // always complete, and a mid-build throw publishes nothing (schema_
        // un-advanced, so the next query rebuilds from scratch).
        std::vector<std::pair<FieldId, std::unique_ptr<index::TextMatchIndex>>>
            staged_text_indexes;
        for (const auto& field_meta : *absent_fields) {
            if (sch->is_function_output(field_meta.get_id())) {
                continue;
            }
            auto field_id = field_meta.get_id();
            if (IsStringDataType(field_meta.get_data_type()) &&
                field_meta.enable_match() &&
                text_indexes_.find(field_id) == text_indexes_.end()) {
                auto index = BuildTextIndexForMeta(field_meta);
                if (row_count > 0) {
                    const bool has_default =
                        field_meta.default_value().has_value();
                    std::vector<std::string> texts(
                        row_count,
                        has_default ? field_meta.default_value()->string_data()
                                    : std::string());
                    FixedVector<bool> texts_valid_data(row_count, has_default);
                    index->AddTextsGrowing(
                        row_count, texts.data(), texts_valid_data.data(), 0);
                }
                index->Commit();
                index->Reload();
                staged_text_indexes.emplace_back(field_id, std::move(index));
            }
        }
        if (!staged_text_indexes.empty()) {
            std::unique_lock lock(mutex_);
            for (auto& [field_id, index] : staged_text_indexes) {
                text_indexes_[field_id] = std::move(index);
            }
        }

        schema_ = sch;

        for (const auto& field_meta : *absent_fields) {
            if (sch->is_function_output(field_meta.get_id())) {
                continue;
            }
            EnsureArrayOffsetsForStructField(field_meta, row_count);
        }
    }

    UpdateResourceTracking();
}

void
SegmentGrowingImpl::Reopen(
    milvus::OpContext* op_ctx,
    const milvus::proto::segcore::SegmentLoadInfo& new_load_info) {
    ThrowInfo(milvus::UnexpectedError,
              "Unexpected reopening growing segment {} with load info",
              id_);
}

void
SegmentGrowingImpl::Reopen(
    milvus::OpContext* op_ctx,
    const milvus::proto::segcore::SegmentLoadInfo& new_load_info,
    SchemaPtr new_schema) {
    ThrowInfo(milvus::UnexpectedError,
              "Unexpected reopening growing segment {} with load info",
              id_);
}

void
SegmentGrowingImpl::Load(milvus::tracer::TraceContext& trace_ctx,
                         milvus::OpContext* op_ctx) {
    // Convert load_info_ (SegmentLoadInfo) to LoadFieldDataInfo
    LoadFieldDataInfo field_data_info;

    // Set storage version
    field_data_info.storage_version = load_info_.storageversion();

    // Set load priority
    field_data_info.load_priority = load_info_.priority();
    field_data_info.shard = load_info_.insert_channel();

    auto manifest_path = load_info_.manifest_path();
    if (manifest_path != "") {
        LoadColumnsGroups(manifest_path);
        FillAbsentFields();
        return;
    }

    // Convert binlog_paths to field_infos
    for (const auto& field_binlog : load_info_.binlog_paths()) {
        FieldBinlogInfo binlog_info;
        binlog_info.field_id = field_binlog.fieldid();

        // Process each binlog
        int64_t total_row_count = 0;
        auto binlog_count = field_binlog.binlogs().size();
        binlog_info.entries_nums.reserve(binlog_count);
        binlog_info.insert_files.reserve(binlog_count);
        binlog_info.memory_sizes.reserve(binlog_count);
        for (const auto& binlog : field_binlog.binlogs()) {
            binlog_info.entries_nums.push_back(binlog.entries_num());
            binlog_info.insert_files.push_back(binlog.log_path());
            binlog_info.memory_sizes.push_back(binlog.memory_size());
            total_row_count += binlog.entries_num();
        }
        binlog_info.row_count = total_row_count;

        // Set child field ids
        binlog_info.child_field_ids.reserve(field_binlog.child_fields().size());
        for (const auto& child_field : field_binlog.child_fields()) {
            binlog_info.child_field_ids.push_back(child_field);
        }

        // Add to field_infos map
        field_data_info.field_infos[binlog_info.field_id] =
            std::move(binlog_info);
    }

    // Call LoadFieldData with the converted info
    if (!field_data_info.field_infos.empty()) {
        LoadFieldData(field_data_info);
    }

    FillAbsentFields();

    // Update resource tracking
    UpdateResourceTracking();
}

void
SegmentGrowingImpl::FillAbsentFields() {
    if (insert_record_.row_count() == 0) {
        return;
    }
    for (const auto& [field_id, field_meta] : schema_->get_fields()) {
        if (field_id.get() < START_USER_FIELDID) {
            continue;
        }
        if (schema_->is_function_output(field_id)) {
            continue;
        }
        if (IsVectorDataType(field_meta.get_data_type())) {
            // A vector field may be legally absent from the loaded data only
            // when it is nullable (added by AddField after the binlogs were
            // written). Backfill its validity bitmap so that queries observe
            // all-null values instead of an uninitialized column.
            if (field_meta.is_nullable() &&
                insert_record_.is_data_exist(field_id) &&
                insert_record_.is_valid_data_exist(field_id) &&
                insert_record_.get_valid_data(field_id)->get_data().empty()) {
                fill_empty_field(field_meta);
            }
            continue;
        }
        // append_data is called according to schema before
        // so we must check data empty here
        if (insert_record_.get_data_base(field_id)->empty()) {
            fill_empty_field(field_meta);
        }
    }
}

void
SegmentGrowingImpl::LoadColumnsGroups(std::string manifest_path) {
    LOG_INFO(
        "Loading segment {} field data with manifest {}", id_, manifest_path);
    // size_t num_rows = storage::GetNumRowsForLoadInfo(infos);
    auto num_rows = load_info_.num_of_rows();
    auto primary_field_id =
        schema_->get_primary_field_id().value_or(FieldId(-1));
    auto properties = milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
                          .GetProperties();
    auto loon_manifest = GetLoonManifest(manifest_path, properties);
    auto column_groups = std::make_shared<milvus_storage::api::ColumnGroups>(
        loon_manifest->columnGroups());

    // Initialize LOB paths before field data is loaded so TEXT text-match
    // indexes can be built from resolved text instead of raw LOB references.
    InitTextLobPaths(manifest_path);

    auto arrow_schema =
        schema_->ConvertToLoonArrowSchema(/*text_lob_as_binary=*/true);
    reader_ = milvus_storage::api::Reader::create(
        column_groups, arrow_schema, nullptr, *properties);

    // A column group whose fields were all dropped from the segment schema
    // has an empty read projection; opening it violates the reader contract
    // (ChunkReaderImpl::open rejects a group containing none of the needed
    // columns). Such a group is a legal leftover of drop semantics — skip it
    // and let compaction remove it from the manifest.
    auto schema_snapshot = schema_;
    auto group_has_live_field = [&](int64_t group_index) {
        for (const auto& column : column_groups->at(group_index)->columns) {
            // An untranslatable column name is a broken manifest and throws
            // (same contract as the sealed loader).
            auto field_id = schema_snapshot->ResolveColumnFieldId(column);
            if (schema_snapshot->has_field(field_id)) {
                return true;
            }
        }
        return false;
    };

    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    std::vector<
        std::future<std::unordered_map<FieldId, std::vector<FieldDataPtr>>>>
        load_group_futures;
    for (int64_t i = 0; i < column_groups->size(); ++i) {
        if (!group_has_live_field(i)) {
            LOG_INFO(
                "skip loading column group {} of segment {}: all of its "
                "fields are dropped from the schema",
                i,
                id_);
            continue;
        }
        auto future =
            pool.Submit([this, column_groups, properties, i, num_rows] {
                return LoadColumnGroup(column_groups, properties, i, num_rows);
            });
        load_group_futures.emplace_back(std::move(future));
    }

    std::vector<std::unordered_map<FieldId, std::vector<FieldDataPtr>>>
        column_group_results;
    std::vector<std::exception_ptr> load_exceptions;
    for (auto& future : load_group_futures) {
        try {
            column_group_results.emplace_back(future.get());
        } catch (...) {
            load_exceptions.push_back(std::current_exception());
        }
    }

    // If any exceptions occurred during index loading, handle them
    if (!load_exceptions.empty()) {
        LOG_ERROR("Failed to load {} out of {} indexes for segment {}",
                  load_exceptions.size(),
                  load_group_futures.size(),
                  id_);

        // Rethrow the first exception
        std::rethrow_exception(load_exceptions[0]);
    }

    AssertInfo(primary_field_id.get() != INVALID_FIELD_ID, "Primary key is -1");
    AssertLoadedFieldRows(
        column_group_results, primary_field_id, num_rows, "primary");
    AssertAllLoadedFieldRows(column_group_results, num_rows);
    auto timestamp_rows =
        GetLoadedFieldRows(column_group_results, TimestampFieldID);
    auto row_id_rows = GetLoadedFieldRows(column_group_results, RowFieldID);

    auto reserved_offset = PreInsert(num_rows);
    text_loaded_row_count_ = num_rows;

    if (timestamp_rows == -1 && num_rows > 0) {
        std::vector<Timestamp> timestamps(num_rows, 0);
        insert_record_.timestamps_.set_data_raw(
            reserved_offset, timestamps.data(), num_rows);
        stats_.mem_size += num_rows * sizeof(Timestamp);
    }
    if (row_id_rows == -1 && num_rows > 0) {
        std::vector<int64_t> row_ids(num_rows);
        std::iota(row_ids.begin(), row_ids.end(), reserved_offset);
        insert_record_.row_ids_.set_data_raw(
            reserved_offset, row_ids.data(), num_rows);
        stats_.mem_size += num_rows * sizeof(int64_t);
    }

    for (auto& column_group_result : column_group_results) {
        for (auto& [field_id, field_data] : column_group_result) {
            load_field_data_common(field_id,
                                   reserved_offset,
                                   field_data,
                                   primary_field_id,
                                   num_rows);
            // Build geometry cache for GEOMETRY fields
            if (schema_->operator[](field_id).get_data_type() ==
                    DataType::GEOMETRY &&
                segcore_config_.get_enable_geometry_cache()) {
                BuildGeometryCacheForLoad(field_id, field_data);
            }
        }
    }

    insert_record_.ack_responder_.AddSegment(reserved_offset,
                                             reserved_offset + num_rows);
}

std::unordered_map<FieldId, std::vector<FieldDataPtr>>
SegmentGrowingImpl::LoadColumnGroup(
    const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
    const std::shared_ptr<milvus_storage::api::Properties>& properties,
    int64_t index,
    int64_t row_limit) {
    AssertInfo(index < column_groups->size(),
               "load column group index out of range");
    auto column_group = column_groups->at(index);
    LOG_INFO("Loading segment {} column group {}", id_, index);

    auto chunk_reader_result = reader_->get_chunk_reader(index);
    AssertInfo(chunk_reader_result.ok(),
               "get chunk reader failed, segment {}, column group index {}, "
               "error: {}",
               get_segment_id(),
               index,
               chunk_reader_result.status().ToString());

    auto chunk_reader = std::move(chunk_reader_result.ValueOrDie());

    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);

    AssertInfo(row_limit >= 0,
               "load column group row limit should be non-negative, got {}",
               row_limit);
    auto chunk_rows_result = chunk_reader->get_chunk_rows();
    AssertInfo(chunk_rows_result.ok(),
               "get chunk rows failed, segment {}, column group index {}, "
               "error: {}",
               get_segment_id(),
               index,
               chunk_rows_result.status().ToString());

    auto chunk_rows = std::move(chunk_rows_result).ValueOrDie();
    std::vector<int64_t> row_groups_to_load;
    row_groups_to_load.reserve(chunk_rows.size());
    auto rows_to_read = int64_t{0};
    for (auto chunk_index = size_t{0};
         chunk_index < chunk_rows.size() && rows_to_read < row_limit;
         ++chunk_index) {
        row_groups_to_load.push_back(static_cast<int64_t>(chunk_index));
        rows_to_read += static_cast<int64_t>(chunk_rows[chunk_index]);
    }
    AssertInfo(rows_to_read >= row_limit,
               "column group {} has fewer rows than checkpoint row count, "
               "loaded rows {}, expected rows {}",
               index,
               rows_to_read,
               row_limit);

    // create parallel degree split strategy
    auto strategy =
        std::make_unique<ParallelDegreeSplitStrategy>(parallel_degree);
    auto split_result = strategy->split(row_groups_to_load);

    auto& thread_pool =
        ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);

    auto part_futures = std::vector<
        std::future<std::vector<std::shared_ptr<arrow::RecordBatch>>>>();
    for (const auto& part : split_result) {
        part_futures.emplace_back(
            thread_pool.Submit([chunk_reader = chunk_reader.get(), part]() {
                std::vector<int64_t> chunk_ids(part.count);
                std::iota(chunk_ids.begin(), chunk_ids.end(), part.offset);

                auto result = chunk_reader->get_chunks(chunk_ids, 1);
                AssertInfo(result.ok(), "get chunks failed");
                return result.ValueOrDie();
            }));
    }

    std::unordered_map<FieldId, std::vector<FieldDataPtr>> field_data_map;
    // Growing recovery may reuse a manifest that already contains rows past the
    // segment checkpoint. Only load rows covered by SegmentLoadInfo so WAL
    // replay can append the remaining rows at the expected offsets.
    int64_t loaded_rows = 0;
    storage::ProcessFuturesInOrder(part_futures, [&](auto part_result) {
        for (auto& record_batch : part_result) {
            if (loaded_rows >= row_limit) {
                break;
            }
            auto batch_num_rows = record_batch->num_rows();
            auto rows_to_load =
                std::min<int64_t>(batch_num_rows, row_limit - loaded_rows);
            for (auto i = 0; i < record_batch->num_columns(); ++i) {
                auto column = record_batch->column_name(i);

                auto field_id = FieldId(std::stoll(column));

                auto field = schema_->operator[](field_id);
                auto data_type = field.get_data_type();

                auto field_data = storage::CreateFieldData(
                    data_type,
                    field.get_element_type(),
                    field.is_nullable(),
                    IsVectorDataType(data_type) &&
                            !IsSparseFloatVectorDataType(data_type)
                        ? field.get_dim()
                        : 1,
                    rows_to_load);
                auto array = record_batch->column(i);
                if (rows_to_load < batch_num_rows) {
                    array = array->Slice(0, rows_to_load);
                }
                field_data->FillFieldData(array);
                field_data_map[field_id].push_back(field_data);
            }
            loaded_rows += rows_to_load;
        }
    });

    LOG_INFO("Finished loading segment {} column group {}", id_, index);
    return field_data_map;
}

void
SegmentGrowingImpl::fill_empty_field(const FieldMeta& field_meta) {
    auto field_id = field_meta.get_id();
    LOG_INFO("start fill empty field {} (data type {}) for growing segment {}",
             field_meta.get_data_type(),
             field_id.get(),
             id_);
    // append meta only needed when schema is old
    // loading old segment with new schema will have meta appended
    if (!insert_record_.is_data_exist(field_id)) {
        insert_record_.append_field_meta(
            field_id, field_meta, size_per_chunk(), mmap_descriptor_);
    }

    auto total_row_num = insert_record_.row_count();

    auto data = bulk_subscript_not_exist_field(field_meta, total_row_num);
    if (insert_record_.is_valid_data_exist(field_id)) {
        insert_record_.get_valid_data(field_id)->set_data_raw(
            total_row_num, data.get(), field_meta);
    }
    insert_record_.get_data_base(field_id)->set_data_raw(
        0, total_row_num, data.get(), field_meta);

    LOG_INFO("fill empty field {} (data type {}) for growing segment {} done",
             field_meta.get_data_type(),
             field_id.get(),
             id_);
}

void
SegmentGrowingImpl::EnsureArrayOffsetsForStructField(
    const FieldMeta& field_meta, int64_t row_count) {
    auto struct_name = GetStructNameForArrayField(field_meta);
    if (!struct_name.has_value()) {
        return;
    }

    std::shared_ptr<ArrayOffsetsGrowing> array_offsets;
    for (const auto& [field_id, offsets] : array_offsets_map_) {
        auto field_it = schema_->get_fields().find(field_id);
        if (field_it == schema_->get_fields().end()) {
            continue;
        }

        auto existing_struct_name =
            GetStructNameForArrayField(field_it->second);
        if (existing_struct_name == struct_name) {
            array_offsets = offsets;
            break;
        }
    }

    if (!array_offsets) {
        array_offsets = std::make_shared<ArrayOffsetsGrowing>();
        if (row_count > 0) {
            std::vector<int32_t> empty_lengths(row_count, 0);
            array_offsets->Insert(0, empty_lengths.data(), row_count);
        }
        struct_representative_fields_.insert(field_meta.get_id());
    }

    array_offsets_map_[field_meta.get_id()] = array_offsets;
}

void
SegmentGrowingImpl::BuildGeometryCacheForInsert(FieldId field_id,
                                                const DataArray* data_array,
                                                int64_t num_rows) {
    try {
        // Get geometry cache for this segment+field
        auto& geometry_cache =
            milvus::exec::SimpleGeometryCacheManager::Instance()
                .GetOrCreateCache(get_segment_id(), field_id);

        // Process geometry data from DataArray
        const auto& geometry_data = data_array->scalars().geometry_data();
        const auto& valid_data = data_array->valid_data();

        for (int64_t i = 0; i < num_rows; ++i) {
            if (valid_data.empty() ||
                (i < valid_data.size() && valid_data[i])) {
                // Valid geometry data
                const auto& wkb_data = geometry_data.data(i);
                geometry_cache.AppendData(
                    ctx_, wkb_data.data(), wkb_data.size());
            } else {
                // Null/invalid geometry
                geometry_cache.AppendData(ctx_, nullptr, 0);
            }
        }

        LOG_INFO(
            "Successfully appended {} geometries to cache for growing "
            "segment "
            "{} field {}",
            num_rows,
            get_segment_id(),
            field_id.get());

    } catch (const std::exception& e) {
        ThrowInfo(UnexpectedError,
                  "Failed to build geometry cache for growing segment {} field "
                  "{} insert: {}",
                  get_segment_id(),
                  field_id.get(),
                  e.what());
    }
}

void
SegmentGrowingImpl::BuildGeometryCacheForLoad(
    FieldId field_id, const std::vector<FieldDataPtr>& field_data) {
    try {
        // Get geometry cache for this segment+field
        auto& geometry_cache =
            milvus::exec::SimpleGeometryCacheManager::Instance()
                .GetOrCreateCache(get_segment_id(), field_id);

        // Process each field data chunk
        for (const auto& data : field_data) {
            auto num_rows = data->get_num_rows();

            for (int64_t i = 0; i < num_rows; ++i) {
                if (data->is_valid(i)) {
                    // Valid geometry data
                    auto wkb_data =
                        static_cast<const std::string*>(data->RawValue(i));
                    geometry_cache.AppendData(
                        ctx_, wkb_data->data(), wkb_data->size());
                } else {
                    // Null/invalid geometry
                    geometry_cache.AppendData(ctx_, nullptr, 0);
                }
            }
        }

        size_t total_rows = 0;
        for (const auto& data : field_data) {
            total_rows += data->get_num_rows();
        }

        LOG_INFO(
            "Successfully loaded {} geometries to cache for growing "
            "segment {} "
            "field {}",
            total_rows,
            get_segment_id(),
            field_id.get());

    } catch (const std::exception& e) {
        ThrowInfo(UnexpectedError,
                  "Failed to build geometry cache for growing segment {} field "
                  "{} load: {}",
                  get_segment_id(),
                  field_id.get(),
                  e.what());
    }
}

SegmentGrowingImpl::ValidResult
SegmentGrowingImpl::FilterVectorValidOffsets(milvus::OpContext* op_ctx,
                                             FieldId field_id,
                                             const int64_t* seg_offsets,
                                             int64_t count) const {
    ValidResult result;
    result.valid_count = count;

    if (indexing_record_.SyncDataWithIndex(field_id)) {
        const auto& field_indexing =
            indexing_record_.get_vec_field_indexing(field_id);
        auto indexing = field_indexing.get_segment_indexing();
        auto vec_index = dynamic_cast<index::VectorIndex*>(indexing.get());

        if (vec_index != nullptr && vec_index->HasValidData()) {
            result.valid_data = std::make_unique<bool[]>(count);
            vec_index->GetOffsetMapping().FilterValidLogicalOffsets(
                seg_offsets,
                count,
                result.valid_data.get(),
                result.valid_offsets);
            result.valid_count = result.valid_offsets.size();
        }
    } else {
        auto vec_base = insert_record_.get_data_base(field_id);
        if (vec_base != nullptr) {
            auto valid_data_vec = vec_base->get_valid_data();
            bool is_mapping_storage = vec_base->is_mapping_storage();
            if (!valid_data_vec.empty()) {
                result.valid_data = std::make_unique<bool[]>(count);

                if (is_mapping_storage) {
                    vec_base->get_offset_mapping().FilterValidLogicalOffsets(
                        seg_offsets,
                        count,
                        result.valid_data.get(),
                        result.valid_offsets);
                } else {
                    result.valid_offsets.reserve(count);
                    for (int64_t i = 0; i < count; ++i) {
                        auto offset = seg_offsets[i];
                        bool is_valid = offset >= 0 &&
                                        offset < static_cast<int64_t>(
                                                     valid_data_vec.size()) &&
                                        valid_data_vec[offset];
                        result.valid_data[i] = is_valid;
                    }
                }
                result.valid_count = result.valid_offsets.size();
            }
        }
    }
    return result;
}

}  // namespace milvus::segcore
