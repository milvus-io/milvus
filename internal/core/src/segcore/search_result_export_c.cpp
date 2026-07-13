// Copyright (C) 2019-2026 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "segcore/search_result_export_c.h"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/c/abi.h>
#include <folly/CancellationToken.h>
#include <folly/ScopeGuard.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <exception>
#include <future>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "log/Log.h"
#include "common/QueryResult.h"
#include "common/Types.h"
#include "futures/Future.h"
#include "monitor/Monitor.h"
#include "monitor/scope_metric.h"
#include "prometheus/histogram.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentInterface.h"
#include "segcore/Utils.h"
#include "segcore/reduce/Reduce.h"
#include "storage/ThreadPools.h"

using SearchResult = milvus::SearchResult;

namespace {

// GroupByArrowInfo describes one $group_by_<fieldID> Arrow column to emit.
// Element type is derived from the plan's search_info_, falling back to
// json_type_ when the group-by field is a JSON column. field_data_type is the
// Milvus type serialized as Arrow field metadata.
struct GroupByArrowInfo {
    milvus::FieldId field_id{0};
    milvus::DataType milvus_type{milvus::DataType::NONE};
    milvus::DataType field_data_type{milvus::DataType::NONE};
    std::shared_ptr<arrow::DataType> arrow_type;
};

struct CFreeDeleter {
    void
    operator()(void* ptr) const {
        free(ptr);
    }
};

using CBufferPtr = std::unique_ptr<void, CFreeDeleter>;
using ChunkSizesPtr = std::unique_ptr<int64_t, CFreeDeleter>;

void
ReleaseArrowSchemaIfNeeded(ArrowSchema* schema) {
    if (schema != nullptr && schema->release != nullptr) {
        schema->release(schema);
    }
}

void
ReleaseArrowArrayIfNeeded(ArrowArray* array) {
    if (array != nullptr && array->release != nullptr) {
        array->release(array);
    }
}

void
AssertEmptyCProto(const CProto* proto) {
    AssertInfo(proto != nullptr, "null CProto output");
    AssertInfo(proto->proto_blob == nullptr && proto->proto_size == 0,
               "CProto output must be empty before FillOutputFieldsOrdered");
}

constexpr const char* kMilvusFieldIDMetadataKey = "milvus.field_id";
constexpr const char* kMilvusDataTypeMetadataKey = "milvus.data_type";

std::shared_ptr<arrow::KeyValueMetadata>
MilvusFieldMetadata(milvus::FieldId field_id, milvus::DataType data_type) {
    return arrow::key_value_metadata(
        {kMilvusFieldIDMetadataKey, kMilvusDataTypeMetadataKey},
        {std::to_string(field_id.get()),
         std::to_string(static_cast<int32_t>(data_type))});
}

std::shared_ptr<arrow::Field>
MilvusField(const std::string& name,
            const std::shared_ptr<arrow::DataType>& arrow_type,
            bool nullable,
            milvus::FieldId field_id,
            milvus::DataType data_type) {
    return arrow::field(
        name, arrow_type, nullable, MilvusFieldMetadata(field_id, data_type));
}

void
SetFieldDataElementTypeIfNeeded(milvus::proto::schema::FieldData* field_data,
                                const milvus::FieldMeta& field_meta) {
    if (field_meta.get_data_type() == milvus::DataType::ARRAY) {
        field_data->mutable_scalars()->mutable_array_data()->set_element_type(
            milvus::proto::schema::DataType(field_meta.get_element_type()));
    } else if (field_meta.get_data_type() == milvus::DataType::VECTOR_ARRAY) {
        field_data->mutable_vectors()->mutable_vector_array()->set_element_type(
            milvus::proto::schema::DataType(field_meta.get_element_type()));
    }
}

CStatus
SerializeSearchResultDataToCProto(
    const milvus::proto::schema::SearchResultData& result_data,
    CProto* out_result,
    const char* allocation_error,
    const char* serialization_error) {
    auto size = result_data.ByteSizeLong();
    CBufferPtr buffer(malloc(size));
    if (buffer == nullptr) {
        return milvus::FailureCStatus(milvus::ErrorCode::UnexpectedError,
                                      allocation_error);
    }
    if (!result_data.SerializeToArray(buffer.get(), size)) {
        return milvus::FailureCStatus(milvus::ErrorCode::UnexpectedError,
                                      serialization_error);
    }

    out_result->proto_blob = buffer.release();
    out_result->proto_size = size;
    return milvus::SuccessCStatus();
}

std::string
GroupByColumnName(milvus::FieldId field_id) {
    return "$group_by_" + std::to_string(field_id.get());
}

arrow::Result<std::shared_ptr<arrow::DataType>>
EmptyExtraFieldArrowType(const milvus::FieldMeta& field_meta) {
    switch (field_meta.get_data_type()) {
        case milvus::DataType::BOOL:
            return arrow::boolean();
        case milvus::DataType::INT8:
        case milvus::DataType::INT16:
        case milvus::DataType::INT32:
            return arrow::int32();
        case milvus::DataType::INT64:
        case milvus::DataType::TIMESTAMPTZ:
            return arrow::int64();
        case milvus::DataType::FLOAT:
            return arrow::float32();
        case milvus::DataType::DOUBLE:
            return arrow::float64();
        case milvus::DataType::STRING:
        case milvus::DataType::VARCHAR:
        case milvus::DataType::TEXT:
            return arrow::utf8();
        case milvus::DataType::JSON:
            return arrow::binary();
        case milvus::DataType::GEOMETRY:
            return arrow::Status::NotImplemented(
                "GEOMETRY extra field Arrow export is not implemented");
        default:
            return milvus::GetArrowDataType(field_meta.get_data_type());
    }
}

std::vector<GroupByArrowInfo>
ResolveGroupByArrowInfos(milvus::query::Plan* plan) {
    std::vector<GroupByArrowInfo> infos;
    auto& search_info = plan->plan_node_->search_info_;
    if (!search_info.has_group_by()) {
        return infos;
    }
    AssertInfo(!search_info.group_by_field_ids_.empty(),
               "Go reduce Arrow export has group_by enabled but no group_by "
               "fields");

    infos.reserve(search_info.group_by_field_ids_.size());
    int json_group_by_field_count = 0;
    for (auto field_id : search_info.group_by_field_ids_) {
        GroupByArrowInfo info;
        info.field_id = field_id;
        auto& field_meta = plan->schema_->operator[](info.field_id);
        auto dt = field_meta.get_data_type();
        info.field_data_type = dt;

        if (dt == milvus::DataType::JSON) {
            ++json_group_by_field_count;
            AssertInfo(
                json_group_by_field_count <= 1,
                "Go reduce Arrow export supports at most one JSON group_by "
                "field with the current single json_path/json_type plan, got "
                "{} JSON fields",
                json_group_by_field_count);
            dt = search_info.json_type_.value_or(milvus::DataType::VARCHAR);
            info.field_data_type = dt;
        } else if (dt == milvus::DataType::GEOMETRY) {
            // GEOMETRY stores std::string in the GroupByValueType variant.
            // Map to VARCHAR so the Arrow column is utf8, while metadata
            // preserves the logical type when the proto needs it later.
            dt = milvus::DataType::VARCHAR;
        }

        info.milvus_type = dt;
        info.arrow_type = milvus::GetArrowDataType(info.milvus_type);
        infos.push_back(info);
    }
    return infos;
}

// Build an empty RecordBatch (0 rows) for empty search results.
// The plan provides PK type, group-by metadata, and extra-field metadata.
arrow::Result<std::shared_ptr<arrow::RecordBatch>>
BuildEmptyBatch(milvus::query::Plan* plan,
                const int64_t* extra_field_ids,
                int64_t num_extra_fields,
                bool element_level = false) {
    auto& schema = plan->schema_;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;

    // Determine PK type from schema
    auto pk_field_id = schema->get_primary_field_id();
    auto pk_type = arrow::int64();
    if (pk_field_id.has_value()) {
        auto& pk_meta = schema->operator[](pk_field_id.value());
        if (pk_meta.get_data_type() == milvus::DataType::VARCHAR) {
            pk_type = arrow::utf8();
        }
    }

    // $id
    ARROW_ASSIGN_OR_RAISE(auto id_arr, arrow::MakeEmptyArray(pk_type));
    fields.push_back(arrow::field("$id", pk_type));
    arrays.push_back(id_arr);

    // $score
    ARROW_ASSIGN_OR_RAISE(auto score_arr,
                          arrow::MakeEmptyArray(arrow::float32()));
    fields.push_back(arrow::field("$score", arrow::float32()));
    arrays.push_back(score_arr);

    // $seg_offset
    ARROW_ASSIGN_OR_RAISE(auto offset_arr,
                          arrow::MakeEmptyArray(arrow::int64()));
    fields.push_back(arrow::field("$seg_offset", arrow::int64()));
    arrays.push_back(offset_arr);

    // $group_by_<fieldID> (when group-by is enabled in the plan), kept
    // consistent with non-empty batches so the Arrow stream schema is uniform
    // per segment.
    for (const auto& group_by_info : ResolveGroupByArrowInfos(plan)) {
        ARROW_ASSIGN_OR_RAISE(auto gb_arr,
                              arrow::MakeEmptyArray(group_by_info.arrow_type));
        fields.push_back(MilvusField(GroupByColumnName(group_by_info.field_id),
                                     group_by_info.arrow_type,
                                     true,
                                     group_by_info.field_id,
                                     group_by_info.field_data_type));
        arrays.push_back(gb_arr);
    }

    // $element_indices (when element-level search is active)
    if (element_level) {
        ARROW_ASSIGN_OR_RAISE(auto ei_arr,
                              arrow::MakeEmptyArray(arrow::int32()));
        fields.push_back(arrow::field("$element_indices", arrow::int32()));
        arrays.push_back(ei_arr);
    }

    // Extra fields (e.g., for L0 rerank)
    for (int64_t i = 0; i < num_extra_fields; i++) {
        auto field_id = milvus::FieldId(extra_field_ids[i]);
        auto& field_meta = schema->operator[](field_id);
        auto name = std::string(field_meta.get_name().get());
        // Keep empty extra-field batches aligned with FieldDataToArrow's
        // physical Arrow types. Full GEOMETRY extra-field export will be
        // implemented together with L0 rerank support.
        ARROW_ASSIGN_OR_RAISE(auto arrow_type,
                              EmptyExtraFieldArrowType(field_meta));
        ARROW_ASSIGN_OR_RAISE(auto arr, arrow::MakeEmptyArray(arrow_type));
        fields.push_back(MilvusField(name,
                                     arrow_type,
                                     field_meta.is_nullable(),
                                     field_id,
                                     field_meta.get_data_type()));
        arrays.push_back(arr);
    }

    return arrow::RecordBatch::Make(arrow::schema(fields), 0, arrays);
}

// BuildFixedWidthArray builds an Arrow Array from a fixed-width protobuf repeated field.
template <typename BuilderType, typename DataContainer>
arrow::Result<std::shared_ptr<arrow::Array>>
BuildFixedWidthArray(const DataContainer& data,
                     const milvus::DataArray& field_data,
                     size_t total_valid) {
    AssertInfo(static_cast<size_t>(data.size()) >= total_valid,
               "field data length {} is smaller than expected row count {}",
               data.size(),
               total_valid);
    const bool has_valid_data = field_data.valid_data_size() > 0;
    if (has_valid_data) {
        AssertInfo(
            static_cast<size_t>(field_data.valid_data_size()) == total_valid,
            "valid_data length {} does not match expected row count {}",
            field_data.valid_data_size(),
            total_valid);
    }

    BuilderType builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(total_valid));
    for (size_t i = 0; i < total_valid; ++i) {
        if (has_valid_data && !field_data.valid_data(i)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
            continue;
        }
        builder.UnsafeAppend(data[i]);
    }
    std::shared_ptr<arrow::Array> arr;
    ARROW_RETURN_NOT_OK(builder.Finish(&arr));
    return arr;
}

// BuildVarLenArray builds an Arrow Array from a variable-length protobuf repeated field.
template <typename BuilderType, typename DataContainer>
arrow::Result<std::shared_ptr<arrow::Array>>
BuildVarLenArray(const DataContainer& data,
                 const milvus::DataArray& field_data,
                 size_t total_valid) {
    AssertInfo(static_cast<size_t>(data.size()) >= total_valid,
               "field data length {} is smaller than expected row count {}",
               data.size(),
               total_valid);
    const bool has_valid_data = field_data.valid_data_size() > 0;
    if (has_valid_data) {
        AssertInfo(
            static_cast<size_t>(field_data.valid_data_size()) == total_valid,
            "valid_data length {} does not match expected row count {}",
            field_data.valid_data_size(),
            total_valid);
    }

    BuilderType builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(total_valid));
    for (size_t i = 0; i < total_valid; ++i) {
        if (has_valid_data && !field_data.valid_data(i)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
            continue;
        }
        ARROW_RETURN_NOT_OK(builder.Append(data[i]));
    }
    std::shared_ptr<arrow::Array> arr;
    ARROW_RETURN_NOT_OK(builder.Finish(&arr));
    return arr;
}

// Build the $group_by Arrow array from SearchResult::composite_group_by_values_,
// dispatching on the resolved element type. Each entry in `values` is an
// std::optional<std::variant<monostate, ints..., bool, string>>; entries that
// are nullopt or hold std::monostate become Arrow null values.
template <typename T, typename BuilderType>
arrow::Result<std::shared_ptr<arrow::Array>>
BuildGroupByTypedArray(const std::vector<milvus::GroupByValueType>& values) {
    BuilderType builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(values.size()));
    for (auto& v : values) {
        if (!v.has_value() ||
            std::holds_alternative<std::monostate>(v.value())) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
            continue;
        }
        if constexpr (std::is_same_v<T, std::string>) {
            ARROW_RETURN_NOT_OK(builder.Append(std::get<T>(v.value())));
        } else {
            builder.UnsafeAppend(std::get<T>(v.value()));
        }
    }
    std::shared_ptr<arrow::Array> arr;
    ARROW_RETURN_NOT_OK(builder.Finish(&arr));
    return arr;
}

arrow::Result<std::shared_ptr<arrow::Array>>
BuildGroupByArray(const std::vector<milvus::GroupByValueType>& values,
                  milvus::DataType element_type) {
    switch (element_type) {
        case milvus::DataType::INT8:
            return BuildGroupByTypedArray<int8_t, arrow::Int8Builder>(values);
        case milvus::DataType::INT16:
            return BuildGroupByTypedArray<int16_t, arrow::Int16Builder>(values);
        case milvus::DataType::INT32:
            return BuildGroupByTypedArray<int32_t, arrow::Int32Builder>(values);
        case milvus::DataType::INT64:
        case milvus::DataType::TIMESTAMPTZ:
            return BuildGroupByTypedArray<int64_t, arrow::Int64Builder>(values);
        case milvus::DataType::BOOL:
            return BuildGroupByTypedArray<bool, arrow::BooleanBuilder>(values);
        case milvus::DataType::VARCHAR:
        case milvus::DataType::STRING:
            return BuildGroupByTypedArray<std::string, arrow::StringBuilder>(
                values);
        default:
            return arrow::Status::NotImplemented(
                "unsupported group-by element type in Arrow export");
    }
}

// Convert a protobuf FieldData (scalar) to an Arrow Array + Field.
arrow::Result<
    std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::Array>>>
FieldDataToArrow(const std::string& field_name,
                 const milvus::DataArray& field_data,
                 size_t total_valid) {
    if (!field_data.has_scalars()) {
        return arrow::Status::NotImplemented(
            "non-scalar output field not supported in Arrow export");
    }
    const auto& scalars = field_data.scalars();

    if (scalars.has_bool_data()) {
        ARROW_ASSIGN_OR_RAISE(
            auto arr,
            BuildFixedWidthArray<arrow::BooleanBuilder>(
                scalars.bool_data().data(), field_data, total_valid));
        return std::make_pair(arrow::field(field_name, arrow::boolean()), arr);
    }
    if (scalars.has_int_data()) {
        ARROW_ASSIGN_OR_RAISE(
            auto arr,
            BuildFixedWidthArray<arrow::Int32Builder>(
                scalars.int_data().data(), field_data, total_valid));
        return std::make_pair(arrow::field(field_name, arrow::int32()), arr);
    }
    if (scalars.has_long_data()) {
        ARROW_ASSIGN_OR_RAISE(
            auto arr,
            BuildFixedWidthArray<arrow::Int64Builder>(
                scalars.long_data().data(), field_data, total_valid));
        return std::make_pair(arrow::field(field_name, arrow::int64()), arr);
    }
    if (scalars.has_float_data()) {
        ARROW_ASSIGN_OR_RAISE(
            auto arr,
            BuildFixedWidthArray<arrow::FloatBuilder>(
                scalars.float_data().data(), field_data, total_valid));
        return std::make_pair(arrow::field(field_name, arrow::float32()), arr);
    }
    if (scalars.has_double_data()) {
        ARROW_ASSIGN_OR_RAISE(
            auto arr,
            BuildFixedWidthArray<arrow::DoubleBuilder>(
                scalars.double_data().data(), field_data, total_valid));
        return std::make_pair(arrow::field(field_name, arrow::float64()), arr);
    }
    if (scalars.has_string_data()) {
        ARROW_ASSIGN_OR_RAISE(
            auto arr,
            BuildVarLenArray<arrow::StringBuilder>(
                scalars.string_data().data(), field_data, total_valid));
        return std::make_pair(arrow::field(field_name, arrow::utf8()), arr);
    }
    if (scalars.has_json_data()) {
        ARROW_ASSIGN_OR_RAISE(
            auto arr,
            BuildVarLenArray<arrow::BinaryBuilder>(
                scalars.json_data().data(), field_data, total_valid));
        return std::make_pair(arrow::field(field_name, arrow::binary()), arr);
    }

    return arrow::Status::NotImplemented(
        "unsupported scalar type in Arrow export");
}

// Build Arrow RecordBatch from a SearchResult that has been filtered and had PKs filled.
// extra_fields contains additional field data to include (e.g., for L0 rerank).
// The plan provides schema and group-by metadata.
arrow::Result<std::shared_ptr<arrow::RecordBatch>>
BuildSearchResultBatch(
    SearchResult* search_result,
    milvus::query::Plan* plan,
    const int64_t* extra_field_ids,
    int64_t num_extra_fields,
    const std::map<milvus::FieldId, std::unique_ptr<milvus::DataArray>>&
        extra_fields) {
    auto& schema = plan->schema_;
    auto total_valid = search_result->seg_offsets_.size();

    // Collect fields and arrays
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;

    // Build $id column based on PK type
    {
        if (search_result->pk_type_ == milvus::DataType::INT64) {
            arrow::Int64Builder id_builder;
            ARROW_RETURN_NOT_OK(id_builder.Reserve(total_valid));
            for (size_t i = 0; i < total_valid; ++i) {
                auto& pk = search_result->primary_keys_[i];
                id_builder.UnsafeAppend(std::get<int64_t>(pk));
            }
            std::shared_ptr<arrow::Array> id_array;
            ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
            fields.push_back(arrow::field("$id", arrow::int64()));
            arrays.push_back(id_array);
        } else {
            arrow::StringBuilder id_builder;
            for (size_t i = 0; i < total_valid; ++i) {
                auto& pk = search_result->primary_keys_[i];
                ARROW_RETURN_NOT_OK(
                    id_builder.Append(std::get<std::string>(pk)));
            }
            std::shared_ptr<arrow::Array> id_array;
            ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
            fields.push_back(arrow::field("$id", arrow::utf8()));
            arrays.push_back(id_array);
        }
    }

    // Build $score column (float32)
    {
        arrow::FloatBuilder score_builder;
        ARROW_RETURN_NOT_OK(score_builder.Reserve(total_valid));
        for (size_t i = 0; i < total_valid; ++i) {
            score_builder.UnsafeAppend(search_result->distances_[i]);
        }
        std::shared_ptr<arrow::Array> score_array;
        ARROW_RETURN_NOT_OK(score_builder.Finish(&score_array));
        fields.push_back(arrow::field("$score", arrow::float32()));
        arrays.push_back(score_array);
    }

    // Build $seg_offset column (int64)
    {
        arrow::Int64Builder seg_offset_builder;
        ARROW_RETURN_NOT_OK(seg_offset_builder.Reserve(total_valid));
        for (size_t i = 0; i < total_valid; ++i) {
            seg_offset_builder.UnsafeAppend(search_result->seg_offsets_[i]);
        }
        std::shared_ptr<arrow::Array> seg_offset_array;
        ARROW_RETURN_NOT_OK(seg_offset_builder.Finish(&seg_offset_array));
        fields.push_back(arrow::field("$seg_offset", arrow::int64()));
        arrays.push_back(seg_offset_array);
    }

    // Build $group_by_<fieldID> columns when group-by is enabled. Type is
    // resolved from the plan (with json_type_ fallback for JSON path group-by)
    // so the column schema stays consistent across empty and non-empty batches.
    auto group_by_infos = ResolveGroupByArrowInfos(plan);
    if (!group_by_infos.empty()) {
        AssertInfo(search_result->composite_group_by_values_.has_value(),
                   "plan has group_by_field_ids but SearchResult is missing "
                   "composite_group_by_values_");
        auto& composite = search_result->composite_group_by_values_.value();
        AssertInfo(
            composite.size() == total_valid,
            "composite_group_by_values_ size {} does not match seg_offsets_ "
            "size {}",
            composite.size(),
            total_valid);
        for (const auto& key : composite) {
            AssertInfo(key.Size() == group_by_infos.size(),
                       "group_by value count {} does not match group_by field "
                       "count {}",
                       key.Size(),
                       group_by_infos.size());
        }

        for (size_t field_idx = 0; field_idx < group_by_infos.size();
             ++field_idx) {
            const auto& group_by_info = group_by_infos[field_idx];
            std::vector<milvus::GroupByValueType> field_values;
            field_values.reserve(composite.size());
            for (const auto& key : composite) {
                field_values.push_back(key[field_idx]);
            }
            ARROW_ASSIGN_OR_RAISE(
                auto gb_arr,
                BuildGroupByArray(field_values, group_by_info.milvus_type));
            fields.push_back(
                MilvusField(GroupByColumnName(group_by_info.field_id),
                            group_by_info.arrow_type,
                            true,
                            group_by_info.field_id,
                            group_by_info.field_data_type));
            arrays.push_back(gb_arr);
        }
    }

    // Build $element_indices column when element-level search is active.
    // element_indices_ is int32 and aligned with seg_offsets_ after compaction.
    if (search_result->element_level_) {
        AssertInfo(
            search_result->element_indices_.size() == total_valid,
            "element_indices_ size {} does not match seg_offsets_ size {}",
            search_result->element_indices_.size(),
            total_valid);
        arrow::Int32Builder ei_builder;
        ARROW_RETURN_NOT_OK(ei_builder.Reserve(total_valid));
        for (size_t i = 0; i < total_valid; ++i) {
            ei_builder.UnsafeAppend(search_result->element_indices_[i]);
        }
        std::shared_ptr<arrow::Array> ei_array;
        ARROW_RETURN_NOT_OK(ei_builder.Finish(&ei_array));
        fields.push_back(arrow::field("$element_indices", arrow::int32()));
        arrays.push_back(ei_array);
    }

    // Build extra field columns (e.g., for L0 rerank). Keep this in the
    // caller-requested order so empty and non-empty batches expose the same
    // schema layout.
    if (num_extra_fields > 0) {
        for (int64_t i = 0; i < num_extra_fields; i++) {
            auto field_id = milvus::FieldId(extra_field_ids[i]);
            auto it = extra_fields.find(field_id);
            if (it == extra_fields.end()) {
                return arrow::Status::Invalid(
                    "missing extra field data for requested field id ",
                    field_id.get());
            }
            auto& field_data = it->second;
            auto& field_meta = schema->operator[](field_id);
            auto name = std::string(field_meta.get_name().get());
            auto result = FieldDataToArrow(name, *field_data, total_valid);
            if (!result.ok()) {
                return result.status();
            }
            auto [field, arr] = *result;
            fields.push_back(MilvusField(field->name(),
                                         field->type(),
                                         field_meta.is_nullable(),
                                         field_id,
                                         field_meta.get_data_type()));
            arrays.push_back(arr);
        }
    }

    {
        return arrow::RecordBatch::Make(
            arrow::schema(fields), total_valid, arrays);
    }
}

CStatus
PopulateChunkSizes(SearchResult* search_result,
                   int64_t** out_chunk_sizes,
                   int64_t* out_num_chunks) {
    AssertInfo(out_chunk_sizes != nullptr, "null chunk sizes output");
    AssertInfo(out_num_chunks != nullptr, "null chunk size count output");

    auto nq = search_result->total_nq_;
    auto num_chunks = std::max(nq, int64_t(1));
    ChunkSizesPtr chunk_sizes(
        static_cast<int64_t*>(malloc(sizeof(int64_t) * num_chunks)));
    if (chunk_sizes == nullptr) {
        return milvus::FailureCStatus(milvus::ErrorCode::UnexpectedError,
                                      "failed to allocate Arrow chunk sizes");
    }

    if (nq == 0) {
        chunk_sizes.get()[0] = 0;
    } else if (search_result->topk_per_nq_prefix_sum_.size() ==
               static_cast<size_t>(nq + 1)) {
        const auto& prefix = search_result->topk_per_nq_prefix_sum_;
        for (int64_t i = 0; i < nq; i++) {
            chunk_sizes.get()[i] = prefix[i + 1] - prefix[i];
        }
    } else if (search_result->topk_per_nq_prefix_sum_.empty() &&
               (search_result->unity_topK_ == 0 ||
                search_result->seg_offsets_.empty())) {
        // Some no-hit paths only set total_nq_/unity_topK_ and leave the
        // per-NQ prefix vector empty. Legacy stream export returned one empty
        // batch per NQ for those results; keep the RecordBatch export contract
        // equivalent by reporting zero rows for every NQ.
        for (int64_t i = 0; i < nq; i++) {
            chunk_sizes.get()[i] = 0;
        }
    } else {
        return milvus::FailureCStatus(
            milvus::ErrorCode::UnexpectedError,
            "topk_per_nq_prefix_sum size does not match total_nq");
    }

    *out_chunk_sizes = chunk_sizes.release();
    *out_num_chunks = num_chunks;
    return milvus::SuccessCStatus();
}

CStatus
BuildSearchResultFullBatch(CSearchResult c_search_result,
                           CSearchPlan c_plan,
                           const int64_t* extra_field_ids,
                           int64_t num_extra_fields,
                           std::shared_ptr<arrow::RecordBatch>* out_batch,
                           int64_t** out_chunk_sizes,
                           int64_t* out_num_chunks,
                           const folly::CancellationToken& cancel_token) {
    milvus::futures::throwIfCancelled(cancel_token);
    auto search_result = static_cast<SearchResult*>(c_search_result);
    auto plan = static_cast<milvus::query::Plan*>(c_plan);
    AssertInfo(search_result != nullptr, "null search result");
    AssertInfo(plan != nullptr, "null search plan");
    AssertInfo(out_batch != nullptr, "null Arrow RecordBatch output");
    AssertInfo(out_chunk_sizes != nullptr, "null chunk sizes output");
    AssertInfo(out_num_chunks != nullptr, "null chunk size count output");
    *out_chunk_sizes = nullptr;
    *out_num_chunks = 0;

    auto export_chunk_sizes = [&]() -> CStatus {
        return PopulateChunkSizes(
            search_result, out_chunk_sizes, out_num_chunks);
    };

    if (search_result->unity_topK_ == 0 ||
        search_result->seg_offsets_.empty()) {
        auto empty_batch_result =
            BuildEmptyBatch(plan,
                            extra_field_ids,
                            num_extra_fields,
                            search_result->element_level_);
        if (!empty_batch_result.ok()) {
            return milvus::FailureCStatus(
                milvus::ErrorCode::UnexpectedError,
                empty_batch_result.status().ToString());
        }
        *out_batch = *empty_batch_result;
        return export_chunk_sizes();
    }

    AssertInfo(search_result->topk_per_nq_prefix_sum_.size() ==
                   static_cast<size_t>(search_result->total_nq_ + 1),
               "topk_per_nq_prefix_sum_ size {} does not match total_nq {}",
               search_result->topk_per_nq_prefix_sum_.size(),
               search_result->total_nq_);

    if (search_result->get_total_result_count() == 0) {
        auto empty_batch_result =
            BuildEmptyBatch(plan,
                            extra_field_ids,
                            num_extra_fields,
                            search_result->element_level_);
        if (!empty_batch_result.ok()) {
            return milvus::FailureCStatus(
                milvus::ErrorCode::UnexpectedError,
                empty_batch_result.status().ToString());
        }
        *out_batch = *empty_batch_result;
        return export_chunk_sizes();
    }

    auto segment = static_cast<milvus::segcore::SegmentInternalInterface*>(
        search_result->segment_);

    milvus::futures::throwIfCancelled(cancel_token);
    { milvus::segcore::SortEqualScoresByPks(search_result); }

    std::map<milvus::FieldId, std::unique_ptr<milvus::DataArray>> extra_fields;
    if (num_extra_fields > 0 && extra_field_ids != nullptr &&
        search_result->get_total_result_count() > 0) {
        auto size = search_result->seg_offsets_.size();
        milvus::OpContext op_ctx(cancel_token);
        auto storage_start = std::chrono::steady_clock::now();
        for (int64_t i = 0; i < num_extra_fields; i++) {
            milvus::futures::throwIfCancelled(cancel_token);
            auto field_id = milvus::FieldId(extra_field_ids[i]);
            auto field_data = segment->bulk_subscript(
                &op_ctx, field_id, search_result->seg_offsets_.data(), size);
            extra_fields[field_id] = std::move(field_data);
        }
        auto cold_bytes = op_ctx.storage_usage.scanned_cold_bytes.load();
        search_result->search_storage_cost_.scanned_remote_bytes +=
            cold_bytes;
        search_result->search_storage_cost_.scanned_total_bytes +=
            op_ctx.storage_usage.scanned_total_bytes.load();
        if (cold_bytes > 0) {
            search_result->storage_profile_.ObserveRead(
                std::chrono::steady_clock::now() - storage_start,
                static_cast<uint64_t>(cold_bytes));
        }
    }

    milvus::futures::throwIfCancelled(cancel_token);
    auto batch_result = BuildSearchResultBatch(
        search_result, plan, extra_field_ids, num_extra_fields, extra_fields);
    if (!batch_result.ok()) {
        return milvus::FailureCStatus(milvus::ErrorCode::UnexpectedError,
                                      batch_result.status().ToString());
    }
    *out_batch = *batch_result;
    return export_chunk_sizes();
}

}  // namespace

CStatus
ExportSearchResultAsArrowRecordBatch(CSearchResult c_search_result,
                                     CSearchPlan c_plan,
                                     const int64_t* extra_field_ids,
                                     int64_t num_extra_fields,
                                     ArrowSchema* out_schema,
                                     ArrowArray* out_array,
                                     int64_t** out_chunk_sizes,
                                     int64_t* out_num_chunks,
                                     void* cancellation_source) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(out_schema != nullptr, "null ArrowSchema output");
        AssertInfo(out_array != nullptr, "null ArrowArray output");
        AssertInfo(out_schema->release == nullptr,
                   "ArrowSchema output must be empty before export");
        AssertInfo(out_array->release == nullptr,
                   "ArrowArray output must be empty before export");
        auto cancel_token = folly::CancellationToken();
        if (cancellation_source != nullptr) {
            auto source =
                static_cast<folly::CancellationSource*>(cancellation_source);
            cancel_token = source->getToken();
        }
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = BuildSearchResultFullBatch(c_search_result,
                                                 c_plan,
                                                 extra_field_ids,
                                                 num_extra_fields,
                                                 &batch,
                                                 out_chunk_sizes,
                                                 out_num_chunks,
                                                 cancel_token);
        if (status.error_code != 0) {
            return status;
        }
        auto export_status =
            arrow::ExportRecordBatch(*batch, out_array, out_schema);
        if (!export_status.ok()) {
            ReleaseArrowArrayIfNeeded(out_array);
            ReleaseArrowSchemaIfNeeded(out_schema);
            if (out_chunk_sizes != nullptr && *out_chunk_sizes != nullptr) {
                ChunkSizesPtr chunk_sizes_guard(*out_chunk_sizes);
                *out_chunk_sizes = nullptr;
            }
            if (out_num_chunks != nullptr) {
                *out_num_chunks = 0;
            }
            return milvus::FailureCStatus(milvus::ErrorCode::UnexpectedError,
                                          export_status.ToString());
        }
        return milvus::SuccessCStatus();
    } catch (folly::FutureCancellation& e) {
        return milvus::FailureCStatus(milvus::ErrorCode::FollyCancel, e.what());
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
FillOutputFieldsOrderedImpl(CSearchResult* search_results,
                            int64_t num_search_results,
                            CSearchPlan c_plan,
                            const int32_t* result_seg_indices,
                            const int64_t* result_seg_offsets,
                            int64_t total_rows,
                            CProto* out_result,
                            const folly::CancellationToken& cancel_token) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertEmptyCProto(out_result);
        auto plan = static_cast<milvus::query::Plan*>(c_plan);
        milvus::OpContext op_ctx(cancel_token);

        if (plan->target_entries_.empty()) {
            return milvus::SuccessCStatus();
        }

        // When total_rows is 0 but target_entries_ is non-empty, produce a
        // SearchResultData proto with properly typed but empty FieldData
        // entries for each output field. The delegator-side reduce
        // (ReduceSearchResultData) uses searchResultData[0].FieldsData
        // length to initialize the output structure; if we return nullptr
        // here, the output FieldsData has 0 entries, causing an
        // index-out-of-range panic when merging with non-empty results.
        if (total_rows == 0) {
            auto& schema = plan->schema_;
            auto result_data =
                std::make_unique<milvus::proto::schema::SearchResultData>();
            for (auto& field_id : plan->target_entries_) {
                auto& field_meta = schema->operator[](field_id);
                auto field_data =
                    field_meta.is_vector()
                        ? milvus::segcore::CreateEmptyVectorDataArray(
                              0, field_meta)
                        : milvus::segcore::CreateEmptyScalarDataArray(
                              0, field_meta);
                field_data->set_field_name(field_meta.get_name().get());
                SetFieldDataElementTypeIfNeeded(field_data.get(), field_meta);
                result_data->mutable_fields_data()->AddAllocated(
                    field_data.release());
            }
            return SerializeSearchResultDataToCProto(
                *result_data,
                out_result,
                "failed to allocate for empty field data proto",
                "failed to serialize empty field data proto");
        }

        std::unordered_map<int32_t, std::vector<std::pair<int64_t, int64_t>>>
            seg_groups;
        for (int64_t i = 0; i < total_rows; i++) {
            seg_groups[result_seg_indices[i]].emplace_back(
                i, result_seg_offsets[i]);
        }

        struct SegResult {
            SearchResult temp_result;
            std::vector<int64_t> result_positions;
        };
        std::unordered_map<int32_t, SegResult> seg_results;

        for (auto& [seg_idx, pairs] : seg_groups) {
            AssertInfo(seg_idx >= 0 && seg_idx < num_search_results,
                       "seg_idx {} out of range [0, {})",
                       seg_idx,
                       num_search_results);
            auto sr = static_cast<SearchResult*>(search_results[seg_idx]);

            auto& seg_res = seg_results[seg_idx];
            seg_res.temp_result.segment_ = sr->segment_;
            seg_res.result_positions.reserve(pairs.size());

            for (auto& [pos, offset] : pairs) {
                seg_res.temp_result.seg_offsets_.push_back(offset);
                seg_res.result_positions.push_back(pos);
            }
            seg_res.temp_result.distances_.resize(pairs.size(), 0.0f);
        }

        if (seg_results.size() > 1) {
            auto& pool = milvus::ThreadPools::GetThreadPool(
                milvus::ThreadPoolPriority::MIDDLE);
            std::vector<std::future<void>> futures;
            futures.reserve(seg_results.size());
            for (auto& [seg_idx, seg_res] : seg_results) {
                auto sr = static_cast<SearchResult*>(search_results[seg_idx]);
                auto segment =
                    static_cast<milvus::segcore::SegmentInternalInterface*>(
                        sr->segment_);
                auto* seg_res_ptr = &seg_res;
                futures.emplace_back(
                    pool.Submit([segment, plan, seg_res_ptr, &op_ctx] {
                        segment->FillTargetEntry(
                            plan, seg_res_ptr->temp_result, &op_ctx);
                    }));
            }
            auto futures_guard = folly::makeGuard([&futures]() {
                for (auto& f : futures) {
                    if (f.valid()) {
                        try {
                            f.get();
                        } catch (...) {
                        }
                    }
                }
            });
            for (auto& future : futures) {
                future.get();
            }
        } else if (seg_results.size() == 1) {
            auto& [seg_idx, seg_res] = *seg_results.begin();
            auto sr = static_cast<SearchResult*>(search_results[seg_idx]);
            auto segment =
                static_cast<milvus::segcore::SegmentInternalInterface*>(
                    sr->segment_);
            segment->FillTargetEntry(plan, seg_res.temp_result, &op_ctx);
        }

        // Write storage cost back to original search results
        for (auto& [seg_idx, seg_res] : seg_results) {
            auto sr = static_cast<SearchResult*>(search_results[seg_idx]);
            sr->search_storage_cost_.scanned_remote_bytes +=
                seg_res.temp_result.search_storage_cost_.scanned_remote_bytes;
            sr->search_storage_cost_.scanned_total_bytes +=
                seg_res.temp_result.search_storage_cost_.scanned_total_bytes;
            sr->storage_profile_.Merge(seg_res.temp_result.storage_profile_);
        }

        std::vector<milvus::segcore::MergeBase> result_pairs(total_rows);
        for (auto& [seg_idx, seg_res] : seg_results) {
            for (size_t i = 0; i < seg_res.result_positions.size(); i++) {
                auto pos = seg_res.result_positions[i];
                result_pairs[pos] = {&seg_res.temp_result.output_fields_data_,
                                     i};
            }
        }

        // For nullable vector fields, FillTargetEntry compacts the vector
        // buffer via FilterVectorValidOffsets (null rows dropped), while the
        // valid_data bitmap keeps its logical length. MergeDataArray reads
        // vectors at physical_offset = getValidDataOffset(), which falls back
        // to the logical offset unless we set it. Compute the per-row physical
        // offset = count of valid rows preceding this one, mirroring the
        // logic in master's reduce/Reduce.cpp.
        for (auto& [seg_idx, seg_res] : seg_results) {
            for (auto field_id : plan->target_entries_) {
                auto& field_meta = plan->schema_->operator[](field_id);
                if (!field_meta.is_vector() || !field_meta.is_nullable()) {
                    continue;
                }
                auto it =
                    seg_res.temp_result.output_fields_data_.find(field_id);
                if (it == seg_res.temp_result.output_fields_data_.end()) {
                    continue;
                }
                auto& field_data = it->second;
                if (field_data->valid_data_size() == 0) {
                    continue;
                }
                int64_t valid_idx = 0;
                for (size_t i = 0; i < seg_res.result_positions.size(); i++) {
                    auto pos = seg_res.result_positions[i];
                    result_pairs[pos].setValidDataOffset(field_id, valid_idx);
                    if (field_data->valid_data(i)) {
                        valid_idx++;
                    }
                }
            }
        }

        auto result_data =
            std::make_unique<milvus::proto::schema::SearchResultData>();
        for (auto field_id : plan->target_entries_) {
            auto& field_meta = plan->schema_->operator[](field_id);
            auto field_data =
                milvus::segcore::MergeDataArray(result_pairs, field_meta);
            SetFieldDataElementTypeIfNeeded(field_data.get(), field_meta);
            result_data->mutable_fields_data()->AddAllocated(
                field_data.release());
        }

        return SerializeSearchResultDataToCProto(
            *result_data,
            out_result,
            "failed to allocate memory for proto serialization",
            "failed to serialize SearchResultData proto");
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
FillOutputFieldsOrdered(CSearchResult* search_results,
                        int64_t num_search_results,
                        CSearchPlan c_plan,
                        const int32_t* result_seg_indices,
                        const int64_t* result_seg_offsets,
                        int64_t total_rows,
                        CProto* out_result,
                        void* cancellation_source) {
    if (cancellation_source != nullptr) {
        auto source =
            static_cast<folly::CancellationSource*>(cancellation_source);
        return FillOutputFieldsOrderedImpl(search_results,
                                           num_search_results,
                                           c_plan,
                                           result_seg_indices,
                                           result_seg_offsets,
                                           total_rows,
                                           out_result,
                                           source->getToken());
    }

    return FillOutputFieldsOrderedImpl(search_results,
                                       num_search_results,
                                       c_plan,
                                       result_seg_indices,
                                       result_seg_offsets,
                                       total_rows,
                                       out_result,
                                       folly::CancellationToken());
}

void
GetSearchResultMetadata(CSearchResult c_search_result,
                        bool* has_group_by,
                        int64_t* group_size,
                        int64_t* scanned_remote_bytes,
                        int64_t* scanned_total_bytes) {
    auto search_result = static_cast<SearchResult*>(c_search_result);
    *has_group_by = search_result->composite_group_by_values_.has_value();
    *group_size = search_result->group_size_.value_or(0);
    *scanned_remote_bytes =
        search_result->search_storage_cost_.scanned_remote_bytes;
    *scanned_total_bytes =
        search_result->search_storage_cost_.scanned_total_bytes;
}

void
GetSearchResultStorageProfile(CSearchResult c_search_result,
                              uint64_t* durations,
                              int64_t duration_capacity,
                              int64_t* out_count,
                              uint64_t* completed_bytes,
                              uint64_t* dropped_observations) {
    auto search_result = static_cast<SearchResult*>(c_search_result);
    if (search_result == nullptr || out_count == nullptr ||
        completed_bytes == nullptr || dropped_observations == nullptr) {
        return;
    }
    auto count = std::min<int64_t>(
        search_result->storage_profile_.read_count,
        std::max<int64_t>(duration_capacity, 0));
    if (durations != nullptr) {
        for (int64_t i = 0; i < count; ++i) {
            durations[i] =
                search_result->storage_profile_.read_duration_nanos[i];
        }
    }
    *out_count = count;
    *completed_bytes =
        search_result->storage_profile_.read_completed_bytes;
    *dropped_observations =
        search_result->storage_profile_.dropped_read_observations +
        (search_result->storage_profile_.read_count - count);
}

CStatus
PrepareSearchResultsForExportImpl(
    CTraceContext c_trace,
    CSearchPlan c_plan,
    CPlaceholderGroup c_placeholder_group,
    CSearchResult* c_search_results,
    int64_t num_segments,
    int64_t* slice_nqs,
    int64_t num_slices,
    int64_t* slice_topKs,
    int64_t* all_search_count,
    const folly::CancellationToken& cancel_token) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(num_segments > 0, "num_segments must be greater than 0");
        AssertInfo(num_slices > 0, "num_slices must be greater than 0");

        auto plan = static_cast<milvus::query::Plan*>(c_plan);
        auto placeholder_group =
            static_cast<const milvus::query::PlaceholderGroup*>(
                c_placeholder_group);
        auto trace_ctx = milvus::tracer::TraceContext{
            c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
        std::vector<milvus::SearchResult*> search_results;
        search_results.reserve(num_segments);
        for (int64_t i = 0; i < num_segments; ++i) {
            AssertInfo(c_search_results[i] != nullptr,
                       "null search result at index {}",
                       i);
            search_results.push_back(
                static_cast<milvus::SearchResult*>(c_search_results[i]));
        }

        milvus::OpContext op_ctx(cancel_token);
        milvus::segcore::ReduceHelper helper(search_results,
                                             plan,
                                             placeholder_group,
                                             slice_nqs,
                                             slice_topKs,
                                             num_slices,
                                             &trace_ctx,
                                             &op_ctx);
        helper.PreReduce();
        if (all_search_count != nullptr) {
            *all_search_count = helper.GetAllSearchCount();
        }
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
PrepareSearchResultsForExport(CTraceContext c_trace,
                              CSearchPlan c_plan,
                              CPlaceholderGroup c_placeholder_group,
                              CSearchResult* c_search_results,
                              int64_t num_segments,
                              int64_t* slice_nqs,
                              int64_t num_slices,
                              int64_t* slice_topKs,
                              int64_t* all_search_count,
                              void* cancellation_source) {
    if (cancellation_source != nullptr) {
        auto source =
            static_cast<folly::CancellationSource*>(cancellation_source);
        return PrepareSearchResultsForExportImpl(c_trace,
                                                 c_plan,
                                                 c_placeholder_group,
                                                 c_search_results,
                                                 num_segments,
                                                 slice_nqs,
                                                 num_slices,
                                                 slice_topKs,
                                                 all_search_count,
                                                 source->getToken());
    }

    return PrepareSearchResultsForExportImpl(c_trace,
                                             c_plan,
                                             c_placeholder_group,
                                             c_search_results,
                                             num_segments,
                                             slice_nqs,
                                             num_slices,
                                             slice_topKs,
                                             all_search_count,
                                             folly::CancellationToken());
}
