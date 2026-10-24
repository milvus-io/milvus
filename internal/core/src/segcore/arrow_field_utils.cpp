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

#include "segcore/arrow_field_utils.h"

#include <arrow/api.h>

#include <algorithm>
#include <string>
#include <utility>

#include "common/EasyAssert.h"
#include "common/Utils.h"

namespace milvus::segcore {

namespace {

constexpr const char* kMilvusFieldIDMetadataKey = "milvus.field_id";

// ProtoBackedBuffer is an arrow::Buffer that borrows memory owned by something
// else -- here, the protobuf DataArray a retrieve result was built into -- and
// keeps that owner alive for exactly as long as Arrow can still reach the
// bytes.
//
// Arrow's own Buffer(data, size) constructor is non-owning, so a buffer built
// that way would dangle the moment the result object went away. Holding the
// owner as a member is what makes the alias safe: ExportRecordBatch's C release
// callback owns the RecordBatch, which owns the ArrayData, which owns this
// buffer, which owns the protobuf storage. Release on the Go side unwinds the
// whole chain.
class ProtoBackedBuffer : public arrow::Buffer {
 public:
    ProtoBackedBuffer(const uint8_t* data, int64_t size, ProtoOwner owner)
        : arrow::Buffer(data, size), owner_(std::move(owner)) {
    }

 private:
    ProtoOwner owner_;
};

// AliasedArray wraps a contiguous, null-free protobuf payload as an Arrow array
// of `type` without copying.
//
// The caller is responsible for having checked that the payload's layout is
// byte-identical to Arrow's value buffer for `type`; this helper only does the
// wrapping. The validity buffer is left null, which is Arrow's representation
// of "no nulls" and is why this is restricted to the null-free case.
arrow::Result<std::shared_ptr<arrow::Array>>
AliasedArray(const std::shared_ptr<arrow::DataType>& type,
             const uint8_t* data,
             int64_t num_rows,
             int64_t num_bytes,
             const ProtoOwner& owner) {
    auto buffer = std::make_shared<ProtoBackedBuffer>(data, num_bytes, owner);
    auto array_data = arrow::ArrayData::Make(
        type, num_rows, {nullptr, std::move(buffer)}, /*null_count=*/0);
    return arrow::MakeArray(array_data);
}

// CanAlias is the single gate for every aliasing branch: an owner must have
// been supplied, and the column must have no validity bitmap. A column that has
// one is compacted by MergeDataArray -- its physical payload holds only the
// valid rows -- so its layout is not Arrow's and the copying path must run.
inline bool
CanAlias(const milvus::DataArray& field_data, const ProtoOwner& owner) {
    return owner != nullptr &&
           milvus::GetFieldDataRowValidData(field_data).empty();
}

// BuildFixedWidthArray builds an Arrow Array from a fixed-width protobuf repeated field.
template <typename BuilderType, typename DataContainer>
arrow::Result<std::shared_ptr<arrow::Array>>
BuildFixedWidthArray(
    const DataContainer& data,
    const milvus::DataArray& field_data,
    size_t total_valid,
    const ProtoOwner& owner = nullptr,
    const std::shared_ptr<arrow::DataType>& alias_type = nullptr) {
    AssertInfo(static_cast<size_t>(data.size()) >= total_valid,
               "field data length {} is smaller than expected row count {}",
               data.size(),
               total_valid);
    // Alias when the protobuf element type IS the Arrow element type. The
    // caller passes alias_type only for those; BOOL (byte-per-element vs
    // bit-packed) and INT8/INT16 (widened to int32 in protobuf) pass nullptr
    // and keep copying.
    if (alias_type != nullptr && CanAlias(field_data, owner)) {
        using Elem = typename DataContainer::value_type;
        return AliasedArray(alias_type,
                            reinterpret_cast<const uint8_t*>(data.data()),
                            static_cast<int64_t>(total_valid),
                            static_cast<int64_t>(total_valid * sizeof(Elem)),
                            owner);
    }
    const auto& valid_data = milvus::GetFieldDataRowValidData(field_data);
    const bool has_valid_data = !valid_data.empty();
    if (has_valid_data) {
        AssertInfo(static_cast<size_t>(valid_data.size()) == total_valid,
                   "valid_data length {} does not match expected row count {}",
                   valid_data.size(),
                   total_valid);
    }

    BuilderType builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(total_valid));
    for (size_t i = 0; i < total_valid; ++i) {
        if (has_valid_data && !valid_data[i]) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
            continue;
        }
        builder.UnsafeAppend(data[i]);
    }
    std::shared_ptr<arrow::Array> arr;
    ARROW_RETURN_NOT_OK(builder.Finish(&arr));
    return arr;
}

// BuildVarLenArray builds an Arrow Array from a variable-length protobuf
// repeated field whose payload has one entry per logical row (including
// placeholders for nulls).  MergeDataArray uses this layout for scalar
// fields (STRING, JSON, GEOMETRY).
template <typename BuilderType, typename DataContainer>
arrow::Result<std::shared_ptr<arrow::Array>>
BuildVarLenArray(const DataContainer& data,
                 const milvus::DataArray& field_data,
                 size_t total_valid) {
    AssertInfo(static_cast<size_t>(data.size()) >= total_valid,
               "field data length {} is smaller than expected row count {}",
               data.size(),
               total_valid);
    const auto& valid_data = milvus::GetFieldDataRowValidData(field_data);
    const bool has_valid_data = !valid_data.empty();
    if (has_valid_data) {
        AssertInfo(static_cast<size_t>(valid_data.size()) == total_valid,
                   "valid_data length {} does not match expected row count {}",
                   valid_data.size(),
                   total_valid);
    }

    BuilderType builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(total_valid));
    // Reserve the VALUE buffer too, not just offsets+validity. builder.Reserve
    // covers the latter only, so without this the value buffer grows by
    // doubling and every varchar/JSON/geometry column pays roughly one extra
    // copy of its own bytes, per segment. The Go counterpart (gatherVarLen)
    // does the same pre-pass for the same reason.
    int64_t value_bytes = 0;
    for (size_t i = 0; i < total_valid; ++i) {
        if (has_valid_data && !valid_data[i]) {
            continue;
        }
        value_bytes += static_cast<int64_t>(data[i].size());
    }
    ARROW_RETURN_NOT_OK(builder.ReserveData(value_bytes));
    for (size_t i = 0; i < total_valid; ++i) {
        if (has_valid_data && !valid_data[i]) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
            continue;
        }
        ARROW_RETURN_NOT_OK(builder.Append(data[i]));
    }
    std::shared_ptr<arrow::Array> arr;
    ARROW_RETURN_NOT_OK(builder.Finish(&arr));
    return arr;
}

// BuildCompactVarLenArray builds an Arrow Array from a variable-length
// protobuf repeated field whose payload is compact — only valid rows have
// physical entries.  MergeDataArray uses this layout for sparse vectors.
template <typename BuilderType, typename DataContainer>
arrow::Result<std::shared_ptr<arrow::Array>>
BuildCompactVarLenArray(const DataContainer& data,
                        const milvus::DataArray& field_data,
                        size_t total_valid) {
    const auto& valid_data = milvus::GetFieldDataRowValidData(field_data);
    const bool has_valid_data = !valid_data.empty();
    if (has_valid_data) {
        AssertInfo(static_cast<size_t>(valid_data.size()) == total_valid,
                   "valid_data length {} does not match expected row count {}",
                   valid_data.size(),
                   total_valid);
    } else {
        AssertInfo(static_cast<size_t>(data.size()) >= total_valid,
                   "field data length {} is smaller than expected row count {}",
                   data.size(),
                   total_valid);
    }

    BuilderType builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(total_valid));
    // Reserve the VALUE buffer too, not just offsets+validity. builder.Reserve
    // covers the latter only, so without this the value buffer grows by
    // doubling and every varchar/JSON/geometry column pays roughly one extra
    // copy of its own bytes, per segment. The Go counterpart (gatherVarLen)
    // does the same pre-pass for the same reason.
    int64_t value_bytes = 0;
    for (int i = 0; i < data.size(); ++i) {
        value_bytes += static_cast<int64_t>(data[i].size());
    }
    ARROW_RETURN_NOT_OK(builder.ReserveData(value_bytes));
    size_t physical = 0;
    for (size_t i = 0; i < total_valid; ++i) {
        if (has_valid_data && !valid_data[i]) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
            continue;
        }
        ARROW_RETURN_NOT_OK(builder.Append(data[physical]));
        ++physical;
    }
    std::shared_ptr<arrow::Array> arr;
    ARROW_RETURN_NOT_OK(builder.Finish(&arr));
    return arr;
}

}  // namespace

CStatus
ArrowExportFailure(const arrow::Status& status) {
    return milvus::FailureCStatus(ArrowExportErrorCode(status),
                                  status.ToString());
}

milvus::ErrorCode
ArrowExportErrorCode(const arrow::Status& status) {
    AssertInfo(!status.ok(), "ArrowExportErrorCode requires a failed status");
    auto code = milvus::ErrorCode::UnexpectedError;
    switch (status.code()) {
        case arrow::StatusCode::OutOfMemory:
            code = milvus::ErrorCode::MemAllocateFailed;
            break;
        case arrow::StatusCode::CapacityError:
            // This is an Arrow representation limit (e.g. 32-bit binary
            // offsets), not allocator pressure. Retrying the same batch
            // cannot help; keep it a permanent system error.
            code = milvus::ErrorCode::OutOfRange;
            break;
        case arrow::StatusCode::NotImplemented:
            code = milvus::ErrorCode::NotImplemented;
            break;
        case arrow::StatusCode::Cancelled:
            code = milvus::ErrorCode::FollyCancel;
            break;
        default:
            // These arrays are assembled by Milvus. Other Arrow failures
            // signal an internal contract violation. Storage/JSON-reader
            // failures are classified at their source and thrown as typed
            // SegcoreError; they must not be stringified through this path.
            break;
    }
    return code;
}

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

arrow::Result<std::shared_ptr<arrow::DataType>>
EmptyExtraFieldArrowType(const milvus::FieldMeta& field_meta,
                         bool preserve_integer_width) {
    switch (field_meta.get_data_type()) {
        case milvus::DataType::BOOL:
            return arrow::boolean();
        case milvus::DataType::INT8:
            return preserve_integer_width ? arrow::int8() : arrow::int32();
        case milvus::DataType::INT16:
            return preserve_integer_width ? arrow::int16() : arrow::int32();
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
            return arrow::binary();
        case milvus::DataType::VECTOR_ARRAY: {
            return milvus::GetArrowDataTypeForVectorArray(
                field_meta.get_element_type(), field_meta.get_dim());
        }
        default: {
            int dim = 1;
            if (field_meta.is_vector() &&
                field_meta.get_data_type() !=
                    milvus::DataType::VECTOR_SPARSE_U32_F32) {
                dim = field_meta.get_dim();
            }
            return milvus::GetArrowDataType(field_meta.get_data_type(), dim);
        }
    }
}

namespace {

// BuildDenseVectorArray builds an Arrow FixedSizeBinary array from a dense
// vector buffer. Nullable vectors produced by MergeDataArray are compacted:
// the physical buffer has only valid_count entries while valid_data has
// total_valid entries. This helper uses separate logical/physical indices
// to avoid reading beyond the buffer.
template <typename RawPtr>
arrow::Result<std::shared_ptr<arrow::Array>>
BuildDenseVectorArray(RawPtr raw,
                      int64_t payload_bytes,
                      int32_t byte_width,
                      const milvus::DataArray& field_data,
                      size_t total_valid,
                      const ProtoOwner& owner = nullptr) {
    auto type = arrow::fixed_size_binary(byte_width);
    // The payload must actually hold the rows we are about to publish. Both
    // branches below do pointer arithmetic off `raw`, and the aliasing branch
    // additionally hands Arrow a Buffer whose declared size would then be a
    // lie -- the over-read surfaces in Go, far from the cause. The nullable
    // branch is compacted, so there the bound is valid_count, not total_valid.
    {
        const auto& vd = milvus::GetFieldDataRowValidData(field_data);
        int64_t needed = static_cast<int64_t>(total_valid);
        if (!vd.empty()) {
            // Same invariant the fixed-width and var-len builders assert: the
            // bitmap describes exactly the rows being published. Without it,
            // counting over a LONGER bitmap would demand more payload than the
            // copy loop below actually reads and could reject a valid query.
            AssertInfo(static_cast<size_t>(vd.size()) == total_valid,
                       "valid_data length {} does not match expected row "
                       "count {}",
                       vd.size(),
                       total_valid);
            needed = std::count(vd.begin(), vd.end(), true);
        }
        AssertInfo(payload_bytes >= needed * byte_width,
                   "dense vector payload is {} bytes but {} rows of width {} "
                   "need {}",
                   payload_bytes,
                   needed,
                   byte_width,
                   needed * byte_width);
    }
    // Dense, null-free and already in Arrow's value-buffer layout -- see the
    // AppendValues comment below, which this supersedes when an owner makes
    // aliasing sound. This is the single biggest win available: on the retrieve
    // path the dominant column is a dense vector, so this removes a full copy
    // of nearly the whole payload.
    if (CanAlias(field_data, owner)) {
        return AliasedArray(type,
                            reinterpret_cast<const uint8_t*>(raw),
                            static_cast<int64_t>(total_valid),
                            static_cast<int64_t>(total_valid) * byte_width,
                            owner);
    }
    arrow::FixedSizeBinaryBuilder builder(type);
    ARROW_RETURN_NOT_OK(builder.Reserve(total_valid));
    const auto& valid_data = milvus::GetFieldDataRowValidData(field_data);
    const bool has_valid_data = !valid_data.empty();

    if (!has_valid_data) {
        // Dense and no nulls: the source is already exactly the Arrow value
        // buffer's layout, so copy it in one call.
        //
        // The per-row loop below is correct but costs a function call, a
        // bounds check and a validity-bitmap append for every row. On the
        // retrieve path the dominant column is a dense float vector, so that
        // was 1000 separate 512-byte memcpys per 1000-row batch instead of
        // one. Measured as the bulk of the Arrow export's deficit against
        // protobuf serialization at the CGO boundary.
        //
        // This cannot be hoisted into the nullable case: MergeDataArray
        // compacts nullable vectors, so there the physical buffer holds only
        // the valid rows and the logical/physical indices diverge.
        ARROW_RETURN_NOT_OK(
            builder.AppendValues(raw, static_cast<int64_t>(total_valid)));
    } else {
        size_t physical = 0;
        for (size_t i = 0; i < total_valid; ++i) {
            if (!valid_data[i]) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
                continue;
            }
            ARROW_RETURN_NOT_OK(builder.Append(raw + physical * byte_width));
            ++physical;
        }
    }
    std::shared_ptr<arrow::Array> arr;
    ARROW_RETURN_NOT_OK(builder.Finish(&arr));
    return arr;
}

}  // namespace

arrow::Result<
    std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::Array>>>
FieldDataToArrow(const std::string& field_name,
                 const milvus::DataArray& field_data,
                 size_t total_valid,
                 bool preserve_integer_width,
                 const ProtoOwner& owner) {
    if (field_data.has_vectors()) {
        const auto& vectors = field_data.vectors();
        int64_t dim = vectors.dim();
        if (vectors.has_float_vector()) {
            const auto& fv = vectors.float_vector().data();
            int32_t byte_width = dim * sizeof(float);
            auto raw = reinterpret_cast<const uint8_t*>(fv.data());
            ARROW_ASSIGN_OR_RAISE(
                auto arr,
                BuildDenseVectorArray(
                    raw,
                    static_cast<int64_t>(fv.size() * sizeof(float)),
                    byte_width,
                    field_data,
                    total_valid,
                    owner));
            return std::make_pair(
                arrow::field(field_name, arrow::fixed_size_binary(byte_width)),
                arr);
        }
        if (vectors.has_binary_vector()) {
            const auto& bv = vectors.binary_vector();
            int32_t byte_width = dim / 8;
            auto raw = reinterpret_cast<const uint8_t*>(bv.data());
            ARROW_ASSIGN_OR_RAISE(
                auto arr,
                BuildDenseVectorArray(raw,
                                      static_cast<int64_t>(bv.size()),
                                      byte_width,
                                      field_data,
                                      total_valid,
                                      owner));
            return std::make_pair(
                arrow::field(field_name, arrow::fixed_size_binary(byte_width)),
                arr);
        }
        if (vectors.has_float16_vector()) {
            const auto& f16v = vectors.float16_vector();
            int32_t byte_width = dim * 2;
            auto raw = reinterpret_cast<const uint8_t*>(f16v.data());
            ARROW_ASSIGN_OR_RAISE(
                auto arr,
                BuildDenseVectorArray(raw,
                                      static_cast<int64_t>(f16v.size()),
                                      byte_width,
                                      field_data,
                                      total_valid,
                                      owner));
            return std::make_pair(
                arrow::field(field_name, arrow::fixed_size_binary(byte_width)),
                arr);
        }
        if (vectors.has_bfloat16_vector()) {
            const auto& bf16v = vectors.bfloat16_vector();
            int32_t byte_width = dim * 2;
            auto raw = reinterpret_cast<const uint8_t*>(bf16v.data());
            ARROW_ASSIGN_OR_RAISE(
                auto arr,
                BuildDenseVectorArray(raw,
                                      static_cast<int64_t>(bf16v.size()),
                                      byte_width,
                                      field_data,
                                      total_valid,
                                      owner));
            return std::make_pair(
                arrow::field(field_name, arrow::fixed_size_binary(byte_width)),
                arr);
        }
        if (vectors.has_int8_vector()) {
            const auto& i8v = vectors.int8_vector();
            int32_t byte_width = dim;
            auto raw = reinterpret_cast<const uint8_t*>(i8v.data());
            ARROW_ASSIGN_OR_RAISE(
                auto arr,
                BuildDenseVectorArray(raw,
                                      static_cast<int64_t>(i8v.size()),
                                      byte_width,
                                      field_data,
                                      total_valid,
                                      owner));
            return std::make_pair(
                arrow::field(field_name, arrow::fixed_size_binary(byte_width)),
                arr);
        }
        if (vectors.has_sparse_float_vector()) {
            ARROW_ASSIGN_OR_RAISE(auto arr,
                                  BuildCompactVarLenArray<arrow::BinaryBuilder>(
                                      vectors.sparse_float_vector().contents(),
                                      field_data,
                                      total_valid));
            return std::make_pair(arrow::field(field_name, arrow::binary()),
                                  arr);
        }
        if (vectors.has_vector_array()) {
            const auto& va = vectors.vector_array();
            int32_t byte_width = 0;
            auto elem_type = va.element_type();
            switch (elem_type) {
                case milvus::proto::schema::FloatVector:
                    byte_width = dim * sizeof(float);
                    break;
                case milvus::proto::schema::BinaryVector:
                    byte_width = dim / 8;
                    break;
                case milvus::proto::schema::Float16Vector:
                case milvus::proto::schema::BFloat16Vector:
                    byte_width = dim * 2;
                    break;
                case milvus::proto::schema::Int8Vector:
                    byte_width = dim;
                    break;
                default:
                    return arrow::Status::NotImplemented(
                        "unsupported VectorArray element type");
            }
            auto value_type = arrow::fixed_size_binary(byte_width);
            auto inner_builder =
                std::make_shared<arrow::FixedSizeBinaryBuilder>(value_type);
            arrow::ListBuilder list_builder(arrow::default_memory_pool(),
                                            inner_builder);
            const auto& valid_data =
                milvus::GetFieldDataRowValidData(field_data);
            const bool has_valid_data = !valid_data.empty();
            for (size_t i = 0; i < total_valid; ++i) {
                if (has_valid_data && !valid_data[i]) {
                    ARROW_RETURN_NOT_OK(list_builder.AppendNull());
                    continue;
                }
                ARROW_RETURN_NOT_OK(list_builder.Append());
                const auto& row = va.data(i);
                const uint8_t* raw = nullptr;
                size_t vec_count = 0;
                switch (elem_type) {
                    case milvus::proto::schema::FloatVector: {
                        const auto& fv = row.float_vector().data();
                        raw = reinterpret_cast<const uint8_t*>(fv.data());
                        vec_count = fv.size() / dim;
                        break;
                    }
                    case milvus::proto::schema::BinaryVector: {
                        const auto& bv = row.binary_vector();
                        raw = reinterpret_cast<const uint8_t*>(bv.data());
                        vec_count = bv.size() / byte_width;
                        break;
                    }
                    case milvus::proto::schema::Float16Vector: {
                        const auto& f16v = row.float16_vector();
                        raw = reinterpret_cast<const uint8_t*>(f16v.data());
                        vec_count = f16v.size() / byte_width;
                        break;
                    }
                    case milvus::proto::schema::BFloat16Vector: {
                        const auto& bf16v = row.bfloat16_vector();
                        raw = reinterpret_cast<const uint8_t*>(bf16v.data());
                        vec_count = bf16v.size() / byte_width;
                        break;
                    }
                    case milvus::proto::schema::Int8Vector: {
                        const auto& i8v = row.int8_vector();
                        raw = reinterpret_cast<const uint8_t*>(i8v.data());
                        vec_count = i8v.size() / byte_width;
                        break;
                    }
                    default:
                        break;
                }
                for (size_t v = 0; v < vec_count; ++v) {
                    ARROW_RETURN_NOT_OK(
                        inner_builder->Append(raw + v * byte_width));
                }
            }
            std::shared_ptr<arrow::Array> arr;
            ARROW_RETURN_NOT_OK(list_builder.Finish(&arr));
            return std::make_pair(
                arrow::field(field_name, arrow::list(value_type)), arr);
        }
        return arrow::Status::NotImplemented(
            "unsupported vector type in Arrow export");
    }

    if (!field_data.has_scalars()) {
        return arrow::Status::NotImplemented(
            "non-scalar/non-vector output field not supported in Arrow export");
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
        if (preserve_integer_width &&
            field_data.type() == milvus::proto::schema::DataType::Int8) {
            ARROW_ASSIGN_OR_RAISE(
                auto arr,
                BuildFixedWidthArray<arrow::Int8Builder>(
                    scalars.int_data().data(), field_data, total_valid));
            return std::make_pair(arrow::field(field_name, arrow::int8()), arr);
        }
        if (preserve_integer_width &&
            field_data.type() == milvus::proto::schema::DataType::Int16) {
            ARROW_ASSIGN_OR_RAISE(
                auto arr,
                BuildFixedWidthArray<arrow::Int16Builder>(
                    scalars.int_data().data(), field_data, total_valid));
            return std::make_pair(arrow::field(field_name, arrow::int16()),
                                  arr);
        }
        ARROW_ASSIGN_OR_RAISE(
            auto arr,
            BuildFixedWidthArray<arrow::Int32Builder>(scalars.int_data().data(),
                                                      field_data,
                                                      total_valid,
                                                      owner,
                                                      arrow::int32()));
        return std::make_pair(arrow::field(field_name, arrow::int32()), arr);
    }
    if (scalars.has_long_data()) {
        ARROW_ASSIGN_OR_RAISE(auto arr,
                              BuildFixedWidthArray<arrow::Int64Builder>(
                                  scalars.long_data().data(),
                                  field_data,
                                  total_valid,
                                  owner,
                                  arrow::int64()));
        return std::make_pair(arrow::field(field_name, arrow::int64()), arr);
    }
    if (scalars.has_timestamptz_data()) {
        ARROW_ASSIGN_OR_RAISE(auto arr,
                              BuildFixedWidthArray<arrow::Int64Builder>(
                                  scalars.timestamptz_data().data(),
                                  field_data,
                                  total_valid,
                                  owner,
                                  arrow::int64()));
        return std::make_pair(arrow::field(field_name, arrow::int64()), arr);
    }
    if (scalars.has_float_data()) {
        ARROW_ASSIGN_OR_RAISE(auto arr,
                              BuildFixedWidthArray<arrow::FloatBuilder>(
                                  scalars.float_data().data(),
                                  field_data,
                                  total_valid,
                                  owner,
                                  arrow::float32()));
        return std::make_pair(arrow::field(field_name, arrow::float32()), arr);
    }
    if (scalars.has_double_data()) {
        ARROW_ASSIGN_OR_RAISE(auto arr,
                              BuildFixedWidthArray<arrow::DoubleBuilder>(
                                  scalars.double_data().data(),
                                  field_data,
                                  total_valid,
                                  owner,
                                  arrow::float64()));
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
    if (scalars.has_geometry_data()) {
        ARROW_ASSIGN_OR_RAISE(
            auto arr,
            BuildVarLenArray<arrow::BinaryBuilder>(
                scalars.geometry_data().data(), field_data, total_valid));
        return std::make_pair(arrow::field(field_name, arrow::binary()), arr);
    }

    if (scalars.has_array_data()) {
        const auto& ad = scalars.array_data();
        const auto& valid_data = milvus::GetFieldDataRowValidData(field_data);
        const bool has_valid_data = !valid_data.empty();
        arrow::BinaryBuilder builder;
        ARROW_RETURN_NOT_OK(builder.Reserve(total_valid));
        // Hoisted: SerializeToString clears and reuses the capacity, so after
        // the first row this is allocation-free. Declared inside the loop it
        // cost a construct/destruct, and a malloc/free for anything past the
        // small-string threshold, per row.
        std::string serialized;
        for (size_t i = 0; i < total_valid; ++i) {
            if (has_valid_data && !valid_data[i]) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
                continue;
            }
            if (!ad.data(i).SerializeToString(&serialized)) {
                return arrow::Status::SerializationError(
                    "failed to serialize ArrayArray element");
            }
            ARROW_RETURN_NOT_OK(builder.Append(serialized));
        }
        std::shared_ptr<arrow::Array> arr;
        ARROW_RETURN_NOT_OK(builder.Finish(&arr));
        return std::make_pair(arrow::field(field_name, arrow::binary()), arr);
    }

    return arrow::Status::NotImplemented(
        "unsupported scalar type in Arrow export");
}

}  // namespace milvus::segcore
