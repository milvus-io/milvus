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

#pragma once

#include <arrow/type_fwd.h>

#include <memory>
#include <string>
#include <utility>

#include "common/FieldMeta.h"
#include "common/QueryResult.h"
#include "common/Types.h"

namespace milvus::segcore {

inline constexpr const char* kMilvusDataTypeMetadataKey = "milvus.data_type";

// Classify a failed in-memory Arrow conversion/export at the C Data boundary.
// Unlike storage's mapper, Invalid/Type/Index errors here describe internal
// result contracts, not corrupt persisted data or invalid client parameters.
// Helpers below propagate Arrow statuses unchanged; Search/Retrieve C entry
// points call this mapper and contain exceptions with CGoCatch.h.
CStatus
ArrowExportFailure(const arrow::Status& status);

// Classify a failed in-memory Arrow status, for a throw site that needs the
// code rather than a CStatus. Shares its mapping with ArrowExportFailure so the
// two cannot disagree -- notably OutOfMemory -> MemAllocateFailed, which is
// retriable, where a bare AssertInfo would collapse everything to the permanent
// UnexpectedError.
milvus::ErrorCode
ArrowExportErrorCode(const arrow::Status& status);

// Build the Arrow field metadata (field id + Milvus data type) attached to
// every exported Arrow field, so downstream consumers can recover the
// originating Milvus field without relying on column name/order alone.
std::shared_ptr<arrow::KeyValueMetadata>
MilvusFieldMetadata(milvus::FieldId field_id, milvus::DataType data_type);

// Build an arrow::Field carrying MilvusFieldMetadata.
std::shared_ptr<arrow::Field>
MilvusField(const std::string& name,
            const std::shared_ptr<arrow::DataType>& arrow_type,
            bool nullable,
            milvus::FieldId field_id,
            milvus::DataType data_type);

// Resolve the Arrow physical type used to build an empty (0-row) Arrow array
// for this scalar field, without requiring materialized field data.
// Search function chains preserve INT8/INT16 widths; retrieve consumers use
// the protobuf-compatible INT32 representation by default.
arrow::Result<std::shared_ptr<arrow::DataType>>
EmptyExtraFieldArrowType(const milvus::FieldMeta& field_meta,
                         bool preserve_integer_width = false);

// ProtoStorageOwner is the base an object must declare in order to be accepted
// as the keep-alive for aliased protobuf storage.
//
// A shared_ptr<void> would also work and is shorter, but every shared_ptr<T>
// converts to it implicitly, so passing an unrelated object would compile and
// produce a dangling alias. Requiring the type to opt in turns that into a
// compile error.
//
// It cannot rule out the remaining hazard -- an owner that owns the WRONG
// storage -- which is why the only production implementation hands out its own
// columns rather than letting callers pair them up.
class ProtoStorageOwner {
 public:
    virtual ~ProtoStorageOwner() = default;
};

// A keep-alive handle for the protobuf storage an Arrow array may alias. See
// FieldDataToArrow's `owner` parameter.
using ProtoOwner = std::shared_ptr<const ProtoStorageOwner>;

// Convert a protobuf FieldData (scalar or vector) to an Arrow Array + Field.
//
// owner, when non-null, permits the result to ALIAS field_data's protobuf
// buffers instead of copying them -- the dominant cost of this function on
// large results, and a pure waste for the many types whose protobuf payload
// already has Arrow's exact value-buffer layout (every dense vector, and
// INT32/INT64/FLOAT/DOUBLE/TIMESTAMPTZ). The returned arrays then keep `owner` alive, so
// the caller MUST make it own the storage backing field_data and MUST NOT
// mutate or free that storage while any returned array is reachable -- which
// includes after arrow::ExportRecordBatch, since the C release callback owns
// the chain.
//
// Passing nullptr (the default) copies everything, which is what every caller
// that cannot make that guarantee must do.
//
// Aliasing is skipped per column when it would not be sound: nullable columns
// (MergeDataArray compacts them, so the physical layout diverges from Arrow's),
// BOOL (protobuf stores a byte per element, Arrow bit-packs), and INT8/INT16
// under preserve_integer_width (protobuf widens them to int32).
arrow::Result<
    std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::Array>>>
FieldDataToArrow(const std::string& field_name,
                 const milvus::DataArray& field_data,
                 size_t total_valid,
                 bool preserve_integer_width = false,
                 const ProtoOwner& owner = nullptr);

}  // namespace milvus::segcore
