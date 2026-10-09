// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <vector>

#include "arrow/array/array_nested.h"

#include "common/Types.h"
#include "pb/schema.pb.h"

namespace milvus {

class ArrayValue;
class ColumnarArrayChunk;

// Count logical leaf elements in one scalar ARRAY row, including null leaves.
size_t
GetLeafElementCount(const ScalarFieldProto& row, DataType data_type);

// Copy native-list rows directly into owning ArrayValues without materializing
// intermediate ScalarFieldProto rows.
std::vector<ArrayValue>
ArrowListToArrayValues(const arrow::ListArray& rows,
                       std::shared_ptr<const proto::schema::TypeSchema> type);

// Materialize Arrow native-list rows using the same validity and placeholder
// rules as ColumnarArrayChunk::OutputRange.
std::vector<ScalarFieldProto>
ArrowListToScalarFieldProto(const arrow::ListArray& rows,
                            const proto::schema::TypeSchema& type);

// Returns `bytes` bytes for one serialized block. The caller owns the memory
// and must keep it in place for longer than the Chunk built over it.
using ColumnarArrayBlockAllocator = std::function<char*(size_t bytes)>;

// Builds one immutable columnar recursive ARRAY block from growing rows. The
// block is written into one allocation from `allocate`, and the returned
// Chunk tree is a read-only view over it. The Chunk shares `type` rather than
// copying it, so every block of a column holds the same immutable object.
std::shared_ptr<const ColumnarArrayChunk>
CreateColumnarArrayChunkFromProtoRows(
    std::span<const ScalarFieldProto* const> rows,
    std::shared_ptr<const proto::schema::TypeSchema> type,
    const ColumnarArrayBlockAllocator& allocate);

// Same, from existing heap-backed ArrayValues; the block shares the type of
// the first value. An empty valid_data span means all rows are valid;
// otherwise it contains one byte per row.
std::shared_ptr<const ColumnarArrayChunk>
CreateColumnarArrayChunkFromValues(std::span<const ArrayValue> values,
                                   std::span<const uint8_t> valid_data,
                                   const ColumnarArrayBlockAllocator& allocate);

}  // namespace milvus
