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
#include <memory>
#include <span>
#include <vector>

#include "arrow/array/array_nested.h"

#include "common/Types.h"
#include "pb/schema.pb.h"
#include "storage/MmapChunkManager.h"

namespace milvus {

class ArrayValue;
class ColumnarArrayChunk;

// Count logical leaf elements in one scalar ARRAY row, including null leaves.
size_t
GetLeafElementCount(const ScalarFieldProto& row, DataType data_type);

// Copy native-list rows directly into owning ArrayValues without materializing
// intermediate ScalarFieldProto rows.
std::vector<ArrayValue>
ArrowListToArrayValues(
    const arrow::ListArray& rows,
    std::shared_ptr<const proto::schema::TypeSchema> type);

// Materialize Arrow native-list rows using the same validity and placeholder
// rules as ColumnarArrayChunk::OutputRange.
std::vector<ScalarFieldProto>
ArrowListToScalarFieldProto(const arrow::ListArray& rows,
                            const proto::schema::TypeSchema& type);

// Builds one immutable columnar recursive ARRAY block directly in growing mmap
// storage. The returned Chunk tree is a read-only view over the allocation;
// the descriptor must outlive the returned Chunk.
std::shared_ptr<const ColumnarArrayChunk>
CreateMmapColumnarArrayChunkFromProtoRows(
    std::span<const ScalarFieldProto* const> rows,
    const proto::schema::TypeSchema& type,
    const storage::MmapChunkDescriptorPtr& mmap_descriptor);

// Builds one columnar recursive ARRAY block from existing heap-backed
// ArrayValues.
// An empty valid_data span means all rows are valid; otherwise it contains one
// byte per row.
std::shared_ptr<const ColumnarArrayChunk>
CreateMmapColumnarArrayChunkFromValues(
    std::span<const ArrayValue> values,
    std::span<const uint8_t> valid_data,
    const storage::MmapChunkDescriptorPtr& mmap_descriptor);

}  // namespace milvus
