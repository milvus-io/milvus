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
#pragma once

#include <folly/io/IOBuf.h>
#include <sys/mman.h>
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "common/Array.h"
#include "common/Common.h"
#include "common/EasyAssert.h"
#include "common/File.h"
#include "common/FieldMeta.h"
#include "common/FieldData.h"
#include "common/Span.h"
#include "fmt/format.h"
#include "log/Log.h"
#include "mmap/Utils.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Array.h"
#include "knowhere/dataset.h"
#include "monitor/prometheus_client.h"
#include "storage/MmapChunkManager.h"

namespace milvus {

constexpr size_t DEFAULT_PK_VRCOL_BLOCK_SIZE = 1;
constexpr size_t DEFAULT_MEM_VRCOL_BLOCK_SIZE = 32;
constexpr size_t DEFAULT_MMAP_VRCOL_BLOCK_SIZE = 256;

/**
 * ColumnBase and its subclasses are designed to store and retrieve the raw data
 * of a field.
 *
 * It has 3 types of constructors corresponding to 3 MappingTypes:
 *
 * 1. MAP_WITH_ANONYMOUS: ColumnBase(size_t reserve_size, const FieldMeta& field_meta)
 *
 *   This is used when we store the entire data in memory. Upon return, a piece
 *   of unwritten memory is allocated and the caller can fill the memory with data by
 *   calling AppendBatch/Append.
 *
 * 2. MAP_WITH_FILE: ColumnBase(const File& file, size_t size, const FieldMeta& field_meta)
 *
 *   This is used when the raw data has already been written into a file, and we
 *   simply mmap the file to memory and interpret the memory as a column. In this
 *   mode, since the data is already in the file/mmapped memory, calling AppendBatch
 *   and Append is not allowed.
 *
 * 3. MAP_WITH_MANAGER: ColumnBase(size_t reserve,
 *                                 const DataType& data_type,
 *                                 storage::MmapChunkManagerPtr mcm,
 *                                 storage::MmapChunkDescriptorPtr descriptor,
 *                                 bool nullable)
 *
 *   This is used when we want to mmap but don't want to download all the data at once.
 *   Instead, we download the data in chunks, cache and mmap each chunk as a single
 *   ColumnBase. Upon return, a piece of unwritten mmaped memory is allocated by the chunk
 *   manager, and the caller should fill the memory with data by calling AppendBatch
 *   and Append.
 *
 * - Types with fixed length can use the Column subclass directly.
 * - Types with variable lengths:
 *   - SparseFloatColumn:
 *     - To store sparse float vectors.
 *     - All 3 modes are supported.
 *   - VariableColumn:
 *      - To store string like types such as VARCHAR and JSON.
 *      - MAP_WITH_MANAGER is not yet supported(as of 2024.09.11).
 *   - ArrayColumn:
 *     - To store ARRAY types.
 *      - MAP_WITH_MANAGER is not yet supported(as of 2024.09.11).
 *
 */
class ColumnBase {
    /**
     * - data_ points at a piece of memory of size data_cap_size_ + padding_.
     *   Based on mapping_type_, such memory can be:
     *   - an anonymous memory region, allocated by mmap(MAP_ANON)
     *   - a file-backed memory region, mapped by mmap(MAP_FILE)
     *   - a memory region managed by MmapChunkManager, allocated by
     *     MmapChunkManager::Allocate()
     *
     * Memory Layout of `data_`:
     *
     * |<--        data_cap_size_         -->|<-- padding_ -->|
     * |<-- data_size_ -->|<-- free space -->|
     *
     * AppendBatch/Append should first check if there's enough space for new data.
     * If not, call ExpandData() to expand the space.
     *
     * - only the first data_cap_size_ bytes can be used to store actual data.
     * - padding at the end is to ensure when all values are empty, we don't try
     *   to allocate/mmap 0 bytes memory, which will cause mmap() to fail.
     * - data_size_ is the number of bytes currently used to store actual data.
     * - num_rows_ is the number of rows currently stored.
     * - valid_data_ is a FixedVector<bool> indicating whether each element is
     *   not null. it is only used when nullable is true.
     * - nullable_ is true if null(0 byte) is a valid value for the column.
     *
     */
 public:
    virtual size_t
    DataByteSize() const = 0;

    virtual const char*
    MmappedData() const = 0;

    virtual void
    AppendBatch(const FieldDataPtr data) = 0;

    virtual const char*
    Data(int chunk_id) const = 0;
};
}  // namespace milvus