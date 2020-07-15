// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <src/db/meta/MetaTypes.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/SnapshotVisitor.h"
#include "segment/SSSegmentReader.h"
#include "segment/Segment.h"
#include "storage/FSHandler.h"
#include "utils/Status.h"

namespace milvus {
namespace segment {

class SSSegmentWriter {
 public:
    explicit SSSegmentWriter(const engine::SegmentVisitorPtr& segment_visitor);

    Status
    AddChunk(const engine::DataChunkPtr& chunk_ptr);

    Status
    AddChunk(const engine::DataChunkPtr& chunk_ptr, uint64_t from, uint64_t to);

    Status
    WriteBloomFilter(const std::string& file_path, const IdBloomFilterPtr& bloom_filter_ptr);

    Status
    WriteDeletedDocs(const std::string& file_path, const DeletedDocsPtr& deleted_docs);

    Status
    Serialize();

    Status
    GetSegment(engine::SegmentPtr& segment_ptr);

    Status
    Merge(const SSSegmentReaderPtr& segment_to_merge);

    size_t
    Size();

    size_t
    RowCount();

    Status
    SetVectorIndex(const std::string& field_name, const knowhere::VecIndexPtr& index);

    Status
    WriteVectorIndex(const std::string& field_name, const std::string& file_path);

 private:
    Status
    Initialize();

    Status
    WriteField(const std::string& file_path, const engine::FIXED_FIELD_DATA& raw);

    Status
    WriteBloomFilter(const std::string& file_path);

    Status
    WriteDeletedDocs(const std::string& file_path);

 private:
    engine::SegmentVisitorPtr segment_visitor_;
    storage::FSHandlerPtr fs_ptr_;
    engine::SegmentPtr segment_ptr_;
};

using SSSegmentWriterPtr = std::shared_ptr<SSSegmentWriter>;

}  // namespace segment
}  // namespace milvus
