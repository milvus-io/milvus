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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/SnapshotVisitor.h"
#include "db/Types.h"
#include "segment/Segment.h"
#include "segment/SegmentReader.h"
#include "storage/FSHandler.h"
#include "utils/Status.h"

namespace milvus {
namespace segment {

class SegmentWriter {
 public:
    explicit SegmentWriter(const std::string& dir_root, const engine::SegmentVisitorPtr& segment_visitor);

    Status
    AddChunk(const engine::DataChunkPtr& chunk_ptr);

    Status
    AddChunk(const engine::DataChunkPtr& chunk_ptr, int64_t from, int64_t to);

    Status
    WriteBloomFilter(const std::string& file_path, const IdBloomFilterPtr& bloom_filter_ptr);

    Status
    WriteDeletedDocs(const std::string& file_path, const DeletedDocsPtr& deleted_docs);

    Status
    Serialize();

    Status
    Merge(const SegmentReaderPtr& segment_reader);

    size_t
    RowCount();

    Status
    LoadUids(std::vector<engine::idx_t>& uids);

    Status
    SetVectorIndex(const std::string& field_name, const knowhere::VecIndexPtr& index);

    Status
    WriteVectorIndex(const std::string& field_name);

    Status
    SetStructuredIndex(const std::string& field_name, const knowhere::IndexPtr& index);

    Status
    WriteStructuredIndex(const std::string& field_name);

    Status
    GetSegment(engine::SegmentPtr& segment_ptr);

    Status
    GetSegmentID(int64_t& id);

    std::string
    GetSegmentPath();

    std::string
    GetRootPath() const {
        return dir_root_;
    }

    engine::SegmentVisitorPtr
    GetSegmentVisitor() const {
        return segment_visitor_;
    }

 private:
    Status
    Initialize();

    Status
    WriteField(const std::string& file_path, const engine::BinaryDataPtr& raw);

    Status
    WriteFields();

    Status
    WriteBloomFilter();

    Status
    WriteDeletedDocs();

 private:
    engine::SegmentVisitorPtr segment_visitor_;
    storage::FSHandlerPtr fs_ptr_;
    engine::SegmentPtr segment_ptr_;

    std::string dir_root_;
    std::string dir_collections_;
};

using SegmentWriterPtr = std::shared_ptr<SegmentWriter>;

}  // namespace segment
}  // namespace milvus
