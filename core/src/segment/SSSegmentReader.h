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
#include <vector>

#include "codecs/Codec.h"
#include "db/SnapshotVisitor.h"
#include "segment/Types.h"
#include "storage/FSHandler.h"
#include "utils/Status.h"

namespace milvus {
namespace segment {

class SSSegmentReader {
 public:
    explicit SSSegmentReader(const std::string& dir_root, const engine::SegmentVisitorPtr& segment_visitor);

    // TODO(zhiru)
    Status
    LoadCache(bool& in_cache);

    Status
    Load();

    Status
    LoadVectors(const std::string& file_path, off_t offset, size_t num_bytes, std::vector<uint8_t>& raw_vectors);

    Status
    LoadAttrs(const std::string& field_name, off_t offset, size_t num_bytes, std::vector<uint8_t>& raw_attrs);

    Status
    LoadUids(const std::string& file_path, std::vector<doc_id_t>& uids);

    Status
    LoadVectorIndex(const std::string& location, codec::ExternalData external_data,
                    segment::VectorIndexPtr& vector_index_ptr);

    Status
    LoadBloomFilter(const std::string file_path, segment::IdBloomFilterPtr& id_bloom_filter_ptr);

    Status
    LoadDeletedDocs(const std::string& file_path, segment::DeletedDocsPtr& deleted_docs_ptr);

    Status
    GetSegment(SegmentPtr& segment_ptr);

    Status
    ReadDeletedDocsSize(size_t& size);

 private:
    engine::SegmentVisitorPtr segment_visitor_;
    storage::FSHandlerPtr fs_ptr_;
    SegmentPtr segment_ptr_;
    std::string dir_root_;
};

using SSSegmentReaderPtr = std::shared_ptr<SSSegmentReader>;

}  // namespace segment
}  // namespace milvus
