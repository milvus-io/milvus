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

#include "segment/Types.h"
#include "storage/FSHandler.h"
#include "utils/Status.h"

namespace milvus {
namespace segment {

class SegmentReader {
 public:
    explicit SegmentReader(const std::string& directory);

    // TODO(zhiru)
    Status
    LoadCache(bool& in_cache);

    Status
    Load();

    Status
    LoadVectors(off_t offset, size_t num_bytes, std::vector<uint8_t>& raw_vectors);

    Status
    LoadUids(std::vector<doc_id_t>& uids);

    Status
    LoadVectorIndex(const std::string& location, segment::VectorIndexPtr& vector_index_ptr);

    Status
    LoadBloomFilter(segment::IdBloomFilterPtr& id_bloom_filter_ptr);

    Status
    LoadDeletedDocs(segment::DeletedDocsPtr& deleted_docs_ptr);

    Status
    GetSegment(SegmentPtr& segment_ptr);

    Status
    ReadDeletedDocsSize(size_t& size);

 private:
    storage::FSHandlerPtr fs_ptr_;
    SegmentPtr segment_ptr_;
};

using SegmentReaderPtr = std::shared_ptr<SegmentReader>;

}  // namespace segment
}  // namespace milvus
