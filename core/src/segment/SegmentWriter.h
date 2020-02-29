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
#include "store/Directory.h"
#include "utils/Status.h"

namespace milvus {
namespace segment {

class SegmentWriter {
 public:
    explicit SegmentWriter(const std::string& directory);

    Status
    AddVectors(const std::string& name, const std::vector<uint8_t>& data, const std::vector<doc_id_t>& uids);

    Status
    WriteBloomFilter(const IdBloomFilterPtr& bloom_filter_ptr);

    Status
    WriteDeletedDocs(const DeletedDocsPtr& deleted_docs);

    Status
    Serialize();

    Status
    Cache();

    Status
    GetSegment(SegmentPtr& segment_ptr);

    Status
    Merge(const std::string& segment_dir_to_merge, const std::string& name);

    size_t
    Size();

    size_t
    VectorCount();

 private:
    Status
    WriteVectors();

    Status
    WriteBloomFilter();

    Status
    WriteDeletedDocs();

 private:
    store::DirectoryPtr directory_ptr_;
    SegmentPtr segment_ptr_;
};

using SegmentWriterPtr = std::shared_ptr<SegmentWriter>;

}  // namespace segment
}  // namespace milvus
