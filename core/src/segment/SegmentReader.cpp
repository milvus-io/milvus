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

#include "segment/SegmentReader.h"

#include <memory>

#include "Vectors.h"
#include "codecs/default/DefaultCodec.h"
#include "store/Directory.h"
#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace segment {

SegmentReader::SegmentReader(const std::string& directory) {
    directory_ptr_ = std::make_shared<store::Directory>(directory);
    segment_ptr_ = std::make_shared<Segment>();
}

Status
SegmentReader::LoadCache(bool& in_cache) {
    in_cache = false;
    return Status::OK();
}

Status
SegmentReader::Load() {
    // TODO(zhiru)
    codec::DefaultCodec default_codec;
    try {
        directory_ptr_->Create();
        default_codec.GetVectorsFormat()->read(directory_ptr_, segment_ptr_->vectors_ptr_);
        default_codec.GetDeletedDocsFormat()->read(directory_ptr_, segment_ptr_->deleted_docs_ptr_);
    } catch (Exception& e) {
        return Status(e.code(), e.what());
    }
    return Status::OK();
}

Status
SegmentReader::LoadVectors(off_t offset, size_t num_bytes, std::vector<uint8_t>& raw_vectors) {
    codec::DefaultCodec default_codec;
    try {
        directory_ptr_->Create();
        default_codec.GetVectorsFormat()->read_vectors(directory_ptr_, offset, num_bytes, raw_vectors);
    } catch (Exception& e) {
        std::string err_msg = "Failed to load raw vectors. " + std::string(e.what());
        ENGINE_LOG_ERROR << err_msg;
        return Status(e.code(), err_msg);
    }
    return Status::OK();
}

Status
SegmentReader::LoadUids(std::vector<doc_id_t>& uids) {
    codec::DefaultCodec default_codec;
    try {
        directory_ptr_->Create();
        default_codec.GetVectorsFormat()->read_uids(directory_ptr_, uids);
    } catch (Exception& e) {
        std::string err_msg = "Failed to load uids. " + std::string(e.what());
        ENGINE_LOG_ERROR << err_msg;
        return Status(e.code(), err_msg);
    }
    return Status::OK();
}

Status
SegmentReader::GetSegment(SegmentPtr& segment_ptr) {
    segment_ptr = segment_ptr_;
    return Status::OK();
}

Status
SegmentReader::LoadBloomFilter(segment::IdBloomFilterPtr& id_bloom_filter_ptr) {
    codec::DefaultCodec default_codec;
    try {
        directory_ptr_->Create();
        default_codec.GetIdBloomFilterFormat()->read(directory_ptr_, id_bloom_filter_ptr);
    } catch (Exception& e) {
        std::string err_msg = "Failed to load bloom filter. " + std::string(e.what());
        ENGINE_LOG_ERROR << err_msg;
        return Status(e.code(), err_msg);
    }
    return Status::OK();
}

Status
SegmentReader::LoadDeletedDocs(segment::DeletedDocsPtr& deleted_docs_ptr) {
    codec::DefaultCodec default_codec;
    try {
        directory_ptr_->Create();
        default_codec.GetDeletedDocsFormat()->read(directory_ptr_, deleted_docs_ptr);
    } catch (Exception& e) {
        std::string err_msg = "Failed to load deleted docs. " + std::string(e.what());
        ENGINE_LOG_ERROR << err_msg;
        return Status(e.code(), err_msg);
    }
    return Status::OK();
}

}  // namespace segment
}  // namespace milvus
