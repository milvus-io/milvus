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
#include "cache/CpuCacheMgr.h"
#include "codecs/default/DefaultCodec.h"
#include "config/Config.h"
#include "utils/Log.h"

namespace milvus {
namespace segment {

SegmentReader::SegmentReader(const std::string& directory) {
    fs_ptr_ = milvus::storage::createFsHandler(directory);
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
        fs_ptr_->operation_ptr_->CreateDirectory();
        default_codec.GetVectorsFormat()->read(fs_ptr_, segment_ptr_->vectors_ptr_);
        // default_codec.GetVectorIndexFormat()->read(fs_ptr_, segment_ptr_->vector_index_ptr_);
        default_codec.GetDeletedDocsFormat()->read(fs_ptr_, segment_ptr_->deleted_docs_ptr_);
    } catch (std::exception& e) {
        return Status(DB_ERROR, e.what());
    }
    return Status::OK();
}

Status
SegmentReader::LoadsVectors(VectorsPtr& vectors_ptr) {
    codec::DefaultCodec default_codec;
    try {
        fs_ptr_->operation_ptr_->CreateDirectory();
        vectors_ptr = std::make_shared<Vectors>();
        default_codec.GetVectorsFormat()->read(fs_ptr_, vectors_ptr);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load raw vectors: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, e.what());
    }
    return Status::OK();
}

Status
SegmentReader::LoadsSingleVector(off_t offset, size_t num_bytes, std::vector<uint8_t>& raw_vectors) {
    codec::DefaultCodec default_codec;
    try {
        fs_ptr_->operation_ptr_->CreateDirectory();
        default_codec.GetVectorsFormat()->read_vectors(fs_ptr_, offset, num_bytes, raw_vectors);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load single vector: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentReader::LoadUids(UidsPtr& uids_ptr) {
    codec::DefaultCodec default_codec;
    try {
        fs_ptr_->operation_ptr_->CreateDirectory();
        uids_ptr = std::make_shared<std::vector<doc_id_t>>();
        default_codec.GetVectorsFormat()->read_uids(fs_ptr_, *uids_ptr);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load uids: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentReader::GetSegment(SegmentPtr& segment_ptr) {
    segment_ptr = segment_ptr_;
    return Status::OK();
}

Status
SegmentReader::LoadVectorIndex(const std::string& location, segment::VectorIndexPtr& vector_index_ptr) {
    codec::DefaultCodec default_codec;
    try {
        fs_ptr_->operation_ptr_->CreateDirectory();
        default_codec.GetVectorIndexFormat()->read(fs_ptr_, location, vector_index_ptr);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load vector index: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentReader::LoadBloomFilter(segment::IdBloomFilterPtr& id_bloom_filter_ptr, bool cache_force) {
    codec::DefaultCodec default_codec;
    try {
        // load id_bloom_filter from cache
        std::string cache_key = fs_ptr_->operation_ptr_->GetDirectory() + cache::BloomFilter_Suffix;
        id_bloom_filter_ptr =
            std::static_pointer_cast<segment::IdBloomFilter>(cache::CpuCacheMgr::GetInstance()->GetItem(cache_key));

        if (id_bloom_filter_ptr == nullptr) {
            fs_ptr_->operation_ptr_->CreateDirectory();
            default_codec.GetIdBloomFilterFormat()->read(fs_ptr_, id_bloom_filter_ptr);

            // add id_bloom_filter into cache
            if (cache_force) {
                cache::CpuCacheMgr::GetInstance()->InsertItem(cache_key, id_bloom_filter_ptr);
            } else {
                cache::CpuCacheMgr::GetInstance()->InsertItemIfNotExist(cache_key, id_bloom_filter_ptr);
            }
        }
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load bloom filter: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentReader::LoadDeletedDocs(segment::DeletedDocsPtr& deleted_docs_ptr) {
    codec::DefaultCodec default_codec;
    try {
        fs_ptr_->operation_ptr_->CreateDirectory();
        default_codec.GetDeletedDocsFormat()->read(fs_ptr_, deleted_docs_ptr);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load deleted docs: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentReader::ReadDeletedDocsSize(size_t& size) {
    codec::DefaultCodec default_codec;
    try {
        fs_ptr_->operation_ptr_->CreateDirectory();
        default_codec.GetDeletedDocsFormat()->readSize(fs_ptr_, size);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to read deleted docs size: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}
}  // namespace segment
}  // namespace milvus
