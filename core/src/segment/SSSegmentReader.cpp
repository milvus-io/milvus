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

#include "segment/SSSegmentReader.h"

#include <memory>

#include "Vectors.h"
#include "codecs/snapshot/SSCodec.h"
#include "db/snapshot/ResourceHelper.h"
#include "knowhere/index/vector_index/VecIndex.h"
#include "storage/disk/DiskIOReader.h"
#include "storage/disk/DiskIOWriter.h"
#include "storage/disk/DiskOperation.h"
#include "utils/Log.h"

namespace milvus {
namespace segment {

SSSegmentReader::SSSegmentReader(const engine::SegmentVisitorPtr& segment_visitor)
    : segment_visitor_(segment_visitor) {
    auto& segment_ptr = segment_visitor_->GetSegment();
    std::string directory = engine::snapshot::GetResPath<engine::snapshot::Segment>(segment_ptr);

    storage::IOReaderPtr reader_ptr = std::make_shared<storage::DiskIOReader>();
    storage::IOWriterPtr writer_ptr = std::make_shared<storage::DiskIOWriter>();
    storage::OperationPtr operation_ptr = std::make_shared<storage::DiskOperation>(directory);
    fs_ptr_ = std::make_shared<storage::FSHandler>(reader_ptr, writer_ptr, operation_ptr);

    segment_ptr_ = std::make_shared<Segment>();
}

Status
SSSegmentReader::LoadCache(bool& in_cache) {
    in_cache = false;
    return Status::OK();
}

Status
SSSegmentReader::Load() {
    try {
//        auto& ss_codec = codec::SSCodec::instance();

        auto& field_visitors_map = segment_visitor_->GetFieldVisitors();
        for (auto& f_kv : field_visitors_map) {
            auto& field_visitor = f_kv.second;
            for (auto& file_kv : field_visitor->GetElementVistors()) {
                auto &field_element_visitor = file_kv.second;

                auto &segment_file = field_element_visitor->GetFile();
                if (segment_file == nullptr) {
                    continue;
                }
                auto file_path = engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(segment_file);
                auto &field_element = field_element_visitor->GetElement();

                switch (field_element->GetFtype()) {
                    case engine::snapshot::FieldElementType::FET_UIDS:
                        LoadUids(file_path, segment_ptr_->vectors_ptr_->GetMutableUids());
                        break;
                    case engine::snapshot::FieldElementType::FET_DELETED_DOCS:
                        LoadDeletedDocs(file_path, segment_ptr_->deleted_docs_ptr_);
                        break;
                    case engine::snapshot::FieldElementType::FET_VECTOR_RAW:
                        LoadVectors(file_path, 0, INT64_MAX, segment_ptr_->vectors_ptr_->GetMutableData());
                        break;
//                    case engine::snapshot::FieldElementType::FET_ATTR_RAW:
//                        ss_codec.GetAttrsFormat()->read(fs_ptr_, segment_ptr_->attrs_ptr_);
//                        break;
//                    case engine::snapshot::FieldElementType::FET_ATTR_INDEX:
//                        ss_codec.GetAttrsIndexFormat()->read(fs_ptr_, segment_ptr_->attrs_index_ptr_);
//                        break;
                    default:
                        break;
                }
            }
        }
    } catch (std::exception& e) {
        return Status(DB_ERROR, e.what());
    }
    return Status::OK();
}

Status
SSSegmentReader::LoadVectors(const std::string& file_path, off_t offset, size_t num_bytes,
                             std::vector<uint8_t>& raw_vectors) {
    try {
        auto& ss_codec = codec::SSCodec::instance();
        ss_codec.GetVectorsFormat()->read_vectors(fs_ptr_, file_path, offset, num_bytes, raw_vectors);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load raw vectors: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SSSegmentReader::LoadAttrs(const std::string& field_name, off_t offset, size_t num_bytes,
                         std::vector<uint8_t>& raw_attrs) {
    try {
        auto& ss_codec = codec::SSCodec::instance();
        ss_codec.GetAttrsFormat()->read_attrs(fs_ptr_, field_name, offset, num_bytes, raw_attrs);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load raw attributes: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SSSegmentReader::LoadUids(const std::string& file_path, std::vector<doc_id_t>& uids) {
    try {
        auto& ss_codec = codec::SSCodec::instance();
        ss_codec.GetVectorsFormat()->read_uids(fs_ptr_, file_path, uids);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load uids: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SSSegmentReader::GetSegment(SegmentPtr& segment_ptr) {
    segment_ptr = segment_ptr_;
    return Status::OK();
}

Status
SSSegmentReader::LoadVectorIndex(const std::string& location, codec::ExternalData external_data,
                                 segment::VectorIndexPtr& vector_index_ptr) {
    try {
        auto& ss_codec = codec::SSCodec::instance();
        ss_codec.GetVectorIndexFormat()->read(fs_ptr_, location, external_data, vector_index_ptr);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load vector index: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SSSegmentReader::LoadBloomFilter(const std::string file_path, segment::IdBloomFilterPtr& id_bloom_filter_ptr) {
    try {
        auto& ss_codec = codec::SSCodec::instance();
        ss_codec.GetIdBloomFilterFormat()->read(fs_ptr_, file_path, id_bloom_filter_ptr);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load bloom filter: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SSSegmentReader::LoadDeletedDocs(const std::string& file_path, segment::DeletedDocsPtr& deleted_docs_ptr) {
    try {
        auto& ss_codec = codec::SSCodec::instance();
        ss_codec.GetDeletedDocsFormat()->read(fs_ptr_, file_path, deleted_docs_ptr);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to load deleted docs: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SSSegmentReader::ReadDeletedDocsSize(size_t& size) {
    try {
        auto& ss_codec = codec::SSCodec::instance();
        ss_codec.GetDeletedDocsFormat()->readSize(fs_ptr_, size);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to read deleted docs size: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    return Status::OK();
}
}  // namespace segment
}  // namespace milvus
