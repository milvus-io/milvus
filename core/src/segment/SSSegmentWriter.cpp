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

#include "segment/SSSegmentWriter.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "SSSegmentReader.h"
#include "Vectors.h"
#include "codecs/snapshot/SSCodec.h"
#include "db/Utils.h"
#include "db/snapshot/ResourceHelper.h"
#include "storage/disk/DiskIOReader.h"
#include "storage/disk/DiskIOWriter.h"
#include "storage/disk/DiskOperation.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace segment {

SSSegmentWriter::SSSegmentWriter(const engine::SegmentVisitorPtr& segment_visitor) : segment_visitor_(segment_visitor) {
    Initialize();
}

Status
SSSegmentWriter::Initialize() {
    auto& segment_ptr = segment_visitor_->GetSegment();
    std::string directory = engine::snapshot::GetResPath<engine::snapshot::Segment>(segment_ptr);

    storage::IOReaderPtr reader_ptr = std::make_shared<storage::DiskIOReader>();
    storage::IOWriterPtr writer_ptr = std::make_shared<storage::DiskIOWriter>();
    storage::OperationPtr operation_ptr = std::make_shared<storage::DiskOperation>(directory);
    fs_ptr_ = std::make_shared<storage::FSHandler>(reader_ptr, writer_ptr, operation_ptr);
    segment_ptr_ = std::make_shared<engine::Segment>();

    const engine::SegmentVisitor::IdMapT& field_map = segment_visitor_->GetFieldVisitors();
    for (auto& iter : field_map) {
        const engine::snapshot::FieldPtr& field = iter.second->GetField();
        std::string name = field->GetName();
        engine::FIELD_TYPE ftype = static_cast<engine::FIELD_TYPE>(field->GetFtype());
        if (ftype == engine::FIELD_TYPE::VECTOR || ftype == engine::FIELD_TYPE::VECTOR ||
            ftype == engine::FIELD_TYPE::VECTOR) {
            json params = field->GetParams();
            if (params.find(engine::VECTOR_DIMENSION_PARAM) == params.end()) {
                std::string msg = "Vector field params must contain: dimension";
                LOG_SERVER_ERROR_ << msg;
                return Status(DB_ERROR, msg);
            }

            uint64_t field_width = 0;
            uint64_t dimension = params[engine::VECTOR_DIMENSION_PARAM];
            if (ftype == engine::FIELD_TYPE::VECTOR_BINARY) {
                field_width += (dimension / 8);
            } else {
                field_width += (dimension * sizeof(float));
            }
            segment_ptr_->AddField(name, ftype, field_width);
        } else {
            segment_ptr_->AddField(name, ftype);
        }
    }

    return Status::OK();
}

Status
SSSegmentWriter::AddChunk(const engine::DataChunkPtr& chunk_ptr) {
    return segment_ptr_->AddChunk(chunk_ptr);
}

Status
SSSegmentWriter::AddChunk(const engine::DataChunkPtr& chunk_ptr, uint64_t from, uint64_t to) {
    return segment_ptr_->AddChunk(chunk_ptr, from, to);
}

Status
SSSegmentWriter::Serialize() {
    auto& field_visitors_map = segment_visitor_->GetFieldVisitors();
    auto uid_field_visitor = segment_visitor_->GetFieldVisitor(engine::DEFAULT_UID_NAME);

    /* write fields raw data */
    for (auto& iter : field_visitors_map) {
        const engine::snapshot::FieldPtr& field = iter.second->GetField();
        std::string name = field->GetName();
        engine::FIXED_FIELD_DATA raw_data;
        segment_ptr_->GetFixedFieldData(name, raw_data);

        auto element_visitor = iter.second->GetElementVisitor(engine::FieldElementType::FET_RAW);
        std::string file_path = engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(element_visitor->GetFile());
        STATUS_CHECK(WriteField(file_path, raw_data));
    }

    /* write UID's deleted docs */
    auto uid_del_visitor = uid_field_visitor->GetElementVisitor(engine::FieldElementType::FET_DELETED_DOCS);
    std::string uid_del_path = engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(uid_del_visitor->GetFile());
    STATUS_CHECK(WriteDeletedDocs(uid_del_path, segment_ptr_->GetDeletedDocs()));

    /* write UID's bloom filter */
    auto uid_blf_visitor = uid_field_visitor->GetElementVisitor(engine::FieldElementType::FET_BLOOM_FILTER);
    std::string uid_blf_path = engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(uid_blf_visitor->GetFile());
    STATUS_CHECK(WriteBloomFilter(uid_blf_path, segment_ptr_->GetBloomFilter()));

    return Status::OK();
}

Status
SSSegmentWriter::WriteField(const std::string& file_path, const engine::FIXED_FIELD_DATA& raw) {
    try {
        auto& ss_codec = codec::SSCodec::instance();
        fs_ptr_->operation_ptr_->CreateDirectory();
        ss_codec.GetBlockFormat()->write(fs_ptr_, file_path, raw);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write vectors: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;

        engine::utils::SendExitSignal();
        return Status(SERVER_WRITE_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SSSegmentWriter::WriteBloomFilter(const std::string& file_path) {
    try {
        auto& ss_codec = codec::SSCodec::instance();

        TimeRecorder recorder("SSSegmentWriter::WriteBloomFilter");

        engine::FIXED_FIELD_DATA uid_data;
        auto status = segment_ptr_->GetFixedFieldData(engine::DEFAULT_UID_NAME, uid_data);
        if (!status.ok()) {
            return status;
        }

        segment::IdBloomFilterPtr bloom_filter_ptr;
        ss_codec.GetIdBloomFilterFormat()->create(fs_ptr_, bloom_filter_ptr);

        recorder.RecordSection("Initializing bloom filter");

        int64_t* uids = (int64_t*)(uid_data.data());
        int64_t row_count = segment_ptr_->GetRowCount();
        for (uint64_t i = 0; i < row_count; i++) {
            bloom_filter_ptr->Add(uids[i]);
        }
        segment_ptr_->SetBloomFilter(bloom_filter_ptr);

        recorder.RecordSection("Adding " + std::to_string(row_count) + " ids to bloom filter");
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write vectors: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;

        engine::utils::SendExitSignal();
        return Status(SERVER_WRITE_ERROR, err_msg);
    }

    return WriteBloomFilter(file_path, segment_ptr_->GetBloomFilter());
}

Status
SSSegmentWriter::WriteBloomFilter(const std::string& file_path, const IdBloomFilterPtr& id_bloom_filter_ptr) {
    try {
        TimeRecorder recorder("SSSegmentWriter::WriteBloomFilter");
        auto& ss_codec = codec::SSCodec::instance();
        fs_ptr_->operation_ptr_->CreateDirectory();
        ss_codec.GetIdBloomFilterFormat()->write(fs_ptr_, file_path, id_bloom_filter_ptr);
        recorder.RecordSection("finish writing bloom filter");
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write bloom filter: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;

        engine::utils::SendExitSignal();
        return Status(SERVER_WRITE_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SSSegmentWriter::WriteDeletedDocs(const std::string& file_path) {
    DeletedDocsPtr deleted_docs_ptr = std::make_shared<DeletedDocs>();
    auto status = WriteDeletedDocs(file_path, deleted_docs_ptr);
    if (!status.ok()) {
        return status;
    }

    segment_ptr_->SetDeletedDocs(deleted_docs_ptr);
    return Status::OK();
}

Status
SSSegmentWriter::WriteDeletedDocs(const std::string& file_path, const DeletedDocsPtr& deleted_docs) {
    try {
        auto& ss_codec = codec::SSCodec::instance();
        fs_ptr_->operation_ptr_->CreateDirectory();
        ss_codec.GetDeletedDocsFormat()->write(fs_ptr_, file_path, deleted_docs);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write deleted docs: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;

        engine::utils::SendExitSignal();
        return Status(SERVER_WRITE_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SSSegmentWriter::GetSegment(engine::SegmentPtr& segment_ptr) {
    segment_ptr = segment_ptr_;
    return Status::OK();
}

Status
SSSegmentWriter::Merge(const SSSegmentReaderPtr& segment_to_merge) {
    //    if (dir_to_merge == fs_ptr_->operation_ptr_->GetDirectory()) {
    //        return Status(DB_ERROR, "Cannot Merge Self");
    //    }
    //
    //    LOG_ENGINE_DEBUG_ << "Merging from " << dir_to_merge << " to " << fs_ptr_->operation_ptr_->GetDirectory();
    //
    //    TimeRecorder recorder("SSSegmentWriter::Merge");
    //
    //    SSSegmentReader segment_reader_to_merge(dir_to_merge);
    //    bool in_cache;
    //    auto status = segment_reader_to_merge.LoadCache(in_cache);
    //    if (!in_cache) {
    //        status = segment_reader_to_merge.Load();
    //        if (!status.ok()) {
    //            std::string msg = "Failed to load segment from " + dir_to_merge;
    //            LOG_ENGINE_ERROR_ << msg;
    //            return Status(DB_ERROR, msg);
    //        }
    //    }
    //    SegmentPtr segment_to_merge;
    //    segment_reader_to_merge.GetSegment(segment_to_merge);
    //    // auto& uids = segment_to_merge->vectors_ptr_->GetUids();
    //
    //    recorder.RecordSection("Loading segment");
    //
    //    if (segment_to_merge->deleted_docs_ptr_ != nullptr) {
    //        auto offsets_to_delete = segment_to_merge->deleted_docs_ptr_->GetDeletedDocs();
    //
    //        // Erase from raw data
    //        segment_to_merge->vectors_ptr_->Erase(offsets_to_delete);
    //    }
    //
    //    recorder.RecordSection("erase");
    //
    //    AddVectors(name, segment_to_merge->vectors_ptr_->GetData(), segment_to_merge->vectors_ptr_->GetUids());
    //
    //    auto rows = segment_to_merge->vectors_ptr_->GetCount();
    //    recorder.RecordSection("Adding " + std::to_string(rows) + " vectors and uids");
    //
    //    std::unordered_map<std::string, uint64_t> attr_nbytes;
    //    std::unordered_map<std::string, std::vector<uint8_t>> attr_data;
    //    auto attr_it = segment_to_merge->attrs_ptr_->attrs.begin();
    //    for (; attr_it != segment_to_merge->attrs_ptr_->attrs.end(); attr_it++) {
    //        attr_nbytes.insert(std::make_pair(attr_it->first, attr_it->second->GetNbytes()));
    //        attr_data.insert(std::make_pair(attr_it->first, attr_it->second->GetData()));
    //
    //        if (segment_to_merge->deleted_docs_ptr_ != nullptr) {
    //            auto offsets_to_delete = segment_to_merge->deleted_docs_ptr_->GetDeletedDocs();
    //
    //            // Erase from field data
    //            attr_it->second->Erase(offsets_to_delete);
    //        }
    //    }
    //    AddAttrs(name, attr_nbytes, attr_data, segment_to_merge->vectors_ptr_->GetUids());
    //
    //  LOG_ENGINE_DEBUG_ << "Merging completed from " << dir_to_merge << " to " <<
    //  fs_ptr_->operation_ptr_->GetDirectory();

    return Status::OK();
}

size_t
SSSegmentWriter::Size() {
    return 0;
}

size_t
SSSegmentWriter::RowCount() {
    return segment_ptr_->GetRowCount();
}

Status
SSSegmentWriter::SetVectorIndex(const std::string& field_name, const milvus::knowhere::VecIndexPtr& index) {
    return segment_ptr_->SetVectorIndex(field_name, index);
}

Status
SSSegmentWriter::WriteVectorIndex(const std::string& field_name, const std::string& file_path) {
    try {
        knowhere::VecIndexPtr index;
        segment_ptr_->GetVectorIndex(field_name, index);
        segment::VectorIndexPtr index_ptr = std::make_shared<segment::VectorIndex>(index);

        auto& ss_codec = codec::SSCodec::instance();
        fs_ptr_->operation_ptr_->CreateDirectory();
        ss_codec.GetVectorIndexFormat()->write(fs_ptr_, file_path, index_ptr);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write vector index: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;

        engine::utils::SendExitSignal();
        return Status(SERVER_WRITE_ERROR, err_msg);
    }
    return Status::OK();
}

}  // namespace segment
}  // namespace milvus
