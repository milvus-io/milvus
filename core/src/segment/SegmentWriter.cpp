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

#include "segment/SegmentWriter.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "SegmentReader.h"
#include "codecs/Codec.h"
#include "db/SnapshotUtils.h"
#include "db/Utils.h"
#include "db/snapshot/ResourceHelper.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "storage/disk/DiskIOReader.h"
#include "storage/disk/DiskIOWriter.h"
#include "storage/disk/DiskOperation.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/SignalHandler.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace segment {

SegmentWriter::SegmentWriter(const std::string& dir_root, const engine::SegmentVisitorPtr& segment_visitor)
    : dir_root_(dir_root), segment_visitor_(segment_visitor) {
    Initialize();
}

Status
SegmentWriter::Initialize() {
    dir_collections_ = dir_root_ + engine::COLLECTIONS_FOLDER;

    std::string directory =
        engine::snapshot::GetResPath<engine::snapshot::Segment>(dir_collections_, segment_visitor_->GetSegment());

    storage::IOReaderPtr reader_ptr = std::make_shared<storage::DiskIOReader>();
    storage::IOWriterPtr writer_ptr = std::make_shared<storage::DiskIOWriter>();
    storage::OperationPtr operation_ptr = std::make_shared<storage::DiskOperation>(directory);
    fs_ptr_ = std::make_shared<storage::FSHandler>(reader_ptr, writer_ptr, operation_ptr);
    fs_ptr_->operation_ptr_->CreateDirectory();

    segment_ptr_ = std::make_shared<engine::Segment>();

    const engine::SegmentVisitor::IdMapT& field_map = segment_visitor_->GetFieldVisitors();
    for (auto& iter : field_map) {
        const engine::snapshot::FieldPtr& field = iter.second->GetField();
        std::string name = field->GetName();
        engine::DataType ftype = static_cast<engine::DataType>(field->GetFtype());
        if (engine::IsVectorField(field)) {
            json params = field->GetParams();
            if (params.find(knowhere::meta::DIM) == params.end()) {
                std::string msg = "Vector field params must contain: dimension";
                LOG_SERVER_ERROR_ << msg;
                return Status(DB_ERROR, msg);
            }

            int64_t field_width = 0;
            int64_t dimension = params[knowhere::meta::DIM];
            if (ftype == engine::DataType::VECTOR_BINARY) {
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
SegmentWriter::AddChunk(const engine::DataChunkPtr& chunk_ptr) {
    return segment_ptr_->AddChunk(chunk_ptr);
}

Status
SegmentWriter::AddChunk(const engine::DataChunkPtr& chunk_ptr, int64_t from, int64_t to) {
    return segment_ptr_->AddChunk(chunk_ptr, from, to);
}

Status
SegmentWriter::Serialize() {
    // write fields raw data
    STATUS_CHECK(WriteFields());

    // write empty UID's deleted docs
    STATUS_CHECK(WriteDeletedDocs());

    // write UID's bloom filter
    STATUS_CHECK(WriteBloomFilter());

    return Status::OK();
}

Status
SegmentWriter::WriteField(const std::string& file_path, const engine::BinaryDataPtr& raw) {
    try {
        auto& ss_codec = codec::Codec::instance();
        ss_codec.GetBlockFormat()->Write(fs_ptr_, file_path, raw);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write field: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;

        engine::utils::SendExitSignal();
        return Status(SERVER_WRITE_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentWriter::WriteFields() {
    TimeRecorder recorder("SegmentWriter::WriteFields");

    auto& field_visitors_map = segment_visitor_->GetFieldVisitors();
    for (auto& iter : field_visitors_map) {
        const engine::snapshot::FieldPtr& field = iter.second->GetField();
        std::string name = field->GetName();
        engine::BinaryDataPtr raw_data;
        segment_ptr_->GetFixedFieldData(name, raw_data);

        auto element_visitor = iter.second->GetElementVisitor(engine::FieldElementType::FET_RAW);
        if (element_visitor && element_visitor->GetFile()) {
            auto segment_file = element_visitor->GetFile();
            std::string file_path =
                engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, segment_file);
            STATUS_CHECK(WriteField(file_path, raw_data));

            auto file_size = milvus::CommonUtil::GetFileSize(file_path);
            segment_file->SetSize(file_size);

            recorder.RecordSection("Serialize field raw file");
        } else {
            return Status(DB_ERROR, "Raw element missed in snapshot");
        }
    }

    return Status::OK();
}

Status
SegmentWriter::WriteBloomFilter() {
    try {
        TimeRecorder recorder("SegmentWriter::WriteBloomFilter");

        engine::BinaryDataPtr uid_data;
        auto status = segment_ptr_->GetFixedFieldData(engine::DEFAULT_UID_NAME, uid_data);
        if (!status.ok()) {
            return status;
        }

        auto& field_visitors_map = segment_visitor_->GetFieldVisitors();
        auto uid_field_visitor = segment_visitor_->GetFieldVisitor(engine::DEFAULT_UID_NAME);
        auto uid_blf_visitor = uid_field_visitor->GetElementVisitor(engine::FieldElementType::FET_BLOOM_FILTER);
        if (uid_blf_visitor && uid_blf_visitor->GetFile()) {
            auto segment_file = uid_blf_visitor->GetFile();
            std::string file_path =
                engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, segment_file);

            auto& ss_codec = codec::Codec::instance();
            segment::IdBloomFilterPtr bloom_filter_ptr;
            ss_codec.GetIdBloomFilterFormat()->Create(fs_ptr_, file_path, bloom_filter_ptr);

            int64_t* uids = (int64_t*)(uid_data->data_.data());
            int64_t row_count = segment_ptr_->GetRowCount();
            for (int64_t i = 0; i < row_count; i++) {
                bloom_filter_ptr->Add(uids[i]);
            }
            segment_ptr_->SetBloomFilter(bloom_filter_ptr);

            recorder.RecordSection("Initialize bloom filter");

            STATUS_CHECK(WriteBloomFilter(file_path, segment_ptr_->GetBloomFilter()));

            auto file_size = milvus::CommonUtil::GetFileSize(file_path + codec::IdBloomFilterFormat::FilePostfix());
            segment_file->SetSize(file_size);
        } else {
            return Status(DB_ERROR, "Bloom filter element missed in snapshot");
        }
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write vectors: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;

        engine::utils::SendExitSignal();
        return Status(SERVER_WRITE_ERROR, err_msg);
    }

    return Status::OK();
}

Status
SegmentWriter::CreateBloomFilter(const std::string& file_path, IdBloomFilterPtr& bloom_filter_ptr) {
    auto& ss_codec = codec::Codec::instance();
    try {
        ss_codec.GetIdBloomFilterFormat()->Create(fs_ptr_, file_path, bloom_filter_ptr);
    } catch (std::exception& er) {
        return Status(DB_ERROR, "Create a new bloom filter fail: " + std::string(er.what()));
    }

    return Status::OK();
}

Status
SegmentWriter::WriteBloomFilter(const std::string& file_path, const IdBloomFilterPtr& id_bloom_filter_ptr) {
    if (id_bloom_filter_ptr == nullptr) {
        return Status(DB_ERROR, "WriteBloomFilter: null pointer");
    }

    try {
        TimeRecorderAuto recorder("SegmentWriter::WriteBloomFilter: " + file_path);

        auto& ss_codec = codec::Codec::instance();
        ss_codec.GetIdBloomFilterFormat()->Write(fs_ptr_, file_path, id_bloom_filter_ptr);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write bloom filter: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;

        engine::utils::SendExitSignal();
        return Status(SERVER_WRITE_ERROR, err_msg);
    }

    return Status::OK();
}

Status
SegmentWriter::WriteDeletedDocs() {
    auto& field_visitors_map = segment_visitor_->GetFieldVisitors();
    auto uid_field_visitor = segment_visitor_->GetFieldVisitor(engine::DEFAULT_UID_NAME);
    auto del_doc_visitor = uid_field_visitor->GetElementVisitor(engine::FieldElementType::FET_DELETED_DOCS);
    if (del_doc_visitor && del_doc_visitor->GetFile()) {
        auto segment_file = del_doc_visitor->GetFile();
        std::string file_path =
            engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, segment_file);

        STATUS_CHECK(WriteDeletedDocs(file_path, segment_ptr_->GetDeletedDocs()));

        auto file_size = milvus::CommonUtil::GetFileSize(file_path + codec::DeletedDocsFormat::FilePostfix());
        segment_file->SetSize(file_size);
    } else {
        return Status(DB_ERROR, "Deleted-doc element missed in snapshot");
    }

    return Status::OK();
}

Status
SegmentWriter::WriteDeletedDocs(const std::string& file_path, const DeletedDocsPtr& deleted_docs) {
    if (deleted_docs == nullptr) {
        return Status::OK();
    }

    try {
        TimeRecorderAuto recorder("SegmentWriter::WriteDeletedDocs: " + file_path);

        auto& ss_codec = codec::Codec::instance();
        ss_codec.GetDeletedDocsFormat()->Write(fs_ptr_, file_path, deleted_docs);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write deleted docs: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;

        engine::utils::SendExitSignal();
        return Status(SERVER_WRITE_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentWriter::Merge(const SegmentReaderPtr& segment_reader) {
    if (segment_reader == nullptr) {
        return Status(DB_ERROR, "Segment reader is null");
    }

    // check conflict
    int64_t src_id, target_id;
    auto status = GetSegmentID(target_id);
    if (!status.ok()) {
        return status;
    }
    status = segment_reader->GetSegmentID(src_id);
    if (!status.ok()) {
        return status;
    }
    if (src_id == target_id) {
        return Status(DB_ERROR, "Cannot Merge Self");
    }

    LOG_ENGINE_DEBUG_ << "Merging from " << segment_reader->GetSegmentPath() << " to " << GetSegmentPath();

    TimeRecorderAuto recorder("SegmentWriter::Merge");

    // load raw data
    status = segment_reader->LoadFields();
    if (!status.ok()) {
        return status;
    }

    // merge deleted docs (Note: this step must before merge raw data)
    segment::DeletedDocsPtr src_deleted_docs;
    status = segment_reader->LoadDeletedDocs(src_deleted_docs);
    if (!status.ok()) {
        return status;
    }

    engine::SegmentPtr src_segment;
    status = segment_reader->GetSegment(src_segment);
    if (!status.ok()) {
        return status;
    }

    if (src_deleted_docs) {
        const std::vector<offset_t>& delete_ids = src_deleted_docs->GetDeletedDocs();
        for (auto offset : delete_ids) {
            src_segment->DeleteEntity(offset);
        }
    }

    // merge filed raw data
    engine::DataChunkPtr chunk = std::make_shared<engine::DataChunk>();
    auto& field_visitors_map = segment_visitor_->GetFieldVisitors();
    for (auto& iter : field_visitors_map) {
        const engine::snapshot::FieldPtr& field = iter.second->GetField();
        std::string name = field->GetName();
        engine::BinaryDataPtr raw_data;
        segment_reader->LoadField(name, raw_data);
        chunk->fixed_fields_[name] = raw_data;
    }

    auto& uid_data = chunk->fixed_fields_[engine::DEFAULT_UID_NAME];
    chunk->count_ = uid_data->data_.size() / sizeof(int64_t);
    status = AddChunk(chunk);
    if (!status.ok()) {
        return status;
    }

    // Note: no need to merge bloom filter, the bloom filter will be created during serialize

    return Status::OK();
}

size_t
SegmentWriter::RowCount() {
    return segment_ptr_->GetRowCount();
}

Status
SegmentWriter::SetVectorIndex(const std::string& field_name, const milvus::knowhere::VecIndexPtr& index) {
    return segment_ptr_->SetVectorIndex(field_name, index);
}

Status
SegmentWriter::WriteVectorIndex(const std::string& field_name) {
    try {
        knowhere::VecIndexPtr index;
        auto status = segment_ptr_->GetVectorIndex(field_name, index);
        if (!status.ok() || index == nullptr) {
            return Status(DB_ERROR, "Index doesn't exist: " + status.message());
        }

        auto& field_visitors_map = segment_visitor_->GetFieldVisitors();
        auto field = segment_visitor_->GetFieldVisitor(field_name);
        if (field == nullptr) {
            return Status(DB_ERROR, "Invalid filed name: " + field_name);
        }
        auto& ss_codec = codec::Codec::instance();
        fs_ptr_->operation_ptr_->CreateDirectory();

        // serialize index file
        {
            auto element_visitor = field->GetElementVisitor(engine::FieldElementType::FET_INDEX);
            if (element_visitor && element_visitor->GetFile()) {
                auto segment_file = element_visitor->GetFile();
                std::string file_path =
                    engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, segment_file);
                ss_codec.GetVectorIndexFormat()->WriteIndex(fs_ptr_, file_path, index);

                auto file_size = milvus::CommonUtil::GetFileSize(file_path + codec::VectorIndexFormat::FilePostfix());
                segment_file->SetSize(file_size);
            }
        }

        // serialize compress file
        {
            auto element_visitor = field->GetElementVisitor(engine::FieldElementType::FET_COMPRESS_SQ8);
            if (element_visitor && element_visitor->GetFile()) {
                auto segment_file = element_visitor->GetFile();
                std::string file_path =
                    engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, segment_file);
                ss_codec.GetVectorIndexFormat()->WriteCompress(fs_ptr_, file_path, index);

                auto file_size =
                    milvus::CommonUtil::GetFileSize(file_path + codec::VectorCompressFormat::FilePostfix());
                segment_file->SetSize(file_size);
            }
        }
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write vector index: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;

        engine::utils::SendExitSignal();
        return Status(SERVER_WRITE_ERROR, err_msg);
    }

    return Status::OK();
}

Status
SegmentWriter::SetStructuredIndex(const std::string& field_name, const knowhere::IndexPtr& index) {
    return segment_ptr_->SetStructuredIndex(field_name, index);
}

Status
SegmentWriter::WriteStructuredIndex(const std::string& field_name) {
    try {
        knowhere::IndexPtr index;
        auto status = segment_ptr_->GetStructuredIndex(field_name, index);
        if (!status.ok() || index == nullptr) {
            return Status(DB_ERROR, "Structured index doesn't exist: " + status.message());
        }

        auto field = segment_visitor_->GetFieldVisitor(field_name);
        if (field == nullptr) {
            return Status(DB_ERROR, "Invalid filed name: " + field_name);
        }

        auto& ss_codec = codec::Codec::instance();
        fs_ptr_->operation_ptr_->CreateDirectory();

        // serialize index file
        auto element_visitor = field->GetElementVisitor(engine::FieldElementType::FET_INDEX);
        if (element_visitor && element_visitor->GetFile()) {
            engine::DataType field_type;
            segment_ptr_->GetFieldType(field_name, field_type);
            auto segment_file = element_visitor->GetFile();
            std::string file_path =
                engine::snapshot::GetResPath<engine::snapshot::SegmentFile>(dir_collections_, segment_file);
            ss_codec.GetStructuredIndexFormat()->Write(fs_ptr_, file_path, field_type, index);

            auto file_size = milvus::CommonUtil::GetFileSize(file_path + codec::StructuredIndexFormat::FilePostfix());
            segment_file->SetSize(file_size);
        }
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write vector index: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;

        engine::utils::SendExitSignal();
        return Status(SERVER_WRITE_ERROR, err_msg);
    }

    return Status::OK();
}

Status
SegmentWriter::GetSegment(engine::SegmentPtr& segment_ptr) {
    segment_ptr = segment_ptr_;
    return Status::OK();
}

Status
SegmentWriter::GetSegmentID(int64_t& id) {
    if (segment_visitor_) {
        auto segment = segment_visitor_->GetSegment();
        if (segment) {
            id = segment->GetID();
            return Status::OK();
        }
    }

    return Status(DB_ERROR, "SegmentWriter::GetSegmentID: null pointer");
}

std::string
SegmentWriter::GetSegmentPath() {
    std::string seg_path =
        engine::snapshot::GetResPath<engine::snapshot::Segment>(dir_collections_, segment_visitor_->GetSegment());
    return seg_path;
}

}  // namespace segment
}  // namespace milvus
