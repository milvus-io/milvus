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
#include "Vectors.h"
#include "codecs/default/DefaultCodec.h"
#include "storage/disk/DiskIOReader.h"
#include "storage/disk/DiskIOWriter.h"
#include "storage/disk/DiskOperation.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace segment {

SegmentWriter::SegmentWriter(const std::string& directory) {
    storage::IOReaderPtr reader_ptr = std::make_shared<storage::DiskIOReader>();
    storage::IOWriterPtr writer_ptr = std::make_shared<storage::DiskIOWriter>();
    storage::OperationPtr operation_ptr = std::make_shared<storage::DiskOperation>(directory);
    fs_ptr_ = std::make_shared<storage::FSHandler>(reader_ptr, writer_ptr, operation_ptr);
    segment_ptr_ = std::make_shared<Segment>();
}

Status
SegmentWriter::AddVectors(const std::string& name, const std::vector<uint8_t>& data,
                          const std::vector<doc_id_t>& uids) {
    segment_ptr_->vectors_ptr_->AddData(data);
    segment_ptr_->vectors_ptr_->AddUids(uids);
    segment_ptr_->vectors_ptr_->SetName(name);

    return Status::OK();
}

Status
SegmentWriter::AddAttrs(const std::string& name, const std::unordered_map<std::string, uint64_t>& attr_nbytes,
                        const std::unordered_map<std::string, std::vector<uint8_t>>& attr_data,
                        const std::vector<doc_id_t>& uids) {
    auto attr_data_it = attr_data.begin();
    auto attrs = segment_ptr_->attrs_ptr_->attrs;
    for (; attr_data_it != attr_data.end(); ++attr_data_it) {
        if (attrs.find(attr_data_it->first) != attrs.end()) {
            segment_ptr_->attrs_ptr_->attrs.at(attr_data_it->first)
                ->AddAttr(attr_data_it->second, attr_nbytes.at(attr_data_it->first));
            segment_ptr_->attrs_ptr_->attrs.at(attr_data_it->first)->AddUids(uids);
        } else {
            AttrPtr attr = std::make_shared<Attr>(attr_data_it->second, attr_nbytes.at(attr_data_it->first), uids,
                                                  attr_data_it->first);
            segment_ptr_->attrs_ptr_->attrs.insert(std::make_pair(attr_data_it->first, attr));
        }
    }
    return Status::OK();
}

Status
SegmentWriter::SetVectorIndex(const milvus::knowhere::VecIndexPtr& index) {
    segment_ptr_->vector_index_ptr_->SetVectorIndex(index);
    return Status::OK();
}

Status
SegmentWriter::Serialize() {
    TimeRecorder recorder("SegmentWriter::Serialize");

    auto status = WriteBloomFilter();
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << status.message();
        return status;
    }

    recorder.RecordSection("Writing bloom filter done");

    status = WriteVectors();
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Write vectors fail: " << status.message();
        return status;
    }

    status = WriteAttrs();
    if (!status.ok()) {
        return status;
    }

    recorder.RecordSection("Writing vectors and uids done");

    // Write an empty deleted doc
    status = WriteDeletedDocs();

    recorder.RecordSection("Writing deleted docs done");

    return status;
}

Status
SegmentWriter::WriteVectors() {
    codec::DefaultCodec default_codec;
    try {
        fs_ptr_->operation_ptr_->CreateDirectory();
        default_codec.GetVectorsFormat()->write(fs_ptr_, segment_ptr_->vectors_ptr_);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write vectors: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(SERVER_WRITE_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentWriter::WriteAttrs() {
    codec::DefaultCodec default_codec;
    try {
        fs_ptr_->operation_ptr_->CreateDirectory();
        default_codec.GetAttrsFormat()->write(fs_ptr_, segment_ptr_->attrs_ptr_);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write vectors: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(SERVER_WRITE_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentWriter::WriteVectorIndex(const std::string& location) {
    codec::DefaultCodec default_codec;
    try {
        fs_ptr_->operation_ptr_->CreateDirectory();
        default_codec.GetVectorIndexFormat()->write(fs_ptr_, location, segment_ptr_->vector_index_ptr_);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write vector index: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(SERVER_WRITE_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentWriter::WriteBloomFilter() {
    codec::DefaultCodec default_codec;
    try {
        fs_ptr_->operation_ptr_->CreateDirectory();

        TimeRecorder recorder("SegmentWriter::WriteBloomFilter");

        default_codec.GetIdBloomFilterFormat()->create(fs_ptr_, segment_ptr_->id_bloom_filter_ptr_);

        recorder.RecordSection("Initializing bloom filter");

        auto& uids = segment_ptr_->vectors_ptr_->GetUids();
        for (auto& uid : uids) {
            segment_ptr_->id_bloom_filter_ptr_->Add(uid);
        }

        recorder.RecordSection("Adding " + std::to_string(uids.size()) + " ids to bloom filter");

        default_codec.GetIdBloomFilterFormat()->write(fs_ptr_, segment_ptr_->id_bloom_filter_ptr_);

        recorder.RecordSection("Writing bloom filter");
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write vectors: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(SERVER_WRITE_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentWriter::WriteDeletedDocs() {
    codec::DefaultCodec default_codec;
    try {
        fs_ptr_->operation_ptr_->CreateDirectory();
        DeletedDocsPtr deleted_docs_ptr = std::make_shared<DeletedDocs>();
        default_codec.GetDeletedDocsFormat()->write(fs_ptr_, deleted_docs_ptr);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write deleted docs: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(SERVER_WRITE_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentWriter::WriteDeletedDocs(const DeletedDocsPtr& deleted_docs) {
    codec::DefaultCodec default_codec;
    try {
        fs_ptr_->operation_ptr_->CreateDirectory();
        default_codec.GetDeletedDocsFormat()->write(fs_ptr_, deleted_docs);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write deleted docs: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(SERVER_WRITE_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentWriter::WriteBloomFilter(const IdBloomFilterPtr& id_bloom_filter_ptr) {
    codec::DefaultCodec default_codec;
    try {
        fs_ptr_->operation_ptr_->CreateDirectory();
        default_codec.GetIdBloomFilterFormat()->write(fs_ptr_, id_bloom_filter_ptr);
    } catch (std::exception& e) {
        std::string err_msg = "Failed to write bloom filter: " + std::string(e.what());
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(SERVER_WRITE_ERROR, err_msg);
    }
    return Status::OK();
}

Status
SegmentWriter::Cache() {
    // TODO(zhiru)
    return Status::OK();
}

Status
SegmentWriter::GetSegment(SegmentPtr& segment_ptr) {
    segment_ptr = segment_ptr_;
    return Status::OK();
}

Status
SegmentWriter::Merge(const std::string& dir_to_merge, const std::string& name) {
    if (dir_to_merge == fs_ptr_->operation_ptr_->GetDirectory()) {
        return Status(DB_ERROR, "Cannot Merge Self");
    }

    LOG_ENGINE_DEBUG_ << "Merging from " << dir_to_merge << " to " << fs_ptr_->operation_ptr_->GetDirectory();

    TimeRecorder recorder("SegmentWriter::Merge");

    SegmentReader segment_reader_to_merge(dir_to_merge);
    bool in_cache;
    auto status = segment_reader_to_merge.LoadCache(in_cache);
    if (!in_cache) {
        status = segment_reader_to_merge.Load();
        if (!status.ok()) {
            std::string msg = "Failed to load segment from " + dir_to_merge;
            LOG_ENGINE_ERROR_ << msg;
            return Status(DB_ERROR, msg);
        }
    }
    SegmentPtr segment_to_merge;
    segment_reader_to_merge.GetSegment(segment_to_merge);
    auto& uids = segment_to_merge->vectors_ptr_->GetUids();

    recorder.RecordSection("Loading segment");

    if (segment_to_merge->deleted_docs_ptr_ != nullptr) {
        auto offsets_to_delete = segment_to_merge->deleted_docs_ptr_->GetDeletedDocs();

        // Erase from raw data
        segment_to_merge->vectors_ptr_->Erase(offsets_to_delete);
    }

    recorder.RecordSection("erase");

    AddVectors(name, segment_to_merge->vectors_ptr_->GetData(), segment_to_merge->vectors_ptr_->GetUids());

    auto rows = segment_to_merge->vectors_ptr_->GetCount();
    recorder.RecordSection("Adding " + std::to_string(rows) + " vectors and uids");

    std::unordered_map<std::string, uint64_t> attr_nbytes;
    std::unordered_map<std::string, std::vector<uint8_t>> attr_data;
    auto attr_it = segment_to_merge->attrs_ptr_->attrs.begin();
    for (; attr_it != segment_to_merge->attrs_ptr_->attrs.end(); attr_it++) {
        attr_nbytes.insert(std::make_pair(attr_it->first, attr_it->second->GetNbytes()));
        attr_data.insert(std::make_pair(attr_it->first, attr_it->second->GetData()));

        if (segment_to_merge->deleted_docs_ptr_ != nullptr) {
            auto offsets_to_delete = segment_to_merge->deleted_docs_ptr_->GetDeletedDocs();

            // Erase from field data
            attr_it->second->Erase(offsets_to_delete);
        }
    }
    AddAttrs(name, attr_nbytes, attr_data, segment_to_merge->vectors_ptr_->GetUids());

    LOG_ENGINE_DEBUG_ << "Merging completed from " << dir_to_merge << " to " << fs_ptr_->operation_ptr_->GetDirectory();

    return Status::OK();
}

size_t
SegmentWriter::Size() {
    // TODO(zhiru): switch to actual directory size
    size_t vectors_size = segment_ptr_->vectors_ptr_->VectorsSize();
    size_t uids_size = segment_ptr_->vectors_ptr_->UidsSize();
    /*
    if (segment_ptr_->id_bloom_filter_ptr_) {
        ret += segment_ptr_->id_bloom_filter_ptr_->Size();
    }
     */
    return (vectors_size * sizeof(uint8_t) + uids_size * sizeof(doc_id_t));
}

size_t
SegmentWriter::VectorCount() {
    return segment_ptr_->vectors_ptr_->GetCount();
}

}  // namespace segment
}  // namespace milvus
