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

#include "SegmentReader.h"
#include "Vectors.h"
#include "codecs/default/DefaultCodec.h"
#include "store/Directory.h"
#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace segment {

SegmentWriter::SegmentWriter(const std::string& directory) {
    directory_ptr_ = std::make_shared<store::Directory>(directory);
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
SegmentWriter::Serialize() {
    // TODO(zhiru)
    auto status = WriteVectors();
    if (!status.ok()) {
        return status;
    }
    status = WriteBloomFilter();
    if (!status.ok()) {
        return status;
    }
    // Write an empty deleted doc
    status = WriteDeletedDocs();
    return status;
}

Status
SegmentWriter::WriteVectors() {
    codec::DefaultCodec default_codec;
    try {
        directory_ptr_->Create();
        default_codec.GetVectorsFormat()->write(directory_ptr_, segment_ptr_->vectors_ptr_);
    } catch (Exception& e) {
        std::string err_msg = "Failed to write vectors. " + std::string(e.what());
        ENGINE_LOG_ERROR << err_msg;
        return Status(e.code(), err_msg);
    }
    return Status::OK();
}

Status
SegmentWriter::WriteBloomFilter() {
    codec::DefaultCodec default_codec;
    try {
        directory_ptr_->Create();
        default_codec.GetIdBloomFilterFormat()->create(directory_ptr_, segment_ptr_->id_bloom_filter_ptr_);
        auto& uids = segment_ptr_->vectors_ptr_->GetUids();
        for (auto& uid : uids) {
            segment_ptr_->id_bloom_filter_ptr_->Add(uid);
        }
        default_codec.GetIdBloomFilterFormat()->write(directory_ptr_, segment_ptr_->id_bloom_filter_ptr_);
    } catch (Exception& e) {
        std::string err_msg = "Failed to write vectors. " + std::string(e.what());
        ENGINE_LOG_ERROR << err_msg;
        return Status(e.code(), err_msg);
    }
    return Status::OK();
}

Status
SegmentWriter::WriteDeletedDocs() {
    codec::DefaultCodec default_codec;
    try {
        directory_ptr_->Create();
        DeletedDocsPtr deleted_docs_ptr = std::make_shared<DeletedDocs>();
        default_codec.GetDeletedDocsFormat()->write(directory_ptr_, deleted_docs_ptr);
    } catch (Exception& e) {
        std::string err_msg = "Failed to write deleted docs. " + std::string(e.what());
        ENGINE_LOG_ERROR << err_msg;
        return Status(e.code(), err_msg);
    }
    return Status::OK();
}

Status
SegmentWriter::WriteDeletedDocs(const DeletedDocsPtr& deleted_docs) {
    codec::DefaultCodec default_codec;
    try {
        directory_ptr_->Create();
        default_codec.GetDeletedDocsFormat()->write(directory_ptr_, deleted_docs);
    } catch (Exception& e) {
        std::string err_msg = "Failed to write deleted docs. " + std::string(e.what());
        ENGINE_LOG_ERROR << err_msg;
        return Status(e.code(), err_msg);
    }
    return Status::OK();
}

Status
SegmentWriter::WriteBloomFilter(const IdBloomFilterPtr& id_bloom_filter_ptr) {
    codec::DefaultCodec default_codec;
    try {
        directory_ptr_->Create();
        default_codec.GetIdBloomFilterFormat()->write(directory_ptr_, id_bloom_filter_ptr);
    } catch (Exception& e) {
        std::string err_msg = "Failed to write bloom filter. " + std::string(e.what());
        ENGINE_LOG_ERROR << err_msg;
        return Status(e.code(), err_msg);
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
    if (dir_to_merge == directory_ptr_->GetDirPath()) {
        return Status(DB_ERROR, "Cannot Merge Self");
    }

    SegmentReader segment_reader_to_merge(dir_to_merge);
    bool in_cache;
    auto status = segment_reader_to_merge.LoadCache(in_cache);
    if (!in_cache) {
        status = segment_reader_to_merge.Load();
        if (!status.ok()) {
            std::string msg = "Failed to load segment from " + dir_to_merge;
            ENGINE_LOG_ERROR << msg;
            return Status(DB_ERROR, msg);
        }
    }
    SegmentPtr segment_to_merge;
    segment_reader_to_merge.GetSegment(segment_to_merge);
    auto& uids = segment_to_merge->vectors_ptr_->GetUids();

    if (segment_to_merge->deleted_docs_ptr_ != nullptr) {
        auto offsets_to_delete = segment_to_merge->deleted_docs_ptr_->GetDeletedDocs();

        // Sort and remove duplicates
        std::sort(offsets_to_delete.begin(), offsets_to_delete.end());
        offsets_to_delete.erase(std::unique(offsets_to_delete.begin(), offsets_to_delete.end()),
                                offsets_to_delete.end());

        // Erase from raw data
        size_t deleted_count = 0;
        for (auto& offset : offsets_to_delete) {
            segment_to_merge->vectors_ptr_->Erase(offset - deleted_count);
            deleted_count++;
        }
    }

    AddVectors(name, segment_to_merge->vectors_ptr_->GetData(), segment_to_merge->vectors_ptr_->GetUids());

    return Status::OK();
}

size_t
SegmentWriter::Size() {
    // TODO(zhiru): switch to actual directory size
    size_t ret = segment_ptr_->vectors_ptr_->Size();
    if (segment_ptr_->id_bloom_filter_ptr_) {
        ret += segment_ptr_->id_bloom_filter_ptr_->Size();
    }
    return ret;
}

size_t
SegmentWriter::VectorCount() {
    return segment_ptr_->vectors_ptr_->GetCount();
}

}  // namespace segment
}  // namespace milvus
