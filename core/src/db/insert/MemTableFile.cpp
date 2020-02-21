// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "db/insert/MemTableFile.h"

#include <algorithm>
#include <cmath>
#include <iterator>
#include <string>
#include <vector>

#include "db/Constants.h"
#include "db/Utils.h"
#include "db/engine/EngineFactory.h"
#include "metrics/Metrics.h"
#include "segment/SegmentReader.h"
#include "utils/Log.h"
#include "utils/ValidationUtil.h"

namespace milvus {
namespace engine {

MemTableFile::MemTableFile(const std::string& table_id, const meta::MetaPtr& meta, const DBOptions& options)
    : table_id_(table_id), meta_(meta), options_(options) {
    current_mem_ = 0;
    auto status = CreateTableFile();
    if (status.ok()) {
        /*execution_engine_ = EngineFactory::Build(
            table_file_schema_.dimension_, table_file_schema_.location_, (EngineType)table_file_schema_.engine_type_,
            (MetricType)table_file_schema_.metric_type_, table_file_schema_.nlist_);*/
        std::string directory;
        utils::GetParentPath(table_file_schema_.location_, directory);
        segment_writer_ptr_ = std::make_shared<segment::SegmentWriter>(directory);
    }
}

Status
MemTableFile::CreateTableFile() {
    meta::TableFileSchema table_file_schema;
    table_file_schema.table_id_ = table_id_;
    auto status = meta_->CreateTableFile(table_file_schema);
    if (status.ok()) {
        table_file_schema_ = table_file_schema;
    } else {
        std::string err_msg = "MemTableFile::CreateTableFile failed: " + status.ToString();
        ENGINE_LOG_ERROR << err_msg;
    }
    return status;
}

Status
MemTableFile::Add(const VectorSourcePtr& source) {
    if (table_file_schema_.dimension_ <= 0) {
        std::string err_msg =
            "MemTableFile::Add: table_file_schema dimension = " + std::to_string(table_file_schema_.dimension_) +
            ", table_id = " + table_file_schema_.table_id_;
        ENGINE_LOG_ERROR << err_msg;
        return Status(DB_ERROR, "Not able to create table file");
    }

    size_t single_vector_mem_size = source->SingleVectorSize(table_file_schema_.dimension_);
    size_t mem_left = GetMemLeft();
    if (mem_left >= single_vector_mem_size) {
        size_t num_vectors_to_add = std::ceil(mem_left / single_vector_mem_size);
        size_t num_vectors_added;

        auto status = source->Add(/*execution_engine_,*/ segment_writer_ptr_, table_file_schema_, num_vectors_to_add,
                                  num_vectors_added);
        if (status.ok()) {
            current_mem_ += (num_vectors_added * single_vector_mem_size);
        }
        return status;
    }
    return Status::OK();
}

Status
MemTableFile::Delete(segment::doc_id_t doc_id) {
    segment::SegmentPtr segment_ptr;
    segment_writer_ptr_->GetSegment(segment_ptr);
    // Check wither the doc_id is present, if yes, delete it's corresponding buffer
    auto uids = segment_ptr->vectors_ptr_->GetUids();
    auto found = std::find(uids.begin(), uids.end(), doc_id);
    if (found != uids.end()) {
        auto offset = std::distance(uids.begin(), found);
        segment_ptr->vectors_ptr_->Erase(offset);
    }

    return Status::OK();
}

Status
MemTableFile::Delete(const std::vector<segment::doc_id_t>& doc_ids) {
    segment::SegmentPtr segment_ptr;
    segment_writer_ptr_->GetSegment(segment_ptr);
    // Check wither the doc_id is present, if yes, delete it's corresponding buffer
    auto uids = segment_ptr->vectors_ptr_->GetUids();
    for (auto& doc_id : doc_ids) {
        auto found = std::find(uids.begin(), uids.end(), doc_id);
        if (found != uids.end()) {
            auto offset = std::distance(uids.begin(), found);
            segment_ptr->vectors_ptr_->Erase(offset);
            uids = segment_ptr->vectors_ptr_->GetUids();
        }
    }

    return Status::OK();
}

size_t
MemTableFile::GetCurrentMem() {
    return current_mem_;
}

size_t
MemTableFile::GetMemLeft() {
    return (MAX_TABLE_FILE_MEM - current_mem_);
}

bool
MemTableFile::IsFull() {
    size_t single_vector_mem_size = table_file_schema_.dimension_ * FLOAT_TYPE_SIZE;
    return (GetMemLeft() < single_vector_mem_size);
}

Status
MemTableFile::Serialize(uint64_t wal_lsn) {
    size_t size = GetCurrentMem();
    server::CollectSerializeMetrics metrics(size);

    auto status = segment_writer_ptr_->Serialize();
    if (!status.ok()) {
        std::string directory;
        utils::GetParentPath(table_file_schema_.location_, directory);
        ENGINE_LOG_ERROR << "Failed to flush segment " << directory;
        return status;
    }

    //    execution_engine_->Serialize();

    // TODO(zhiru):
    //    table_file_schema_.file_size_ = execution_engine_->PhysicalSize();
    //    table_file_schema_.row_count_ = execution_engine_->Count();
    table_file_schema_.file_size_ = segment_writer_ptr_->Size();
    table_file_schema_.row_count_ = segment_writer_ptr_->VectorCount();

    // if index type isn't IDMAP, set file type to TO_INDEX if file size exceed index_file_size
    // else set file type to RAW, no need to build index
    if (table_file_schema_.engine_type_ != (int)EngineType::FAISS_IDMAP &&
        table_file_schema_.engine_type_ != (int)EngineType::FAISS_BIN_IDMAP) {
        table_file_schema_.file_type_ = (size >= table_file_schema_.index_file_size_) ? meta::TableFileSchema::TO_INDEX
                                                                                      : meta::TableFileSchema::RAW;
    } else {
        table_file_schema_.file_type_ = meta::TableFileSchema::RAW;
    }

    // Set table file's flush_lsn so WAL can roll back and delete garbage files which can be obtained from
    // GetTableFilesByFlushLSN() in meta.
    table_file_schema_.flush_lsn_ = wal_lsn;

    status = meta_->UpdateTableFile(table_file_schema_);

    ENGINE_LOG_DEBUG << "New " << ((table_file_schema_.file_type_ == meta::TableFileSchema::RAW) ? "raw" : "to_index")
                     << " file " << table_file_schema_.file_id_ << " of size " << size << " bytes, lsn = " << wal_lsn;

    // TODO(zhiru): cache
    /*
        if (options_.insert_cache_immediately_) {
            execution_engine_->Cache();
        }
    */
    if (options_.insert_cache_immediately_) {
        segment_writer_ptr_->Cache();
    }

    return status;
}

const std::string&
MemTableFile::GetSegmentId() const {
    return table_file_schema_.segment_id_;
}

}  // namespace engine
}  // namespace milvus
