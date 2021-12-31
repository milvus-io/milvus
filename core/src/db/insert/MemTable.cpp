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

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

#include "cache/CpuCacheMgr.h"
#include "codecs/default/DefaultCodec.h"
#include "db/Utils.h"
#include "db/insert/MemTable.h"
#include "db/meta/FilesHolder.h"
#include "knowhere/index/vector_index/VecIndex.h"
#include "segment/SegmentReader.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace engine {

MemTable::MemTable(const std::string& collection_id, const meta::MetaPtr& meta, const DBOptions& options)
    : collection_id_(collection_id), meta_(meta), options_(options) {
    SetIdentity("MemTable");
    AddCacheInsertDataListener();
}

Status
MemTable::Add(const VectorSourcePtr& source) {
    while (!source->AllAdded()) {
        MemTableFilePtr current_mem_table_file;
        if (!mem_table_file_list_.empty()) {
            current_mem_table_file = mem_table_file_list_.back();
        }

        Status status;
        if (mem_table_file_list_.empty() || current_mem_table_file->IsFull()) {
            MemTableFilePtr new_mem_table_file = std::make_shared<MemTableFile>(collection_id_, meta_, options_);
            status = new_mem_table_file->Add(source);
            if (status.ok()) {
                mem_table_file_list_.emplace_back(new_mem_table_file);
            }
        } else {
            status = current_mem_table_file->Add(source);
        }

        if (!status.ok()) {
            std::string err_msg = "Insert failed: " + status.ToString();
            LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] ", "insert", 0) << err_msg;
            return Status(DB_ERROR, err_msg);
        }
    }
    return Status::OK();
}

Status
MemTable::Delete(segment::doc_id_t doc_id) {
    // Locate which collection file the doc id lands in
    for (auto& table_file : mem_table_file_list_) {
        table_file->Delete(doc_id);
    }
    // Add the id to delete list so it can be applied to other segments on disk during the next flush
    doc_ids_to_delete_.insert(doc_id);

    return Status::OK();
}

Status
MemTable::Delete(const std::vector<segment::doc_id_t>& doc_ids) {
    // Locate which collection file the doc id lands in
    for (auto& table_file : mem_table_file_list_) {
        table_file->Delete(doc_ids);
    }
    // Add the id to delete list so it can be applied to other segments on disk during the next flush
    for (auto& id : doc_ids) {
        doc_ids_to_delete_.insert(id);
    }

    return Status::OK();
}

void
MemTable::GetCurrentMemTableFile(MemTableFilePtr& mem_table_file) {
    mem_table_file = mem_table_file_list_.back();
}

size_t
MemTable::GetTableFileCount() {
    return mem_table_file_list_.size();
}

Status
MemTable::Serialize(uint64_t wal_lsn, bool apply_delete) {
    TimeRecorder recorder("MemTable::Serialize collection " + collection_id_);

    // The ApplyDeletes() do two things
    // 1. delete vectors from buffer
    // 2. delete vectors from storage
    if (!doc_ids_to_delete_.empty() && apply_delete) {
        auto status = ApplyDeletes();
        if (!status.ok()) {
            return Status(DB_ERROR, status.message());
        }
    }

    meta::SegmentsSchema update_files;
    for (auto mem_table_file = mem_table_file_list_.begin(); mem_table_file != mem_table_file_list_.end();) {
        // For empty segment
        if ((*mem_table_file)->Empty()) {
            // Mark the empty segment as to_delete so that the meta system can remove it later
            auto schema = (*mem_table_file)->GetSegmentSchema();
            schema.file_type_ = meta::SegmentSchema::TO_DELETE;
            update_files.push_back(schema);

            std::lock_guard<std::mutex> lock(mutex_);
            mem_table_file = mem_table_file_list_.erase(mem_table_file);
            continue;
        }

        auto status = (*mem_table_file)->Serialize(wal_lsn);
        auto schema = (*mem_table_file)->GetSegmentSchema();
        if (!status.ok()) {
            // mark the failed segment as to_delete so that the meta system can remove it later
            schema.file_type_ = meta::SegmentSchema::TO_DELETE;
            meta_->UpdateCollectionFile(schema);
            return status;
        }

        // succeed, record the segment into meta by UpdateCollectionFiles()
        update_files.push_back(schema);
        LOG_ENGINE_DEBUG_ << "Flushed segment " << (*mem_table_file)->GetSegmentId();

        {
            std::lock_guard<std::mutex> lock(mutex_);
            mem_table_file = mem_table_file_list_.erase(mem_table_file);
        }
    }

    // Update meta files and flush lsn
    auto status = meta_->UpdateCollectionFiles(update_files);
    if (!status.ok()) {
        return status;
    }

    // Record WAL flag
    status = meta_->UpdateCollectionFlushLSN(collection_id_, wal_lsn);
    if (!status.ok()) {
        std::string err_msg = "Failed to write flush lsn to meta: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    recorder.RecordSection("Finished flushing");

    return Status::OK();
}

bool
MemTable::Empty() {
    return mem_table_file_list_.empty() && doc_ids_to_delete_.empty();
}

const std::string&
MemTable::GetTableId() const {
    return collection_id_;
}

size_t
MemTable::GetCurrentMem() {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t total_mem = 0;
    for (auto& mem_table_file : mem_table_file_list_) {
        total_mem += mem_table_file->GetCurrentMem();
    }
    return total_mem;
}

Status
MemTable::ApplyDeletes() {
    // Applying deletes to other segments on disk:
    // For each segment in collection:
    //     Load its bloom filter
    //     For each id in delete list:
    //         If present, add the uid to segment's delete list
    //     if segment delete list is empty
    //         continue
    //     Load its uids and deleted docs file
    //     Scan the uids, if any un-deleted uid in segment's delete list
    //         add its offset to deletedDoc
    //         remove the id from bloom filter
    //     Serialize segment's deletedDoc
    //     Serialize bloom filter

    LOG_ENGINE_DEBUG_ << "Applying " << doc_ids_to_delete_.size() << " deletes in collection: " << collection_id_;

    TimeRecorder recorder("MemTable::ApplyDeletes for collection " + collection_id_);

    std::vector<int> file_types{meta::SegmentSchema::FILE_TYPE::RAW, meta::SegmentSchema::FILE_TYPE::TO_INDEX,
                                meta::SegmentSchema::FILE_TYPE::BACKUP};
    meta::FilesHolder files_holder;
    auto status = meta_->FilesByType(collection_id_, file_types, files_holder);
    if (!status.ok()) {
        std::string err_msg = "Failed to apply deletes: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    // attention: here is a copy, not reference, since files_holder.UnmarkFile will change the array internal
    milvus::engine::meta::SegmentsSchema files = files_holder.HoldFiles();

    meta::SegmentsSchema files_to_update;

    for (auto& file : files) {
        LOG_ENGINE_DEBUG_ << "Applying deletes in segment: " << file.segment_id_;
        if (file.row_count_ == 0) {
            LOG_ENGINE_DEBUG_ << "Applying deletes in segment: segment is empty, skip";
            continue;
        }

        segment::IdBloomFilterPtr id_bloom_filter_ptr = nullptr;
        segment::UidsPtr uids_ptr = nullptr;
        segment::DeletedDocsPtr deleted_docs_ptr = nullptr;

        std::unordered_set<segment::doc_id_t> ids_to_check;

        TimeRecorder rec("handle segment " + file.segment_id_);

        // segment reader
        std::string segment_dir;
        utils::GetParentPath(file.location_, segment_dir);
        segment::SegmentReader segment_reader(segment_dir);

        // prepare segment_files
        meta::FilesHolder segment_holder;
        status = meta_->GetCollectionFilesBySegmentId(file.segment_id_, segment_holder);
        if (!status.ok()) {
            break;
        }
        milvus::engine::meta::SegmentsSchema& segment_files = segment_holder.HoldFiles();

        // Lamda: LoadUid
        auto LoadUid = [&]() {
            for (auto& segment_file : segment_files) {
                auto data_obj_ptr = cache::CpuCacheMgr::GetInstance()->GetItem(segment_file.location_);
                auto index = std::static_pointer_cast<knowhere::VecIndex>(data_obj_ptr);
                if (index != nullptr) {
                    uids_ptr = index->GetUids();
                    return Status::OK();
                }
            }

            return segment_reader.LoadUids(uids_ptr);
        };

        // Lamda: LoadDeleteDoc
        auto LoadDeleteDoc = [&]() { return segment_reader.LoadDeletedDocs(deleted_docs_ptr); };

        // load bloom filter
        status = segment_reader.LoadBloomFilter(id_bloom_filter_ptr, true);
        if (!status.ok()) {
            // Some accidents may cause the bloom filter file destroyed.
            // If failed to load bloom filter, just to create a new one.
            if (!(status = LoadUid()).ok()) {
                return status;
            }
            if (!(status = LoadDeleteDoc()).ok()) {
                return status;
            }

            codec::DefaultCodec default_codec;
            default_codec.GetIdBloomFilterFormat()->create(uids_ptr->size(), id_bloom_filter_ptr);
            id_bloom_filter_ptr->Add(*uids_ptr, deleted_docs_ptr->GetMutableDeletedDocs());
            LOG_ENGINE_DEBUG_ << "A new bloom filter is created";

            segment::SegmentWriter segment_writer(segment_dir);
            segment_writer.WriteBloomFilter(id_bloom_filter_ptr);
        }

        // check ids by bloom filter
        for (auto& id : doc_ids_to_delete_) {
            if (id_bloom_filter_ptr->Check(id)) {
                ids_to_check.emplace(id);
            }
        }

        rec.RecordSection("bloom filter check end, segment delete list cnt " + std::to_string(ids_to_check.size()));

        // release unused files
        if (ids_to_check.empty()) {
            files_holder.UnmarkFile(file);
            continue;
        }

        // Load its uids and deleted docs file
        if (uids_ptr == nullptr && !(status = LoadUid()).ok()) {
            return status;
        }
        if (deleted_docs_ptr == nullptr && !(status = LoadDeleteDoc()).ok()) {
            return status;
        }
        auto& deleted_docs = deleted_docs_ptr->GetMutableDeletedDocs();

        rec.RecordSection("load uids and deleted docs");

        // insert deleted docs to bitset
        auto deleted_bitset_ptr = std::make_shared<faiss::ConcurrentBitset>(uids_ptr->size());
        for (auto& offset : deleted_docs) {
            deleted_bitset_ptr->set(offset);
        }

        // for each id
        int64_t segment_deleted_count = 0;
        for (size_t i = 0; i < uids_ptr->size(); ++i) {
            if (deleted_bitset_ptr->test(i)) {
                continue;
            }

            if (ids_to_check.find((*uids_ptr)[i]) == ids_to_check.end()) {
                continue;
            }

            // delete
            id_bloom_filter_ptr->Remove((*uids_ptr)[i]);
            deleted_docs.push_back(i);
            segment_deleted_count++;
        }

        rec.RecordSection("Find uids and set deleted docs and bloom filter, append " +
                          std::to_string(segment_deleted_count) + " offsets");

        if (segment_deleted_count == 0) {
            LOG_ENGINE_DEBUG_ << "deleted_docs does not need to be updated";
            files_holder.UnmarkFile(file);
            continue;
        }

        segment::Segment tmp_segment;
        segment::SegmentWriter segment_writer(segment_dir);
        status = segment_writer.WriteDeletedDocs(deleted_docs_ptr);
        if (!status.ok()) {
            break;
        }

        rec.RecordSection("Updated deleted docs");

        status = segment_writer.WriteBloomFilter(id_bloom_filter_ptr);
        if (!status.ok()) {
            break;
        }

        rec.RecordSection("Updated bloom filter");

        // Update collection file row count
        for (auto& segment_file : segment_files) {
            if (segment_file.file_type_ == meta::SegmentSchema::RAW ||
                segment_file.file_type_ == meta::SegmentSchema::TO_INDEX ||
                segment_file.file_type_ == meta::SegmentSchema::INDEX ||
                segment_file.file_type_ == meta::SegmentSchema::BACKUP) {
                segment_file.row_count_ -= segment_deleted_count;
                files_to_update.emplace_back(segment_file);
            }
        }
        rec.RecordSection("Update collection file row count in vector");
    }

    recorder.RecordSection("Finished " + std::to_string(files.size()) + " segment to apply deletes");

    status = meta_->UpdateCollectionFilesRowCount(files_to_update);

    if (!status.ok()) {
        std::string err_msg = "Failed to apply deletes: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    doc_ids_to_delete_.clear();

    recorder.RecordSection("Update deletes to meta");
    recorder.ElapseFromBegin("Finished deletes");

    return Status::OK();
}

uint64_t
MemTable::GetLSN() {
    return lsn_;
}

void
MemTable::SetLSN(uint64_t lsn) {
    lsn_ = lsn;
}

void
MemTable::OnCacheInsertDataChanged(bool value) {
    options_.insert_cache_immediately_ = value;
}

}  // namespace engine
}  // namespace milvus
