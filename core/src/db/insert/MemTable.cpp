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
#include <unordered_map>

#include "cache/CpuCacheMgr.h"
#include "db/OngoingFileChecker.h"
#include "db/Utils.h"
#include "db/insert/MemTable.h"
#include "knowhere/index/vector_index/VecIndex.h"
#include "segment/SegmentReader.h"
#include "utils/Log.h"

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
MemTable::AddEntities(const milvus::engine::VectorSourcePtr& source) {
    while (!source->AllAdded()) {
        MemTableFilePtr current_mem_table_file;
        if (!mem_table_file_list_.empty()) {
            current_mem_table_file = mem_table_file_list_.back();
        }

        Status status;
        if (mem_table_file_list_.empty() || current_mem_table_file->IsFull()) {
            MemTableFilePtr new_mem_table_file = std::make_shared<MemTableFile>(collection_id_, meta_, options_);
            status = new_mem_table_file->AddEntities(source);
            if (status.ok()) {
                mem_table_file_list_.emplace_back(new_mem_table_file);
            }
        } else {
            status = current_mem_table_file->AddEntities(source);
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
    auto start = std::chrono::high_resolution_clock::now();

    if (!doc_ids_to_delete_.empty() && apply_delete) {
        auto status = ApplyDeletes();
        if (!status.ok()) {
            return Status(DB_ERROR, status.message());
        }
    }

    for (auto mem_table_file = mem_table_file_list_.begin(); mem_table_file != mem_table_file_list_.end();) {
        auto status = (*mem_table_file)->Serialize(wal_lsn);
        if (!status.ok()) {
            return status;
        }

        LOG_ENGINE_DEBUG_ << "Flushed segment " << (*mem_table_file)->GetSegmentId();

        {
            std::lock_guard<std::mutex> lock(mutex_);
            mem_table_file = mem_table_file_list_.erase(mem_table_file);
        }
    }

    // Update flush lsn
    auto status = meta_->UpdateCollectionFlushLSN(collection_id_, wal_lsn);
    if (!status.ok()) {
        std::string err_msg = "Failed to write flush lsn to meta: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;
    LOG_ENGINE_DEBUG_ << "Finished flushing for collection " << collection_id_ << " in " << diff.count() << " s";

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
    // Applying deletes to other segments on disk and their corresponding cache:
    // For each segment in collection:
    //     Load its bloom filter
    //     For each id in delete list:
    //         If present, add the uid to segment's uid list
    // For each segment
    //     Get its cache if exists
    //     Load its uids file.
    //     Scan the uids, if any uid in segment's uid list exists:
    //         add its offset to deletedDoc
    //         remove the id from bloom filter
    //         set black list in cache
    //     Serialize segment's deletedDoc TODO(zhiru): append directly to previous file for now, may have duplicates
    //     Serialize bloom filter

    LOG_ENGINE_DEBUG_ << "Applying " << doc_ids_to_delete_.size() << " deletes in collection: " << collection_id_;

    auto start_total = std::chrono::high_resolution_clock::now();

    //    auto start = std::chrono::high_resolution_clock::now();

    std::vector<int> file_types{meta::SegmentSchema::FILE_TYPE::RAW, meta::SegmentSchema::FILE_TYPE::TO_INDEX,
                                meta::SegmentSchema::FILE_TYPE::BACKUP};
    meta::SegmentsSchema table_files;
    auto status = meta_->FilesByType(collection_id_, file_types, table_files);
    if (!status.ok()) {
        std::string err_msg = "Failed to apply deletes: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    OngoingFileChecker::GetInstance().MarkOngoingFiles(table_files);

    std::unordered_map<size_t, std::vector<segment::doc_id_t>> ids_to_check_map;

    for (size_t i = 0; i < table_files.size(); ++i) {
        auto& table_file = table_files[i];
        std::string segment_dir;
        utils::GetParentPath(table_file.location_, segment_dir);

        segment::SegmentReader segment_reader(segment_dir);
        segment::IdBloomFilterPtr id_bloom_filter_ptr;
        segment_reader.LoadBloomFilter(id_bloom_filter_ptr);

        for (auto& id : doc_ids_to_delete_) {
            if (id_bloom_filter_ptr->Check(id)) {
                ids_to_check_map[i].emplace_back(id);
            }
        }
    }

    meta::SegmentsSchema files_to_check;
    for (auto& kv : ids_to_check_map) {
        files_to_check.emplace_back(table_files[kv.first]);
    }

    OngoingFileChecker::GetInstance().UnmarkOngoingFiles(table_files);

    OngoingFileChecker::GetInstance().MarkOngoingFiles(files_to_check);

    auto time0 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff0 = time0 - start_total;
    LOG_ENGINE_DEBUG_ << "Found " << ids_to_check_map.size() << " segment to apply deletes in " << diff0.count()
                      << " s";

    meta::SegmentsSchema table_files_to_update;

    for (auto& kv : ids_to_check_map) {
        auto& table_file = table_files[kv.first];
        LOG_ENGINE_DEBUG_ << "Applying deletes in segment: " << table_file.segment_id_;

        auto time1 = std::chrono::high_resolution_clock::now();

        std::string segment_dir;
        utils::GetParentPath(table_file.location_, segment_dir);
        segment::SegmentReader segment_reader(segment_dir);

        auto& segment_id = table_file.segment_id_;
        meta::SegmentsSchema segment_files;
        status = meta_->GetCollectionFilesBySegmentId(segment_id, segment_files);
        if (!status.ok()) {
            break;
        }

        // Get all index that contains blacklist in cache
        std::vector<knowhere::VecIndexPtr> indexes;
        std::vector<faiss::ConcurrentBitsetPtr> blacklists;
        for (auto& file : segment_files) {
            auto data_obj_ptr = cache::CpuCacheMgr::GetInstance()->GetIndex(file.location_);
            auto index = std::static_pointer_cast<knowhere::VecIndex>(data_obj_ptr);
            if (index != nullptr) {
                faiss::ConcurrentBitsetPtr blacklist = index->GetBlacklist();
                if (blacklist != nullptr) {
                    indexes.emplace_back(index);
                    blacklists.emplace_back(blacklist);
                }
            }
        }

        std::vector<segment::doc_id_t> uids;
        status = segment_reader.LoadUids(uids);
        if (!status.ok()) {
            break;
        }
        segment::IdBloomFilterPtr id_bloom_filter_ptr;
        status = segment_reader.LoadBloomFilter(id_bloom_filter_ptr);
        if (!status.ok()) {
            break;
        }

        auto& ids_to_check = kv.second;

        segment::DeletedDocsPtr deleted_docs = std::make_shared<segment::DeletedDocs>();

        auto time2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff1 = time2 - time1;
        LOG_ENGINE_DEBUG_ << "Loading uids and deleted docs took " << diff1.count() << " s";

        std::sort(ids_to_check.begin(), ids_to_check.end());

        auto time3 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff2 = time3 - time2;
        LOG_ENGINE_DEBUG_ << "Sorting " << ids_to_check.size() << " ids took " << diff2.count() << " s";

        size_t delete_count = 0;
        auto find_diff = std::chrono::duration<double>::zero();
        auto set_diff = std::chrono::duration<double>::zero();

        for (size_t i = 0; i < uids.size(); ++i) {
            auto find_start = std::chrono::high_resolution_clock::now();

            auto found = std::binary_search(ids_to_check.begin(), ids_to_check.end(), uids[i]);

            auto find_end = std::chrono::high_resolution_clock::now();
            find_diff += (find_end - find_start);

            if (found) {
                auto set_start = std::chrono::high_resolution_clock::now();

                delete_count++;

                deleted_docs->AddDeletedDoc(i);

                if (id_bloom_filter_ptr->Check(uids[i])) {
                    id_bloom_filter_ptr->Remove(uids[i]);
                }

                for (auto& blacklist : blacklists) {
                    if (!blacklist->test(i)) {
                        blacklist->set(i);
                    }
                }

                auto set_end = std::chrono::high_resolution_clock::now();
                set_diff += (set_end - set_start);
            }
        }

        LOG_ENGINE_DEBUG_ << "Finding " << ids_to_check.size() << " uids in " << uids.size() << " uids took "
                          << find_diff.count() << " s in total";
        LOG_ENGINE_DEBUG_ << "Setting deleted docs and bloom filter took " << set_diff.count() << " s in total";

        auto time4 = std::chrono::high_resolution_clock::now();

        for (auto i = 0; i < indexes.size(); ++i) {
            indexes[i]->SetBlacklist(blacklists[i]);
        }

        //        start = std::chrono::high_resolution_clock::now();

        segment::Segment tmp_segment;
        segment::SegmentWriter segment_writer(segment_dir);
        status = segment_writer.WriteDeletedDocs(deleted_docs);
        if (!status.ok()) {
            break;
        }

        auto time5 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff4 = time5 - time4;
        LOG_ENGINE_DEBUG_ << "Appended " << deleted_docs->GetSize()
                          << " offsets to deleted docs in segment: " << table_file.segment_id_ << " in "
                          << diff4.count() << " s";

        //        start = std::chrono::high_resolution_clock::now();

        status = segment_writer.WriteBloomFilter(id_bloom_filter_ptr);
        if (!status.ok()) {
            break;
        }
        auto time6 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff5 = time6 - time5;
        LOG_ENGINE_DEBUG_ << "Updated bloom filter in segment: " << table_file.segment_id_ << " in " << diff5.count()
                          << " s";

        // Update collection file row count
        for (auto& file : segment_files) {
            if (file.file_type_ == meta::SegmentSchema::RAW || file.file_type_ == meta::SegmentSchema::TO_INDEX ||
                file.file_type_ == meta::SegmentSchema::INDEX || file.file_type_ == meta::SegmentSchema::BACKUP) {
                file.row_count_ -= delete_count;
                table_files_to_update.emplace_back(file);
            }
        }
        auto time7 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff6 = time7 - time6;
        diff6 = time6 - time5;
        LOG_ENGINE_DEBUG_ << "Update collection file row count in vector of segment: " << table_file.segment_id_
                          << " in " << diff6.count() << " s";
    }

    auto time7 = std::chrono::high_resolution_clock::now();

    status = meta_->UpdateCollectionFilesRowCount(table_files_to_update);

    if (!status.ok()) {
        std::string err_msg = "Failed to apply deletes: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    doc_ids_to_delete_.clear();

    auto end_total = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff7 = end_total - time7;
    LOG_ENGINE_DEBUG_ << "Update deletes to meta in collection " << collection_id_ << " in " << diff7.count() << " s";
    std::chrono::duration<double> diff_total = end_total - start_total;
    LOG_ENGINE_DEBUG_ << "Finished deletes in collection " << collection_id_ << " in " << diff_total.count() << " s";

    OngoingFileChecker::GetInstance().UnmarkOngoingFiles(files_to_check);

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
