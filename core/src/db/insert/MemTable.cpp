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

#include "db/insert/MemTable.h"

#include <cache/CpuCacheMgr.h>
#include <segment/SegmentReader.h>
#include <wrapper/VecIndex.h>

#include <memory>
#include <string>

#include "db/Utils.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {

MemTable::MemTable(const std::string& table_id, const meta::MetaPtr& meta, const DBOptions& options)
    : table_id_(table_id), meta_(meta), options_(options) {
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
            MemTableFilePtr new_mem_table_file = std::make_shared<MemTableFile>(table_id_, meta_, options_);
            status = new_mem_table_file->Add(source);
            if (status.ok()) {
                mem_table_file_list_.emplace_back(new_mem_table_file);
            }
        } else {
            status = current_mem_table_file->Add(source);
        }

        if (!status.ok()) {
            std::string err_msg = "Insert failed: " + status.ToString();
            ENGINE_LOG_ERROR << err_msg;
            return Status(DB_ERROR, err_msg);
        }
    }
    return Status::OK();
}

Status
MemTable::Delete(segment::doc_id_t doc_id) {
    // Locate which table file the doc id lands in
    for (auto& table_file : mem_table_file_list_) {
        table_file->Delete(doc_id);
    }
    // Add the id to delete list so it can be applied to other segments on disk during the next flush
    doc_ids_to_delete_.insert(doc_id);

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
MemTable::Serialize(uint64_t wal_lsn) {
    auto status = ApplyDeletes();
    if (!status.ok()) {
        return Status(DB_ERROR, status.message());
    }

    for (auto mem_table_file = mem_table_file_list_.begin(); mem_table_file != mem_table_file_list_.end();) {
        status = (*mem_table_file)->Serialize(wal_lsn);
        if (!status.ok()) {
            std::string err_msg = "Insert data serialize failed: " + status.ToString();
            ENGINE_LOG_ERROR << err_msg;
            return Status(DB_ERROR, err_msg);
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            mem_table_file = mem_table_file_list_.erase(mem_table_file);
        }
        // Update flush lsn
        status = meta_->UpdateTableFlushLSN(table_id_, wal_lsn);
        if (!status.ok()) {
            std::string err_msg = "Failed to write flush lsn to meta: " + status.ToString();
            ENGINE_LOG_ERROR << err_msg;
            return Status(DB_ERROR, err_msg);
        }
    }
    return Status::OK();
}

bool
MemTable::Empty() {
    return mem_table_file_list_.empty();
}

const std::string&
MemTable::GetTableId() const {
    return table_id_;
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
    // For each segment in table:
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

    std::vector<int> file_types{meta::TableFileSchema::FILE_TYPE::RAW, meta::TableFileSchema::FILE_TYPE::TO_INDEX,
                                meta::TableFileSchema::FILE_TYPE::BACKUP};
    meta::TableFilesSchema table_files_schema;
    auto status = meta_->FilesByType(table_id_, file_types, table_files_schema);
    if (!status.ok()) {
        std::string err_msg = "Failed to apply deletes: " + status.ToString();
        ENGINE_LOG_ERROR << err_msg;
        return Status(DB_ERROR, err_msg);
    }
    std::unordered_map<std::string, std::vector<segment::doc_id_t>> ids_to_check_map;

    for (auto& table_file : table_files_schema) {
        std::string segment_dir;
        utils::GetParentPath(table_file.location_, segment_dir);

        segment::SegmentReader segment_reader(segment_dir);
        segment::IdBloomFilterPtr id_bloom_filter_ptr;
        segment_reader.LoadBloomFilter(id_bloom_filter_ptr);

        for (auto& id : doc_ids_to_delete_) {
            if (id_bloom_filter_ptr->Check(id)) {
                ids_to_check_map[table_file.location_].emplace_back(id);
            }
        }
    }

    for (auto& kv : ids_to_check_map) {
        std::string segment_dir;
        utils::GetParentPath(kv.first, segment_dir);
        segment::SegmentReader segment_reader(segment_dir);

        auto index = std::static_pointer_cast<VecIndex>(cache::CpuCacheMgr::GetInstance()->GetIndex(kv.first));
        faiss::ConcurrentBitsetPtr blacklist = nullptr;
        if (index != nullptr) {
            status = index->GetBlacklist(blacklist);
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

        for (size_t i = 0; i < uids.size(); ++i) {
            if (std::find(ids_to_check.begin(), ids_to_check.end(), uids[i]) != ids_to_check.end()) {
                deleted_docs->AddDeletedDoc(i);
                id_bloom_filter_ptr->Remove(uids[i]);

                if (blacklist != nullptr) {
                    if (!blacklist->test(i)) {
                        blacklist->set(i);
                    }
                }
            }
        }

        if (index != nullptr) {
            index->SetBlacklist(blacklist);
        }

        segment::Segment tmp_segment;
        segment::SegmentWriter segment_writer(segment_dir);
        status = segment_writer.WriteDeletedDocs(deleted_docs);
        if (!status.ok()) {
            break;
        }
        status = segment_writer.WriteBloomFilter(id_bloom_filter_ptr);
        if (!status.ok()) {
            break;
        }
    }

    if (!status.ok()) {
        std::string err_msg = "Failed to apply deletes: " + status.ToString();
        ENGINE_LOG_ERROR << err_msg;
        return Status(DB_ERROR, err_msg);
    }

    doc_ids_to_delete_.clear();

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

}  // namespace engine
}  // namespace milvus
