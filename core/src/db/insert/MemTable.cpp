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

#include <memory>
#include <string>

#include "utils/Log.h"

namespace milvus {
namespace engine {

MemTable::MemTable(const std::string& table_id, const meta::MetaPtr& meta, const DBOptions& options)
    : table_id_(table_id), meta_(meta), options_(options) {
}

Status
MemTable::Add(VectorSourcePtr& source) {
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
        // TODO(zhiru):
        // Use bloom filter to check whether the id is present in this table file
        // If present:
        table_file->Delete(doc_id);
    }
    // TODO(zhiru): Add the id to delete list (should it be on table level?)
    // so it can be applied to other segments on disk during the next flush
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
    // TODO(zhiru): applying deletes to other segments on disk
    // Foreach id in delete list:
    //     Foreach segment in table:
    //         Load its bloom filter
    //         If present, add the uid to segment's uid list
    // Foreach segment
    //     Load its uids file.
    //     Scan the uids, if any uid in segment's uid list exists, add its offset to deletedDoc
    //     Serialize segment's deletedDoc (append directly to previous file or make a new file and merge them after
    //     written to meta?)

    for (auto mem_table_file = mem_table_file_list_.begin(); mem_table_file != mem_table_file_list_.end();) {
        auto status = (*mem_table_file)->Serialize(wal_lsn);
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

}  // namespace engine
}  // namespace milvus
