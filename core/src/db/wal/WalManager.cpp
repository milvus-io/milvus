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

#include "db/wal/WalManager.h"
#include "db/Utils.h"
#include "db/wal/WalOperationCodec.h"
#include "utils/CommonUtil.h"

#include <limits>
#include <map>
#include <memory>
#include <utility>

#include <experimental/filesystem>

namespace milvus {
namespace engine {

const char* MAX_OP_ID_FILE_NAME = "max_op";

WalManager::WalManager() : cleanup_thread_pool_(1, 1) = default;

WalManager&
WalManager::GetInstance() {
    static WalManager s_mgr;
    return s_mgr;
}

Status
WalManager::Start(const DBOptions& options) {
    enable_ = options.wal_enable_;
    wal_path_ = options.meta_.path_;
    insert_buffer_size_ = options.insert_buffer_size_;

    CommonUtil::CreateDirectory(wal_path_);

    auto status = ReadMaxOpId();
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

Status
WalManager::Stop() {
    std::lock_guard<std::mutex> lck(cleanup_thread_mutex_);
    for (auto& iter : cleanup_thread_results_) {
        iter.wait();
    }

    return Status::OK();
}

Status
WalManager::RecordOperation(const WalOperationPtr& operation, const DBPtr& db) {
    if (operation == nullptr) {
        return Status(DB_ERROR, "Wal operation is null pointer");
    }

    Status status;
    switch (operation->Type()) {
        case WalOperationType::INSERT_ENTITY: {
            InsertEntityOperationPtr op = std::static_pointer_cast<InsertEntityOperation>(operation);
            status = RecordInsertOperation(op, db);
            break;
        }
        case WalOperationType::DELETE_ENTITY: {
            DeleteEntityOperationPtr op = std::static_pointer_cast<DeleteEntityOperation>(operation);
            status = RecordDeleteOperation(op, db);
            break;
        }
        default:
            break;
    }

    if (!status.ok()) {
        LOG_ENGINE_DEBUG_ << "Failed to record wal opertiaon: " << status.message();
    }

    return status;
}

Status
WalManager::OperationDone(const std::string& collection_name, idx_t op_id) {
    if (!enable_) {
        return Status::OK();
    }

    bool start_clecnup = false;
    {
        // record max operation id for each collection
        std::lock_guard<std::mutex> lock(max_op_mutex_);
        idx_t last_id = max_op_id_map_[collection_name];
        if (op_id > last_id) {
            max_op_id_map_[collection_name] = op_id;
            start_clecnup = true;

            // write max op id to disk
            std::string path = ConstructFilePath(collection_name, MAX_OP_ID_FILE_NAME);
            WalFile file;
            file.OpenFile(path, WalFile::OVER_WRITE);
            file.Write<idx_t>(&op_id);
        }
    }

    if (start_clecnup) {
        StartCleanupThread(collection_name);
    }

    return Status::OK();
}

Status
WalManager::Recovery(const DBPtr& db) {
    using DirectoryIterator = std::experimental::filesystem::recursive_directory_iterator;
    DirectoryIterator iter_outer(wal_path_);
    DirectoryIterator end_outer;
    for (; iter_outer != end_outer; ++iter_outer) {
        auto path_outer = (*iter_outer).path();
        if (!std::experimental::filesystem::is_directory(path_outer)) {
            continue;
        }

        std::string collection_name = path_outer.filename().c_str();

        // iterate files
        std::map<idx_t, std::experimental::filesystem::path> id_files;
        DirectoryIterator iter_inner(path_outer);
        DirectoryIterator end_inner;
        for (; iter_inner != end_inner; ++iter_inner) {
            auto path_inner = (*iter_inner).path();
            std::string file_name = path_inner.filename().c_str();
            if (file_name == MAX_OP_ID_FILE_NAME) {
                continue;
            }
            idx_t op_id = std::stol(file_name);
            id_files.insert(std::make_pair(op_id, path_inner));
        }

        // the max operation id
        idx_t max_op_id = std::numeric_limits<idx_t>::max();
        {
            std::lock_guard<std::mutex> lock(max_op_mutex_);
            if (max_op_id_map_.find(collection_name) != max_op_id_map_.end()) {
                max_op_id = max_op_id_map_[collection_name];
            }
        }

        // id_files arrange id in assendent, we know which file should be read
        for (auto& pair : id_files) {
            WalFilePtr file = std::make_shared<WalFile>();
            file->OpenFile(pair.second.c_str(), WalFile::READ);
            idx_t last_id = 0;
            file->ReadLastOpId(last_id);
            if (last_id <= max_op_id) {
                file->CloseFile();
                std::experimental::filesystem::remove(pair.second);
                continue;  // skip and delete this file since all its operations already done
            }

            Status status = Status::OK();
            while (status.ok()) {
                WalOperationPtr operation;
                operation->collection_name_ = collection_name;
                status = WalOperationCodec::IterateOperation(file, operation, max_op_id);
                PerformOperation(operation, db);
            }
        }
    }

    return Status::OK();
}

Status
WalManager::ReadMaxOpId() {
    using DirectoryIterator = std::experimental::filesystem::recursive_directory_iterator;
    DirectoryIterator iter(wal_path_);
    DirectoryIterator end;
    for (; iter != end; ++iter) {
        auto path = (*iter).path();
        if (std::experimental::filesystem::is_directory(path)) {
            std::string collection_name = path.filename().c_str();
            path.append(MAX_OP_ID_FILE_NAME);
            if (!std::experimental::filesystem::is_regular_file(path)) {
                continue;  // ignore?
            }

            WalFile file;
            file.OpenFile(path.c_str(), WalFile::READ);
            idx_t max_op = 0;
            file.Read(&max_op);

            std::lock_guard<std::mutex> lock(max_op_mutex_);
            max_op_id_map_.insert(std::make_pair(collection_name, max_op));
        }
    }

    return Status::OK();
}

Status
WalManager::RecordInsertOperation(const InsertEntityOperationPtr& operation, const DBPtr& db) {
    std::vector<DataChunkPtr> chunks;
    SplitChunk(operation->data_chunk_, chunks);

    IDNumbers op_ids;
    auto status = id_gen_.GetNextIDNumbers(chunks.size(), op_ids);
    if (!status.ok()) {
        return status;
    }

    for (size_t i = 0; i < chunks.size(); ++i) {
        idx_t op_id = op_ids[i];
        DataChunkPtr& chunk = chunks[i];
        int64_t chunk_size = utils::GetSizeOfChunk(chunk);

        {
            // open wal file
            std::string path = ConstructFilePath(operation->collection_name_, std::to_string(op_id));
            std::lock_guard<std::mutex> lock(file_map_mutex_);
            WalFilePtr file = file_map_[operation->collection_name_];
            if (file == nullptr) {
                file = std::make_shared<WalFile>();
                file_map_[operation->collection_name_] = file;
                file->OpenFile(path, WalFile::APPEND_WRITE);
            } else if (!file->IsOpened() || file->ExceedMaxSize(chunk_size)) {
                file->OpenFile(path, WalFile::APPEND_WRITE);
            }

            // write to wal file
            status = WalOperationCodec::WriteInsertOperation(file, operation->partition_name, chunk, op_id);
            if (!status.ok()) {
                return status;
            }
        }

        // insert action to db
        status = db->Insert(operation->collection_name_, operation->partition_name, operation->data_chunk_, op_id);
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status
WalManager::SplitChunk(const DataChunkPtr& chunk, std::vector<DataChunkPtr>& chunks) {
    //    int64_t chunk_size = utils::GetSizeOfChunk(chunk);
    //    if (chunk_size > insert_buffer_size_) {
    //        int64_t batch = chunk_size / insert_buffer_size_;
    //        int64_t batch_count = chunk->count_ / batch;
    //        for (int64_t i = 0; i <= batch; ++i) {
    //        }
    //    } else {
    //        chunks.push_back(chunk);
    //    }
    chunks.push_back(chunk);

    return Status::OK();
}

Status
WalManager::RecordDeleteOperation(const DeleteEntityOperationPtr& operation, const DBPtr& db) {
    idx_t op_id = id_gen_.GetNextIDNumber();
    int64_t append_size = operation->entity_ids_.size() * sizeof(idx_t);

    {
        // open wal file
        std::string path = ConstructFilePath(operation->collection_name_, std::to_string(op_id));
        std::lock_guard<std::mutex> lock(file_map_mutex_);
        WalFilePtr file = file_map_[operation->collection_name_];
        if (file == nullptr) {
            file = std::make_shared<WalFile>();
            file_map_[operation->collection_name_] = file;
            file->OpenFile(path, WalFile::APPEND_WRITE);
        } else if (!file->IsOpened() || file->ExceedMaxSize(append_size)) {
            file->OpenFile(path, WalFile::APPEND_WRITE);
        }

        // write to wal file
        auto status = WalOperationCodec::WriteDeleteOperation(file, operation->entity_ids_, op_id);
        if (!status.ok()) {
            return status;
        }
    }

    // delete action to db
    return db->DeleteEntityByID(operation->collection_name_, operation->entity_ids_, op_id);
}

std::string
WalManager::ConstructFilePath(const std::string& collection_name, const std::string& file_name) {
    std::experimental::filesystem::path full_path(wal_path_);
    std::experimental::filesystem::create_directory(full_path);
    full_path.append(collection_name);
    std::experimental::filesystem::create_directory(full_path);
    full_path.append(file_name);

    std::string path(full_path.c_str());
    return path;
}

void
WalManager::StartCleanupThread(const std::string& collection_name) {
    // the previous thread finished?
    std::lock_guard<std::mutex> lck(cleanup_thread_mutex_);
    if (cleanup_thread_results_.empty()) {
        // start a new cleanup thread
        cleanup_thread_results_.push_back(
            cleanup_thread_pool_.enqueue(&WalManager::CleanupThread, this, collection_name));
    } else {
        std::chrono::milliseconds span(1);
        if (cleanup_thread_results_.back().wait_for(span) == std::future_status::ready) {
            cleanup_thread_results_.pop_back();

            // start a new cleanup thread
            cleanup_thread_results_.push_back(
                cleanup_thread_pool_.enqueue(&WalManager::CleanupThread, this, collection_name));
        }
    }
}

void
WalManager::CleanupThread(std::string collection_name) {
    SetThreadName("wal_clean");

    using DirectoryIterator = std::experimental::filesystem::recursive_directory_iterator;
    DirectoryIterator iter_outer(wal_path_);
    DirectoryIterator end_outer;
    for (; iter_outer != end_outer; ++iter_outer) {
        auto path_outer = (*iter_outer).path();
        if (!std::experimental::filesystem::is_directory(path_outer)) {
            continue;
        }

        // get max operation id
        std::string file_name = path_outer.filename().c_str();
        if (file_name != collection_name) {
            continue;
        }

        idx_t max_op = std::numeric_limits<idx_t>::max();
        {
            std::lock_guard<std::mutex> lock(max_op_mutex_);
            if (max_op_id_map_.find(collection_name) != max_op_id_map_.end()) {
                max_op = max_op_id_map_[collection_name];
            }
        }

        // iterate files
        std::map<idx_t, std::experimental::filesystem::path> id_files;
        DirectoryIterator iter_inner(path_outer);
        DirectoryIterator end_inner;
        for (; iter_inner != end_inner; ++iter_inner) {
            auto path_inner = (*iter_inner).path();
            std::string file_name = path_inner.filename().c_str();
            if (file_name == MAX_OP_ID_FILE_NAME) {
                continue;
            }
            idx_t op_id = std::stol(file_name);
            id_files.insert(std::make_pair(op_id, path_inner));
        }

        if (id_files.empty()) {
            continue;
        }

        // remove unused files
        // the std::map arrange id in assendent, direct delete files except the last one
        idx_t max_id = id_files.rbegin()->first;
        std::experimental::filesystem::path max_file = id_files.rbegin()->second;
        id_files.erase(max_id);
        for (auto& pair : id_files) {
            std::experimental::filesystem::remove(pair.second);
        }

        // the last wal file need to be deleted?
        WalFile file;
        file.OpenFile(max_file.c_str(), WalFile::READ);
        idx_t last_id = 0;
        file.ReadLastOpId(last_id);
        if (last_id <= max_op) {
            file.CloseFile();
            std::experimental::filesystem::remove(max_file);
        }
    }
}

Status
WalManager::PerformOperation(const WalOperationPtr& operation, const DBPtr& db) {
    Status status;
    switch (operation->Type()) {
        case WalOperationType::INSERT_ENTITY: {
            InsertEntityOperationPtr op = std::static_pointer_cast<InsertEntityOperation>(operation);
            status = db->Insert(op->collection_name_, op->partition_name, op->data_chunk_, op->ID());
            break;
        }
        case WalOperationType::DELETE_ENTITY: {
            DeleteEntityOperationPtr op = std::static_pointer_cast<DeleteEntityOperation>(operation);
            status = db->DeleteEntityByID(op->collection_name_, op->entity_ids_, op->ID());
            break;
        }
        default:
            return Status(DB_ERROR, "Unsupportted wal operation");
    }

    return status;
}

}  // namespace engine
}  // namespace milvus
