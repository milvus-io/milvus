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
#include "utils/Log.h"

#include <map>
#include <memory>
#include <utility>

#include <experimental/filesystem>

namespace milvus {
namespace engine {

const char* WAL_MAX_OP_FILE_NAME = "max_op";
const char* WAL_DEL_FILE_NAME = "del";

namespace {

bool
StrToID(const std::string& str, idx_t& id) {
    try {
        id = std::stol(str);
        return true;
    } catch (std::exception& ex) {
        return false;
    }
}

void
FindWalFiles(const std::experimental::filesystem::path& folder,
             std::map<idx_t, std::experimental::filesystem::path>& files) {
    using DirectoryIterator = std::experimental::filesystem::recursive_directory_iterator;
    DirectoryIterator iter(folder);
    DirectoryIterator end;
    for (; iter != end; ++iter) {
        auto path_inner = (*iter).path();
        std::string file_name = path_inner.filename().c_str();
        if (file_name == WAL_MAX_OP_FILE_NAME || file_name == WAL_DEL_FILE_NAME) {
            continue;
        }
        idx_t op_id = 0;
        if (StrToID(file_name, op_id)) {
            files.insert(std::make_pair(op_id, path_inner));
        }
    }
}

}  // namespace

WalManager::WalManager() : cleanup_thread_pool_(1, 1) {
}

WalManager&
WalManager::GetInstance() {
    static WalManager s_mgr;
    return s_mgr;
}

Status
WalManager::Start(const DBOptions& options) {
    enable_ = options.wal_enable_;
    insert_buffer_size_ = options.insert_buffer_size_;

    std::experimental::filesystem::path wal_path(options.wal_path_);
    wal_path_ = wal_path.c_str();
    CommonUtil::CreateDirectory(wal_path_);

    auto status = Init();
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

Status
WalManager::Stop() {
    {
        std::lock_guard<std::mutex> lock(file_map_mutex_);
        file_map_.clear();
    }

    WaitCleanupFinish();

    return Status::OK();
}

Status
WalManager::DropCollection(const std::string& collection_name) {
    // write a placeholder file 'del' under collection folder, let cleanup thread remove this folder
    std::string path = ConstructFilePath(collection_name, WAL_DEL_FILE_NAME);
    if (!path.empty()) {
        WalFile file;
        file.OpenFile(path, WalFile::OVER_WRITE);
        idx_t op_id = id_gen_.GetNextIDNumber();
        file.Write<idx_t>(&op_id);

        AddCleanupTask(collection_name);
        StartCleanupThread();
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
            std::string path = ConstructFilePath(collection_name, WAL_MAX_OP_FILE_NAME);
            if (!path.empty()) {
                WalFile file;
                file.OpenFile(path, WalFile::OVER_WRITE);
                file.Write<idx_t>(&op_id);
            }
        }
    }

    if (start_clecnup) {
        AddCleanupTask(collection_name);
        StartCleanupThread();
    }

    return Status::OK();
}

Status
WalManager::Recovery(const DBPtr& db, const CollectionMaxOpIDMap& max_op_ids) {
    WaitCleanupFinish();

    if (db == nullptr) {
        return Status(DB_ERROR, "null pointer");
    }

    LOG_ENGINE_DEBUG_ << "Begin wal recovery";

    try {
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
            FindWalFiles(path_outer, id_files);

            // the max operation id
            idx_t max_op_id = 0;
            {
                std::lock_guard<std::mutex> lock(max_op_mutex_);
                if (max_op_id_map_.find(collection_name) != max_op_id_map_.end()) {
                    max_op_id = max_op_id_map_[collection_name];
                }
            }

            auto iter = max_op_ids.find(collection_name);
            if (iter != max_op_ids.end()) {
                idx_t outer_max_id = iter->second;
                max_op_id = outer_max_id > max_op_id ? outer_max_id : max_op_id;
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

                // read operation and execute
                Status status = Status::OK();
                while (status.ok()) {
                    WalOperationPtr operation;
                    status = WalOperationCodec::IterateOperation(file, operation, max_op_id);
                    if (operation) {
                        operation->collection_name_ = collection_name;
                        PerformOperation(operation, db);
                    }
                }
            }
        }

        // flush to makesure data is serialized
        return db->Flush();
    } catch (std::exception& ex) {
        std::string msg = "Failed to recovery wal, reason: " + std::string(ex.what());
        return Status(DB_ERROR, msg);
    }

    LOG_ENGINE_DEBUG_ << "Wal recovery finished";

    return Status::OK();
}

Status
WalManager::Init() {
    try {
        using DirectoryIterator = std::experimental::filesystem::recursive_directory_iterator;
        DirectoryIterator iter(wal_path_);
        DirectoryIterator end;
        for (; iter != end; ++iter) {
            auto path = (*iter).path();
            if (std::experimental::filesystem::is_directory(path)) {
                std::string collection_name = path.filename().c_str();

                // read max op id
                std::experimental::filesystem::path file_path = path;
                file_path.append(WAL_MAX_OP_FILE_NAME);
                if (std::experimental::filesystem::is_regular_file(file_path)) {
                    WalFile file;
                    file.OpenFile(file_path.c_str(), WalFile::READ);
                    idx_t max_op = 0;
                    file.Read(&max_op);

                    std::lock_guard<std::mutex> lock(max_op_mutex_);
                    max_op_id_map_.insert(std::make_pair(collection_name, max_op));
                }

                // this collection has been deleted?
                file_path = path;
                file_path.append(WAL_DEL_FILE_NAME);
                if (std::experimental::filesystem::is_regular_file(file_path)) {
                    AddCleanupTask(collection_name);
                }
            }
        }
    } catch (std::exception& ex) {
        std::string msg = "Failed to initial wal, reason: " + std::string(ex.what());
        return Status(DB_ERROR, msg);
    }

    StartCleanupThread();  // do cleanup
    return Status::OK();
}

Status
WalManager::RecordInsertOperation(const InsertEntityOperationPtr& operation, const DBPtr& db) {
    idx_t op_id = id_gen_.GetNextIDNumber();

    DataChunkPtr& chunk = operation->data_chunk_;
    int64_t chunk_size = utils::GetSizeOfChunk(chunk);

    try {
        // open wal file
        std::string path = ConstructFilePath(operation->collection_name_, std::to_string(op_id));
        if (!path.empty()) {
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
            auto status = WalOperationCodec::WriteInsertOperation(file, operation->partition_name, chunk, op_id);
            if (!status.ok()) {
                return status;
            }
        }
    } catch (std::exception& ex) {
        std::string msg = "Failed to record insert operation, reason: " + std::string(ex.what());
        return Status(DB_ERROR, msg);
    }

    // insert action to db
    if (db) {
        auto status = db->Insert(operation->collection_name_, operation->partition_name, operation->data_chunk_, op_id);
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status
WalManager::RecordDeleteOperation(const DeleteEntityOperationPtr& operation, const DBPtr& db) {
    idx_t op_id = id_gen_.GetNextIDNumber();
    int64_t append_size = operation->entity_ids_.size() * sizeof(idx_t);

    // open wal file
    try {
        std::string path = ConstructFilePath(operation->collection_name_, std::to_string(op_id));
        if (!path.empty()) {
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
    } catch (std::exception& ex) {
        std::string msg = "Failed to record delete operation, reason: " + std::string(ex.what());
        return Status(DB_ERROR, msg);
    }

    // delete action to db
    if (db) {
        return db->DeleteEntityByID(operation->collection_name_, operation->entity_ids_, op_id);
    }

    return Status::OK();
}

std::string
WalManager::ConstructFilePath(const std::string& collection_name, const std::string& file_name) {
    // typically, the wal file path is like: /xxx/milvus/wal/[collection_name]/xxxxxxxxxx
    std::experimental::filesystem::path full_path(wal_path_);
    full_path.append(collection_name);
    full_path.append(file_name);

    std::string path(full_path.c_str());
    return path;
}

void
WalManager::AddCleanupTask(const std::string& collection_name) {
    std::lock_guard<std::mutex> lck(cleanup_task_mutex_);
    if (cleanup_tasks_.empty()) {
        cleanup_tasks_.push_back(collection_name);
    } else {
        // no need to add duplicate name
        std::string back = cleanup_tasks_.back();
        if (back != collection_name) {
            cleanup_tasks_.push_back(collection_name);
        }
    }
}

void
WalManager::TakeCleanupTask(std::string& collection_name) {
    collection_name = "";
    std::lock_guard<std::mutex> lck(cleanup_task_mutex_);
    if (cleanup_tasks_.empty()) {
        return;
    }
    collection_name = cleanup_tasks_.front();
    cleanup_tasks_.pop_front();
}

void
WalManager::StartCleanupThread() {
    // the previous thread finished?
    std::lock_guard<std::mutex> lck(cleanup_thread_mutex_);
    if (cleanup_thread_results_.empty()) {
        // start a new cleanup thread
        cleanup_thread_results_.push_back(cleanup_thread_pool_.enqueue(&WalManager::CleanupThread, this));
    } else {
        std::chrono::milliseconds span(1);
        if (cleanup_thread_results_.back().wait_for(span) == std::future_status::ready) {
            cleanup_thread_results_.pop_back();

            // start a new cleanup thread
            cleanup_thread_results_.push_back(cleanup_thread_pool_.enqueue(&WalManager::CleanupThread, this));
        }
    }
}

void
WalManager::WaitCleanupFinish() {
    std::lock_guard<std::mutex> lck(cleanup_thread_mutex_);
    for (auto& iter : cleanup_thread_results_) {
        iter.wait();
    }
}

void
WalManager::CleanupThread() {
    SetThreadName("wal_clean");

    std::string target_collection;
    TakeCleanupTask(target_collection);

    using DirectoryIterator = std::experimental::filesystem::recursive_directory_iterator;
    while (!target_collection.empty()) {
        std::string path = ConstructFilePath(target_collection, "");
        std::experimental::filesystem::path collection_path = path;

        // not a folder
        if (!std::experimental::filesystem::is_directory(path)) {
            TakeCleanupTask(target_collection);
            continue;
        }

        // collection already deleted
        std::experimental::filesystem::path file_path = collection_path;
        file_path.append(WAL_DEL_FILE_NAME);
        if (std::experimental::filesystem::is_regular_file(file_path)) {
            // clean max operation id
            {
                std::lock_guard<std::mutex> lock(max_op_mutex_);
                max_op_id_map_.erase(target_collection);
            }
            // clean opened file in buffer
            {
                std::lock_guard<std::mutex> lock(file_map_mutex_);
                file_map_.erase(target_collection);

                // remove collection folder
                // do this under the lock to avoid multi-thread conflict
                std::experimental::filesystem::remove_all(collection_path);
            }

            TakeCleanupTask(target_collection);
            continue;
        }

        // get max operation id
        idx_t max_op = 0;
        {
            std::lock_guard<std::mutex> lock(max_op_mutex_);
            if (max_op_id_map_.find(target_collection) != max_op_id_map_.end()) {
                max_op = max_op_id_map_[target_collection];
            }
        }

        // iterate files
        std::map<idx_t, std::experimental::filesystem::path> wal_files;
        FindWalFiles(collection_path, wal_files);

        // no wal file
        if (wal_files.empty()) {
            TakeCleanupTask(target_collection);
            continue;
        }

        // the std::map arrange id in assendent
        // if the last id < max_op, delete the wal file
        for (auto& pair : wal_files) {
            WalFile file;
            file.OpenFile(pair.second.c_str(), WalFile::READ);
            idx_t last_id = 0;
            file.ReadLastOpId(last_id);
            if (last_id <= max_op) {
                file.CloseFile();

                // makesure wal file is closed
                {
                    std::lock_guard<std::mutex> lock(file_map_mutex_);
                    WalFilePtr file = file_map_[target_collection];
                    if (file) {
                        if (file->Path() == pair.second) {
                            file->CloseFile();
                            file_map_.erase(target_collection);
                        }
                    }
                }

                std::experimental::filesystem::remove(pair.second);
            }
        }

        TakeCleanupTask(target_collection);
    }
}

Status
WalManager::PerformOperation(const WalOperationPtr& operation, const DBPtr& db) {
    if (operation == nullptr || db == nullptr) {
        return Status(DB_ERROR, "null pointer");
    }

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
