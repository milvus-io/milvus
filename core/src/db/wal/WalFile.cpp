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

#include "db/wal/WalFile.h"
#include "db/Constants.h"
#include "db/Types.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"

#include <experimental/filesystem>
#include <limits>

namespace milvus {
namespace engine {

WalFile::~WalFile() {
    CloseFile();
}

Status
WalFile::OpenFile(const std::string& path, OpenMode mode) {
    CloseFile();

    try {
        std::string str_mode;
        switch (mode) {
            case OpenMode::READ:
                str_mode = "rb";
                break;
            case OpenMode::APPEND_WRITE:
                str_mode = "awb";
                break;
            case OpenMode::OVER_WRITE:
                str_mode = "wb";
                break;
            default:
                return Status(DB_ERROR, "Unsupported file mode");
        }

        // makesure the parent path is created
        std::experimental::filesystem::path temp_path(path);
        auto parent_path = temp_path.parent_path();
        CommonUtil::CreateDirectory(parent_path.c_str());

        file_ = fopen(path.c_str(), str_mode.c_str());
        if (file_ == nullptr) {
            std::string msg = "Failed to create wal file: " + path;
            LOG_ENGINE_ERROR_ << msg;
            return Status(DB_ERROR, msg);
        }
        file_path_ = path;
        mode_ = mode;
    } catch (std::exception& ex) {
        std::string msg = "Failed to create wal file, reason: " + std::string(ex.what());
        LOG_ENGINE_ERROR_ << msg;
        return Status(DB_ERROR, msg);
    }

    return Status::OK();
}

Status
WalFile::CloseFile() {
    if (file_ != nullptr) {
        fclose(file_);
        file_ = nullptr;
        file_size_ = 0;
        file_path_ = "";
    }

    return Status::OK();
}

bool
WalFile::ExceedMaxSize(int64_t append_size) {
    return (file_size_ + append_size) > MAX_WAL_FILE_SIZE;
}

Status
WalFile::ReadLastOpId(idx_t& op_id) {
    op_id = std::numeric_limits<idx_t>::max();
    if (file_ == nullptr || mode_ != OpenMode::READ) {
        return Status(DB_ERROR, "File not opened or not read mode");
    }

    // current position
    auto cur_poz = ftell(file_);

    // get total lenth
    fseek(file_, 0, SEEK_END);
    auto end_poz = ftell(file_);

    // read last id
    idx_t last_id = 0;
    int64_t offset = end_poz - sizeof(last_id);
    fseek(file_, offset, SEEK_SET);

    int64_t bytes = fread(&last_id, 1, sizeof(last_id), file_);
    if (bytes == sizeof(op_id)) {
        op_id = last_id;
    }

    // back to current postiion
    fseek(file_, cur_poz, SEEK_SET);
    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
