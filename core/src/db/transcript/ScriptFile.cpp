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

#include "db/transcript/ScriptFile.h"
#include "db/Types.h"

#include <limits>

namespace milvus {
namespace engine {

ScriptFile::~ScriptFile() {
    CloseFile();
}

Status
ScriptFile::OpenFile(const std::string& path, OpenMode mode) {
    CloseFile();

    try {
        std::string str_mode;
        switch (mode) {
            case OpenMode::READ:
                str_mode = "r";
                break;
            case OpenMode::APPEND_WRITE:
                str_mode = "aw";
                break;
            default:
                return Status(DB_ERROR, "Unsupported file mode");
        }
        file_ = fopen(path.c_str(), str_mode.c_str());
        if (file_ == nullptr) {
            std::string msg = "Failed to create wal file: " + path;
            return Status(DB_ERROR, msg);
        }
        mode_ = mode;
    } catch (std::exception& ex) {
        std::string msg = "Failed to create wal file, reason: " + std::string(ex.what());
        return Status(DB_ERROR, msg);
    }

    return Status::OK();
}

Status
ScriptFile::CloseFile() {
    if (file_ != nullptr) {
        fclose(file_);
        file_ = nullptr;
        file_size_ = 0;
    }

    return Status::OK();
}

bool
ScriptFile::ExceedMaxSize(int64_t append_size) {
    return (file_size_ + append_size) > MAX_SCRIPT_FILE_SIZE;
}

}  // namespace engine
}  // namespace milvus
