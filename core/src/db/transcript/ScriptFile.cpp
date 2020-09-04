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
ScriptFile::OpenWrite(const std::string& path) {
    try {
        if (writer_.is_open()) {
            writer_.close();
        }

        file_size_ = 0;
        writer_.open(path.c_str(), std::ios::out | std::ios::app);
    } catch (std::exception& ex) {
        return Status(DB_ERROR, ex.what());
    }

    return Status::OK();
}

Status
ScriptFile::OpenRead(const std::string& path) {
    try {
        if (reader_.is_open()) {
            reader_.close();
        }
        reader_.open(path.c_str(), std::ios::in);
    } catch (std::exception& ex) {
        return Status(DB_ERROR, ex.what());
    }

    return Status::OK();
}

Status
ScriptFile::CloseFile() {
    try {
        if (reader_.is_open()) {
            reader_.close();
        }
        if (writer_.is_open()) {
            writer_.close();
        }
    } catch (std::exception& ex) {
        return Status(DB_ERROR, ex.what());
    }
    file_size_ = 0;

    return Status::OK();
}

Status
ScriptFile::WriteLine(const std::string& str) {
    try {
        if (writer_.is_open()) {
            writer_.write(str.c_str(), str.size());
            writer_.write("\n", 1);
            writer_.flush();
            file_size_ += str.size() + 1;
        }
    } catch (std::exception& ex) {
        return Status(DB_ERROR, ex.what());
    }

    return Status::OK();
}

bool
ScriptFile::ReadLine(std::string& str) {
    str = "";
    if (reader_.is_open()) {
        while (getline(reader_, str)) {
            if (!str.empty()) {
                return true;
            }
        }
    }

    return false;
}

bool
ScriptFile::ExceedMaxSize(int64_t append_size) {
    return (file_size_ + append_size) > MAX_SCRIPT_FILE_SIZE;
}

}  // namespace engine
}  // namespace milvus
