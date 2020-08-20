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

#pragma once

#include "db/Types.h"
#include "utils/Status.h"

#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace milvus {
namespace engine {

class WalFile {
 public:
    WalFile() = default;
    virtual ~WalFile();

    bool
    IsOpened() const {
        return file_ != nullptr;
    }

    enum OpenMode {
        NA = 0,
        READ = 1,
        OVER_WRITE = 2,
        APPEND_WRITE = 3,
    };
    Status
    OpenFile(const std::string& path, OpenMode mode);

    Status
    CloseFile();

    bool
    ExceedMaxSize(int64_t append_size);

    template <typename T>
    inline int64_t
    Write(T* value) {
        if (file_ == nullptr) {
            return 0;
        }

        int64_t bytes = fwrite(value, 1, sizeof(T), file_);
        file_size_ += bytes;
        return bytes;
    }

    inline int64_t
    Write(const void* data, int64_t length) {
        if (file_ == nullptr) {
            return 0;
        }

        int64_t bytes = fwrite(data, 1, length, file_);
        file_size_ += bytes;
        return bytes;
    }

    template <typename T>
    inline int64_t
    Read(T* value) {
        if (file_ == nullptr) {
            return 0;
        }

        int64_t bytes = fread(value, 1, sizeof(T), file_);
        return bytes;
    }

    inline int64_t
    Read(void* data, int64_t length) {
        if (file_ == nullptr) {
            return 0;
        }

        int64_t bytes = fread(data, 1, length, file_);
        return bytes;
    }

    inline int64_t
    ReadStr(std::string& str, int64_t length) {
        if (file_ == nullptr || length <= 0) {
            return 0;
        }

        char* buf = new char[length + 1];
        int64_t bytes = fread(buf, 1, length, file_);
        buf[length] = '\0';
        str = buf;
        return bytes;
    }

    inline void
    Flush() {
        if (file_ && mode_ != OpenMode::READ) {
            fflush(file_);
        }
    }

    int64_t
    Size() const {
        return file_size_;
    }
    std::string
    Path() const {
        return file_path_;
    }

    Status
    ReadLastOpId(idx_t& op_id);

    void inline SeekForward(int64_t offset) {
        if (file_ == nullptr || mode_ != OpenMode::READ) {
            return;
        }

        fseek(file_, offset, SEEK_CUR);
    }

 private:
    FILE* file_ = nullptr;
    OpenMode mode_ = OpenMode::NA;
    int64_t file_size_ = 0;
    std::string file_path_;
};

using WalFilePtr = std::shared_ptr<WalFile>;

}  // namespace engine
}  // namespace milvus
