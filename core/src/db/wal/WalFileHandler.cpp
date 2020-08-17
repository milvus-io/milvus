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

#include "db/wal/WalFileHandler.h"
#include "utils/Log.h"

#include <sys/stat.h>
#include <unistd.h>

namespace milvus {
namespace engine {
namespace wal {

MXLogFileHandler::MXLogFileHandler(const std::string& mxlog_path) : file_path_(mxlog_path), p_file_(nullptr) {
}

MXLogFileHandler::~MXLogFileHandler() {
    CloseFile();
}

bool
MXLogFileHandler::OpenFile() {
    if (p_file_ == nullptr) {
        p_file_ = fopen((file_path_ + file_name_).c_str(), file_mode_.c_str());
    }
    return (p_file_ != nullptr);
}

uint32_t
MXLogFileHandler::Load(char* buf, uint32_t data_offset) {
    uint32_t read_size = 0;
    if (OpenFile()) {
        uint32_t file_size = GetFileSize();
        if (file_size > data_offset) {
            read_size = file_size - data_offset;
            fseek(p_file_, data_offset, SEEK_SET);
            auto ret = fread(buf, 1, read_size, p_file_);
            if (ret != read_size) {
                LOG_WAL_ERROR_ << LogOut("MXLogFileHandler::Load error, expect read %d but read %d", read_size, ret);
            }
            __glibcxx_assert(ret == read_size);
        }
    }
    return read_size;
}

bool
MXLogFileHandler::Load(char* buf, uint32_t data_offset, uint32_t data_size) {
    if (OpenFile() && data_size != 0) {
        auto file_size = GetFileSize();
        if ((file_size < data_offset) || (file_size - data_offset < data_size)) {
            return false;
        }

        fseek(p_file_, data_offset, SEEK_SET);
        size_t ret = fread(buf, 1, data_size, p_file_);
        if (ret != data_size) {
            LOG_WAL_ERROR_ << LogOut("MXLogFileHandler::Load error, expect read %d but read %d", data_size, ret);
        }
    }
    return true;
}

bool
MXLogFileHandler::Write(char* buf, uint32_t data_size, bool is_sync) {
    uint32_t written_size = 0;
    if (OpenFile() && data_size != 0) {
        written_size = fwrite(buf, 1, data_size, p_file_);
        fflush(p_file_);
    }
    return (written_size == data_size);
}

bool
MXLogFileHandler::ReBorn(const std::string& file_name, const std::string& open_mode) {
    CloseFile();
    SetFileName(file_name);
    SetFileOpenMode(open_mode);
    return OpenFile();
}

bool
MXLogFileHandler::CloseFile() {
    if (p_file_ != nullptr) {
        fclose(p_file_);
        p_file_ = nullptr;
    }
    return true;
}

std::string
MXLogFileHandler::GetFilePath() {
    return file_path_;
}

std::string
MXLogFileHandler::GetFileName() {
    return file_name_;
}

uint32_t
MXLogFileHandler::GetFileSize() {
    struct stat statbuf;
    if (0 == stat((file_path_ + file_name_).c_str(), &statbuf)) {
        return (uint32_t)statbuf.st_size;
    }

    return 0;
}

void
MXLogFileHandler::DeleteFile() {
    remove((file_path_ + file_name_).c_str());
    file_name_ = "";
}

bool
MXLogFileHandler::FileExists() {
    return access((file_path_ + file_name_).c_str(), 0) != -1;
}

void
MXLogFileHandler::SetFileOpenMode(const std::string& open_mode) {
    file_mode_ = open_mode;
}

void
MXLogFileHandler::SetFileName(const std::string& file_name) {
    file_name_ = file_name;
}

void
MXLogFileHandler::SetFilePath(const std::string& file_path) {
    file_path_ = file_path;
}

}  // namespace wal
}  // namespace engine
}  // namespace milvus
