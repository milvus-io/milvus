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

#include "db/wal/WalFileHandler.h"

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
    p_file_ = fopen((file_path_ + file_name_).c_str(), file_mode_.c_str());
    return p_file_ != nullptr;
}

bool
MXLogFileHandler::Load(char* buf, uint32_t data_offset, uint32_t data_size) {
    if (!IsOpen()) {
        if (!OpenFile())
            return false;
    }

    fseek(p_file_, data_offset, SEEK_SET);

    if (data_size != 0) {
        auto res = fread(buf, 1, data_size, p_file_);
        return (res == data_size);
    }

    return true;
}

bool
MXLogFileHandler::Write(char* buf, uint32_t data_size, bool is_sync) {
    if (!IsOpen()) {
        if (!OpenFile())
            return false;
    }
    auto res = fwrite(buf, 1, data_size, p_file_);
    return (res == data_size);
}

bool
MXLogFileHandler::ReBorn(const std::string& file_name) {
    CloseFile();
    SetFileName(file_name);
    return OpenFile();
}

bool
MXLogFileHandler::CloseFile() {
    bool rst = true;
    if (p_file_ != nullptr) {
        rst = (fclose(p_file_) == 0);
        if (rst) {
            p_file_ = nullptr;
        }
    }
    return rst;
}

std::string
MXLogFileHandler::GetFilePath() {
    return file_path_;
}

std::string
MXLogFileHandler::GetFileName() {
    return file_name_;
}

bool
MXLogFileHandler::IsOpen() {
    return p_file_ != NULL;
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
