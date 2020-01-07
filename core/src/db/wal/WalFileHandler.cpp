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

#include <sys/stat.h>
#include <unistd.h>
#include "WalFileHandler.h"

namespace milvus {
namespace engine {
namespace wal {

MXLogFileHandler::MXLogFileHandler(const std::string& mxlog_path)
: file_path_(mxlog_path)
{
//    OpenFile();
    file_size_ = 0;
}

MXLogFileHandler::~MXLogFileHandler() {
    CloseFile();
}

bool
MXLogFileHandler::OpenFile() {
    p_file_ = fopen((file_path_ + file_name_).c_str(), file_mode_.c_str());
    if (!p_file_) {
        //todo: log error
        return false;
    }
    file_size_ = 0;
    GetFileSize();
    return true;
}

bool
MXLogFileHandler::Load(char *buf,
                       uint64_t data_size) {
    if(!IsOpen()) {
        if (!OpenFile())
            return false;
    }
    if (data_size == -1) {
        data_size = file_size_;
    }
    auto res = fread(buf, 1, data_size, p_file_);
    __glibcxx_assert(res == data_size);
    return true;
}

bool
MXLogFileHandler::Write(char *buf,
                        uint64_t data_size,
                        bool is_sync) {
    if(!IsOpen()) {
        if (!OpenFile())
            return false;
    }
    auto res = fwrite(buf, 1, data_size, p_file_);
    __glibcxx_assert(res == file_size_);
    return true;
}

void
MXLogFileHandler::ReBorn(const uint64_t& new_file_no) {
    CloseFile();
    file_name_ = std::to_string(new_file_no) + ".wal";
    OpenFile();
}

bool
MXLogFileHandler::CloseFile() {
    return fclose(p_file_) == 0;
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

uint64_t
MXLogFileHandler::GetFileSize() {
    if (file_size_)
        return file_size_;
    struct stat statbuf;
    stat(file_name_.c_str(), &statbuf);
    file_size_ = (uint64_t)statbuf.st_size;
    return file_size_;
}

void
MXLogFileHandler::DeleteFile() {
    remove(((file_path_ + file_name_) + ".wal").c_str());
    p_file_ = NULL;
    file_size_ = 0;
    file_name_ = "";
}

bool
MXLogFileHandler::FileExists() {
    return access(((file_path_ + file_name_) + ".wal").c_str(), 0) != -1;
}

void
MXLogFileHandler::SetFileOpenMode(const std::string& open_mode) {
    file_mode_ = open_mode;
}

void
MXLogFileHandler::SetFileName(const std::string& file_name) {
    file_mode_ = file_name;
}

void
MXLogFileHandler::SetFilePath(const std::string& file_path) {
    file_path_ = file_path;
}

} // wal
} // engine
} // milvus
