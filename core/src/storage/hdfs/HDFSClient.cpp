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

#include "storage/hdfs/HDFSClient.h"

namespace milvus {
namespace storage {

HDFSClient::HDFSClient() {
    bld_ = hdfsNewBuilder();
    hdfsBuilderSetNameNode(bld_, "default");  // no need to specify
    hdfs_fs_ = hdfsBuilderConnect(bld_);
}

bool
HDFSClient::read_open(const char* name) {
    hdfs_file_ = hdfsOpenFile(hdfs_fs_, name, O_RDONLY, 0, 0, 0);
    if (hdfs_file_ == nullptr) {
        return false;
    }
    return true;
}

bool
HDFSClient::write_open(const char* name) {
    // attention this mode is "append"
    hdfs_file_ = hdfsOpenFile(hdfs_fs_, name, O_WRONLY | O_APPEND, 0, 0, 0);
    if (hdfs_file_ == nullptr) {
        return false;
    }
    return true;
}

void
HDFSClient::write(char* ptr, int64_t size) {
    tSize nums_write_bytes = hdfsWrite(hdfs_fs_, hdfs_file_, reinterpret_cast<char*>(ptr), static_cast<tSize>(size));
}

void
HDFSClient::read(char* ptr, int64_t size) {
    tSize nums_of_bytes = hdfsRead(hdfs_fs_, hdfs_file_, reinterpret_cast<char*>(ptr), static_cast<int32_t>(size));
}

void
HDFSClient::seekg(int64_t pos) {
    int seek_pos = hdfsSeek(hdfs_fs_, hdfs_file_, pos);
}

int64_t
HDFSClient::length(std::string name) {
    hdfsFileInfo* hdfs_file_info = hdfsGetPathInfo(hdfs_fs_, name.c_str());
    return hdfs_file_info->mSize;
}

void
HDFSClient::close() {
    int flag = hdfsCloseFile(hdfs_fs_, hdfs_file_);
}

void
HDFSClient::CreateDirectory(char* path) {
    int flag = hdfsCreateDirectory(hdfs_fs_, path);
}

void
HDFSClient::ListDirectory(std::vector<std::string>& file_paths, std::string dir_path) {
    int numEntries = 0;
    hdfsFileInfo* file_list = nullptr;
    if ((file_list = hdfsListDirectory(hdfs_fs_, dir_path.c_str(), &numEntries)) != nullptr) {
        for (int i = 0; i < numEntries; ++i) {
            std::string temp(dir_path);
            temp.append(file_list[i].mName);
            file_paths.emplace_back(temp);
        }
        hdfsFreeFileInfo(file_list, numEntries);
    }
}

bool
HDFSClient::DeleteFile(const std::string& file_path) {
    int flag = hdfsDelete(hdfs_fs_, file_path.c_str(), 0);
    if (flag == 0)
        return true;
    else
        return false;
}

HDFSClient::~HDFSClient() {
    delete bld_;
    hdfsDisconnect(hdfs_fs_);
}

}  // namespace storage
}  // namespace milvus
