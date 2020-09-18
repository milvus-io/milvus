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

#include "HDFSOperation.h"
#include "HDFSClient.h"

namespace milvus {
namespace storage {

HDFSOperation::HDFSOperation(const std::string& dir_path_) : dir_path_(dir_path_) {
    HDFSClient::getInstance();
}

void
HDFSOperation::CreateDirectory() {
    HDFSClient::getInstance().CreateDirectory(const_cast<char*>(dir_path_.c_str()));
}

const std::string&
HDFSOperation::GetDirectory() {
    return dir_path_;
}

void
HDFSOperation::ListDirectory(std::vector<std::string>& file_paths) {
    HDFSClient::getInstance().ListDirectory(file_paths, dir_path_);
}

bool
HDFSOperation::DeleteFile(const std::string& file_path) {
    return HDFSClient::getInstance().DeleteFile(file_path);
}

}  // namespace storage
}  // namespace milvus