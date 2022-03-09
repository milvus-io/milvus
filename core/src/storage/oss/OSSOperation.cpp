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

#include "storage/oss/OSSOperation.h"

#include "OSSClientWrapper.h"

namespace milvus {
namespace storage {

OSSOperation::OSSOperation(const std::string& dir_path) : dir_path_(dir_path), local_operation_(dir_path) {
    CreateDirectory();
}

void
OSSOperation::CreateDirectory() {
    local_operation_.CreateDirectory();
}

const std::string&
OSSOperation::GetDirectory() const {
    return dir_path_;
}

void
OSSOperation::ListDirectory(std::vector<std::string>& file_paths) {
    OSSClientWrapper::GetInstance().ListObjects(file_paths, dir_path_);
}

bool
OSSOperation::DeleteFile(const std::string& file_path) {
    (void)local_operation_.DeleteFile(file_path);
    return OSSClientWrapper::GetInstance().DeleteObject(file_path).ok();
}

bool
OSSOperation::CacheGet(const std::string& file_path) {
    return OSSClientWrapper::GetInstance().GetObjectFile(file_path, file_path).ok();
}

bool
OSSOperation::CachePut(const std::string& file_path) {
    // TODO: try introducing LRU
    return OSSClientWrapper::GetInstance().PutObjectFile(file_path, file_path).ok();
}

}  // namespace storage
}  // namespace milvus
