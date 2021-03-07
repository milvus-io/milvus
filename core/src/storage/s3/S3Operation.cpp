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

#include "storage/s3/S3Operation.h"

#include "storage/s3/S3ClientWrapper.h"

namespace milvus {
namespace storage {

S3Operation::S3Operation(const std::string& dir_path) : dir_path_(dir_path), local_operation_(dir_path) {
    CreateDirectory();
}

void
S3Operation::CreateDirectory() {
    local_operation_.CreateDirectory();
}

const std::string&
S3Operation::GetDirectory() const {
    return dir_path_;
}

void
S3Operation::ListDirectory(std::vector<std::string>& file_paths) {
    S3ClientWrapper::GetInstance().ListObjects(file_paths, dir_path_);
}

bool
S3Operation::DeleteFile(const std::string& file_path) {
    (void)local_operation_.DeleteFile(file_path);
    return S3ClientWrapper::GetInstance().DeleteObject(file_path).ok();
}

bool
S3Operation::CacheGet(const std::string& file_path) {
    return S3ClientWrapper::GetInstance().GetObjectFile(file_path, file_path).ok();
}

bool
S3Operation::CachePut(const std::string& file_path) {
    // TODO: try introducing LRU
    return S3ClientWrapper::GetInstance().PutObjectFile(file_path, file_path).ok();
}

}  // namespace storage
}  // namespace milvus
