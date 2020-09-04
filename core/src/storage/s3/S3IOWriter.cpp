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

#include "storage/s3/S3IOWriter.h"
#include "storage/s3/S3ClientWrapper.h"

namespace milvus {
namespace storage {

bool
S3IOWriter::Open(const std::string& name) {
    name_ = name;
    len_ = 0;
    buffer_ = "";
    return true;
}

void
S3IOWriter::Write(const void* ptr, int64_t size) {
    buffer_ += std::string(reinterpret_cast<const char*>(ptr), size);
    len_ += size;
}

int64_t
S3IOWriter::Length() {
    return len_;
}

void
S3IOWriter::Close() {
    S3ClientWrapper::GetInstance().PutObjectStr(name_, buffer_);
}

}  // namespace storage
}  // namespace milvus
