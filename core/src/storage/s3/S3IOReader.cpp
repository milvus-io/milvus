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

#include "storage/s3/S3IOReader.h"
#include "storage/s3/S3ClientWrapper.h"

namespace milvus {
namespace storage {

bool
S3IOReader::open(const std::string& name) {
    name_ = name;
    pos_ = 0;
    return (S3ClientWrapper::GetInstance().GetObjectStr(name_, buffer_).ok());
}

void
S3IOReader::read(void* ptr, int64_t size) {
    memcpy(ptr, buffer_.data() + pos_, size);
}

void
S3IOReader::seekg(int64_t pos) {
    pos_ = pos;
}

int64_t
S3IOReader::length() {
    return buffer_.length();
}

void
S3IOReader::close() {
}

}  // namespace storage
}  // namespace milvus
