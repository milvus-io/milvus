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

#include "storage/s3/S3IOReader.h"
#include "storage/s3/S3ClientWrapper.h"

namespace milvus {
namespace storage {

S3IOReader::S3IOReader(const std::string& name) : IOReader(name), pos_(0) {
    S3ClientWrapper::GetInstance().GetObjectStr(name_, buffer_);
}

S3IOReader::~S3IOReader() {
}

void
S3IOReader::read(void* ptr, size_t size) {
    memcpy(ptr, buffer_.data() + pos_, size);
}

void
S3IOReader::seekg(size_t pos) {
    pos_ = pos;
}

size_t
S3IOReader::length() {
    return buffer_.length();
}

}  // namespace storage
}  // namespace milvus
