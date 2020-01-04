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

#include "storage/s3/S3IOWriter.h"
#include "storage/s3/S3ClientWrapper.h"

namespace milvus {
namespace storage {

S3IOWriter::S3IOWriter(const std::string& name) : IOWriter(name) {
    buffer_ = "";
}

S3IOWriter::~S3IOWriter() {
    S3ClientWrapper::GetInstance().PutObjectStr(name_, buffer_);
}

void
S3IOWriter::write(void* ptr, size_t size) {
    buffer_ += std::string(reinterpret_cast<char*>(ptr), size);
    len_ += size;
}

size_t
S3IOWriter::length() {
    return len_;
}

}  // namespace storage
}  // namespace milvus
