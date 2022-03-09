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

#include "storage/oss/OSSIOWriter.h"
#include "OSSClientWrapper.h"

namespace milvus {
namespace storage {

bool
OSSIOWriter::open(const std::string& name) {
    name_ = name;
    len_ = 0;
    buffer_ = "";
    return true;
}

void
OSSIOWriter::write(void* ptr, int64_t size) {
    buffer_ += std::string(reinterpret_cast<char*>(ptr), size);
    len_ += size;
}

int64_t
OSSIOWriter::length() {
    return len_;
}

void
OSSIOWriter::close() {
    OSSClientWrapper::GetInstance().PutObjectStr(name_, buffer_);
}

}  // namespace storage
}  // namespace milvus
