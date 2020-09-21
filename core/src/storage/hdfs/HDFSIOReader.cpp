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

#include "storage/hdfs/HDFSIOReader.h"
#include "HDFSClient.h"

namespace milvus {
namespace storage {

bool
HDFSIOReader::Open(const std::string& name) {
    name_ = name;
    return HDFSClient::getInstance().read_open(name.c_str());
}

void
HDFSIOReader::Read(void* ptr, int64_t size) {
    HDFSClient::getInstance().read(reinterpret_cast<char*>(ptr), size);
}

void
HDFSIOReader::Seekg(int64_t pos) {
    HDFSClient::getInstance().seekg(pos);
}

int64_t
HDFSIOReader::Length() {
    return HDFSClient::getInstance().length(name_);
}

void
HDFSIOReader::Close() {
    HDFSClient::getInstance().close();
}

}  // namespace storage

}  // namespace milvus
