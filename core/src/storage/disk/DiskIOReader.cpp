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

#include "storage/disk/DiskIOReader.h"

namespace milvus {
namespace storage {

DiskIOReader::DiskIOReader(const std::string& name) : IOReader(name) {
    fs_ = std::fstream(name_, std::ios::in | std::ios::binary);
}

DiskIOReader::~DiskIOReader() {
    fs_.close();
}

void
DiskIOReader::read(void* ptr, size_t size) {
    fs_.read(reinterpret_cast<char*>(ptr), size);
}

void
DiskIOReader::seekg(size_t pos) {
    fs_.seekg(pos);
}

size_t
DiskIOReader::length() {
    fs_.seekg(0, fs_.end);
    return fs_.tellg();
}
}  // namespace storage
}  // namespace milvus
