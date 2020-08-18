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

bool
DiskIOReader::Open(const std::string& name) {
    name_ = name;
    fs_ = std::fstream(name_, std::ios::in | std::ios::binary);
    return fs_.is_open();
}

void
DiskIOReader::Read(void* ptr, int64_t size) {
    fs_.read(reinterpret_cast<char*>(ptr), size);
}

void
DiskIOReader::Seekg(int64_t pos) {
    fs_.seekg(pos);
}

int64_t
DiskIOReader::Length() {
    /* save current position */
    int64_t cur = fs_.tellg();

    /* move position to end of file */
    fs_.seekg(0, fs_.end);
    int64_t len = fs_.tellg();

    /* restore position */
    fs_.seekg(cur, fs_.beg);
    return len;
}

void
DiskIOReader::Close() {
    fs_.close();
}

}  // namespace storage
}  // namespace milvus
