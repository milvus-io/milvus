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
DiskIOReader::open(const std::string& name) {
    name_ = name;
    fs_ = std::fstream(name_, std::ios::in | std::ios::binary);
    return fs_.good();
}

void
DiskIOReader::read(void* ptr, int64_t size) {
    fs_.read(reinterpret_cast<char*>(ptr), size);
}

void
DiskIOReader::seekg(int64_t pos) {
    fs_.seekg(pos);
}
void
DiskIOReader::seekg(int64_t pos, std::ios_base::seekdir seekdir) {
    fs_.seekg(pos, seekdir);
}

std::string
DiskIOReader::totallyRead() {
    std::string str((std::istreambuf_iterator<char>(fs_)),
                    std::istreambuf_iterator<char>());
    return str;
}

int64_t
DiskIOReader::length() {
    fs_.seekg(0, fs_.end);
    int64_t len = fs_.tellg();
    fs_.seekg(0, fs_.beg);
    return len;
}

void
DiskIOReader::close() {
    fs_.close();
}

}  // namespace storage
}  // namespace milvus
