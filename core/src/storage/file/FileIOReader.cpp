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

#include "storage/file/FileIOReader.h"

namespace milvus {
namespace storage {

FileIOReader::FileIOReader(const std::string& name) : IOReader(name) {
    fs_ = std::fstream(name_, std::ios::in | std::ios::binary);
}

FileIOReader::~FileIOReader() {
    fs_.close();
}

void
FileIOReader::read(void* ptr, size_t size) {
    fs_.read(reinterpret_cast<char*>(ptr), size);
}

void
FileIOReader::seekg(size_t pos) {
    fs_.seekg(pos);
}

size_t
FileIOReader::length() {
    fs_.seekg(0, fs_.end);
    return fs_.tellg();
}
}  // namespace storage
}  // namespace milvus
