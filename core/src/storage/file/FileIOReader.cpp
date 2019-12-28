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

FileIOReader::FileIOReader(const std::string& fname) {
    name = fname;
    fs = std::fstream(name, std::ios::in | std::ios::binary);
}

FileIOReader::~FileIOReader() {
    fs.close();
}

size_t
FileIOReader::operator()(void* ptr, size_t size) {
    fs.read(reinterpret_cast<char*>(ptr), size);
}

size_t
FileIOReader::operator()(void* ptr, size_t size, size_t pos) {
    return 0;
}

}  // namespace storage
}  // namespace milvus
