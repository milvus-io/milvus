// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/BinlogReader.h"

namespace milvus::storage {

Status
BinlogReader::Read(int64_t nbytes, void* out) {
    auto remain = size_ - tell_;
    if (nbytes > remain) {
        return Status(SERVER_UNEXPECTED_ERROR, "out range of binlog data");
    }
    std::memcpy(out, data_.get() + tell_, nbytes);
    tell_ += nbytes;
    return Status(SERVER_SUCCESS, "");
}

std::pair<Status, std::shared_ptr<uint8_t[]>>
BinlogReader::Read(int64_t nbytes) {
    auto remain = size_ - tell_;
    if (nbytes > remain) {
        return std::make_pair(
            Status(SERVER_UNEXPECTED_ERROR, "out range of binlog data"),
            nullptr);
    }
    auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
    auto res = std::shared_ptr<uint8_t[]>(data_.get() + tell_, deleter);
    tell_ += nbytes;
    return std::make_pair(Status(SERVER_SUCCESS, ""), res);
}

}  // namespace milvus::storage
