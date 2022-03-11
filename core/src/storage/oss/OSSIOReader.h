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

#pragma once

#include <memory>
#include <string>
#include "storage/IOReader.h"

namespace milvus {
namespace storage {

class OSSIOReader : public IOReader {
 public:
    OSSIOReader() = default;
    ~OSSIOReader() = default;

    // No copy and move
    OSSIOReader(const OSSIOReader&) = delete;
    OSSIOReader(OSSIOReader&&) = delete;

    OSSIOReader&
    operator=(const OSSIOReader&) = delete;
    OSSIOReader&
    operator=(OSSIOReader&&) = delete;

    bool
    open(const std::string& name) override;

    void
    read(void* ptr, int64_t size) override;

    void
    seekg(int64_t pos) override;

    int64_t
    length() override;

    void
    close() override;

 public:
    std::string name_;
    std::string buffer_;
    int64_t pos_;
};

using OSSIOReaderPtr = std::shared_ptr<OSSIOReader>;

}  // namespace storage
}  // namespace milvus
