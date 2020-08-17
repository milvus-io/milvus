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

#pragma once

#include <memory>
#include <string>
#include "storage/IOReader.h"

namespace milvus {
namespace storage {

class S3IOReader : public IOReader {
 public:
    S3IOReader() = default;
    ~S3IOReader() = default;

    // No copy and move
    S3IOReader(const S3IOReader&) = delete;
    S3IOReader(S3IOReader&&) = delete;

    S3IOReader&
    operator=(const S3IOReader&) = delete;
    S3IOReader&
    operator=(S3IOReader&&) = delete;

    bool
    open(const std::string& name) override;

    bool
    Read(void* ptr, int64_t size) override;

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

using S3IOReaderPtr = std::shared_ptr<S3IOReader>;

}  // namespace storage
}  // namespace milvus
