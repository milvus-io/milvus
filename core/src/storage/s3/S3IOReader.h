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

#include <string>
#include "storage/IOReader.h"

namespace milvus {
namespace storage {

class S3IOReader : public IOReader {
 public:
    explicit S3IOReader(const std::string& name);
    ~S3IOReader();

    void
    read(void* ptr, size_t size) override;

    void
    seekg(size_t pos) override;

    size_t
    length() override;

 public:
    std::string buffer_;
    size_t pos_;
};

}  // namespace storage
}  // namespace milvus
