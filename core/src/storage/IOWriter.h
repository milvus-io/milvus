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

namespace milvus {
namespace storage {

class IOWriter {
 public:
    virtual void
    open(const std::string& name) = 0;

    virtual void
    write(void* ptr, size_t size) = 0;

    virtual size_t
    length() = 0;

    virtual void
    close() = 0;
};

}  // namespace storage
}  // namespace milvus
