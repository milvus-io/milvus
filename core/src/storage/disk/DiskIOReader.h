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

#include <fstream>
#include <memory>
#include <string>
#include "storage/IOReader.h"

namespace milvus {
namespace storage {

class DiskIOReader : public IOReader {
 public:
    DiskIOReader() = default;
    ~DiskIOReader() = default;

    // No copy and move
    DiskIOReader(const DiskIOReader&) = delete;
    DiskIOReader(DiskIOReader&&) = delete;

    DiskIOReader&
    operator=(const DiskIOReader&) = delete;
    DiskIOReader&
    operator=(DiskIOReader&&) = delete;

    bool
    Open(const std::string& name) override;

    void
    Read(void* ptr, int64_t size) override;

    void
    Seekg(int64_t pos) override;

    int64_t
    Length() override;

    void
    Close() override;

 public:
    std::string name_;
    std::fstream fs_;
};

using DiskIOReaderPtr = std::shared_ptr<DiskIOReader>;

}  // namespace storage
}  // namespace milvus
