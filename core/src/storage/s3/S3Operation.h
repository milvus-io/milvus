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
#include <vector>

#include "storage/Operation.h"
#include "storage/disk/DiskOperation.h"

namespace milvus {
namespace storage {

class S3Operation : public Operation {
 public:
    explicit S3Operation(const std::string& dir_path);

    void
    CreateDirectory() final;

    const std::string&
    GetDirectory() const final;

    void
    ListDirectory(std::vector<std::string>& file_paths) final;

    bool
    DeleteFile(const std::string& file_path) final;

    bool
    CacheGet(const std::string& file_path) final;

    bool
    CachePut(const std::string& file_path) final;

 private:
    const std::string dir_path_;
    DiskOperation local_operation_;
};

}  // namespace storage
}  // namespace milvus
