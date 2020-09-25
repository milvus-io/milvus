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
#include <vector>

#include "storage/Operation.h"

namespace milvus {
namespace storage {

class S3Operation : public Operation {
 public:
    explicit S3Operation(const std::string& dir_path);

    void
    CreateDirectory() override;

    const std::string&
    GetDirectory() const override;

    void
    ListDirectory(std::vector<std::string>& file_paths) override;

    bool
    DeleteFile(const std::string& file_path) override;

    bool
    Move(const std::string& tar_name, const std::string& src_name) override;

 private:
    const std::string dir_path_;
};

using S3OperationPtr = std::shared_ptr<S3Operation>;

}  // namespace storage
}  // namespace milvus
