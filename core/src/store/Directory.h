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

namespace milvus {
namespace store {

class Directory {
 public:
    explicit Directory(const std::string& dir_path);

    void
    Create();

    void
    ListAll(std::vector<std::string>& file_paths);

    bool
    DeleteFile(const std::string& file_path);

    const std::string&
    GetDirPath() const;

    // TODO(zhiru):
    //  open(), sync(), close()
    //  function that opens a stream for reading file
    //  function that creates a new, empty file and returns an stream for appending data to this file
    //  function that creates a new, empty, temporary file and returns an stream for appending data to this file

 private:
    const std::string dir_path_;
};

using DirectoryPtr = std::shared_ptr<Directory>;

}  // namespace store
}  // namespace milvus
