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

#include <string>
#include <vector>
#include "utils/Status.h"

namespace milvus {
namespace storage {

class IStorage {
 public:
    virtual Status
    CreateBucket() = 0;
    virtual Status
    DeleteBucket() = 0;
    virtual Status
    PutObjectFile(const std::string& object_name, const std::string& file_path) = 0;
    virtual Status
    PutObjectStr(const std::string& object_name, const std::string& content) = 0;
    virtual Status
    GetObjectFile(const std::string& object_name, const std::string& file_path) = 0;
    virtual Status
    GetObjectStr(const std::string& object_name, std::string& content) = 0;
    virtual Status
    ListObjects(std::vector<std::string>& object_list, const std::string& marker = "") = 0;
    virtual Status
    DeleteObject(const std::string& object_name) = 0;
    virtual Status
    DeleteObjects(const std::string& marker) = 0;
};

}  // namespace storage
}  // namespace milvus
