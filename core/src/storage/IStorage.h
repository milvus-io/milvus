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

#include "utils/Status.h"

namespace milvus {
namespace storage {

class IStorage {
 public:
    virtual Status
    Create(const std::string& ip_address, const std::string& port, const std::string& access_key,
           const std::string& secret_key) = 0;
    virtual Status
    Close() = 0;

    virtual Status
    CreateBucket(const std::string& bucket_name) = 0;
    virtual Status
    DeleteBucket(const std::string& bucket_name) = 0;
    virtual Status
    PutObjectFile(const std::string& bucket_name, const std::string& object_name, const std::string& file_path) = 0;
    virtual Status
    PutObjectStr(const std::string& bucket_name, const std::string& object_name, const std::string& content) = 0;
    virtual Status
    GetObjectFile(const std::string& bucket_name, const std::string& object_name, const std::string& file_path) = 0;
    virtual Status
    DeleteObject(const std::string& bucket_name, const std::string& object_name) = 0;
};

}  // namespace storage
}  // namespace milvus
