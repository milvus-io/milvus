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
#include "WalDefinations.h"

namespace milvus {
namespace engine {
namespace wal {

class MXLogFileHandler {
 public:
    MXLogFileHandler(const std::string &mxlog_path, const std::string &file_number);
    ~MXLogFileHandler();

    std::string GetFileName();
    void SetFileName(const std::string &file_name);
    void Read(char *buf);
    void Write(const MXLogBufferHandler &buf_handler, const uint64_t &data_size, bool is_sync = false);
    void DeleteFile();
    void CreateFile();
    void ReBorn();

 private:
    std::string file_name_;
    FILE* file_handler_;
};

} // wal
} // engine
} // milvus

