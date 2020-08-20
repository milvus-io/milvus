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

#ifndef MILVUS_STORAGE_EXTRAFILEINFO_H_
#define MILVUS_STORAGE_EXTRAFILEINFO_H_

#include <cstdio>
#include <cstring>
#include <string>
#include <unordered_map>

#include <src/log/Log.h>
#include <src/utils/Error.h>
#include <src/utils/Exception.h>

#include "storage/FSHandler.h"

#define MAGIC "Milvus"
#define MAGIC_SIZE 6
#define SINGLE_KV_DATA_SIZE 64
#define HEADER_SIZE 4096
#define SUM_SIZE 16

namespace milvus {
namespace storage {

#define CHECK_MAGIC_VALID(PTR, FILE_PATH)                                    \
    if (!CheckMagic(PTR, FILE_PATH)) {                                       \
        throw Exception(SERVER_FILE_MAGIC_BYTES_ERROR, "wrong magic bytes"); \
    }

#define CHECK_SUM_VALID(PTR, FILE_PATH)                                                      \
    if (!CheckSum(PTR, FILE_PATH)) {                                                         \
        throw Exception(SERVER_FILE_SUM_BYTES_ERROR, "wrong sum bytes,file may be changed"); \
    }

#define WRITE_MAGIC(PTR, FILE_PATH) \
    try {                           \
        WriteMagic(PTR, FILE_PATH); \
    } catch (...) {                 \
        throw "write magic failed"; \
    }
#define WRITE_HEADER(PTR, FILE_PATH, KV)       \
    try {                                      \
        WriteHeaderValues(PTR, FILE_PATH, KV); \
    } catch (...) {                            \
        throw "write sum failed";              \
    }

#define WRITE_SUM(PTR, FILE_PATH)                  \
    try {                                          \
        int result = CalculateSum(PTR, FILE_PATH); \
        WriteSum(PTR, FILE_PATH, result);          \
    } catch (...) {                                \
        throw "write sum failed";                  \
    }

void
WriteMagic(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path);

bool
CheckMagic(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path);

bool
CheckSum(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path);

void
WriteSum(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, int result, bool written = false);

std::uint8_t
CalculateSum(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, bool written = false);

std::string
ReadHeaderValue(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, const std::string& key);

std::unordered_map<std::string, std::string>
ReadHeaderValues(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path);

bool
WriteHeaderValue(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, const std::string& key,
                 const std::string& value);

bool
WriteHeaderValues(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                  const std::unordered_map<std::string, std::string>& maps);

}  // namespace storage
}  // namespace milvus
#endif  // end of MILVUS_STORAGE_EXTRAFILEINFO_H_
