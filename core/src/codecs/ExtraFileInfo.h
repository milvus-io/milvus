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

#include <cstdio>
#include <cstring>
#include <string>
#include <unordered_map>

#include "storage/FSHandler.h"
#include "utils/Error.h"
#include "utils/Exception.h"

extern const char* MAGIC;
extern const int64_t MAGIC_SIZE;
extern const int64_t HEADER_SIZE;
extern const int64_t SUM_SIZE;

namespace milvus {
namespace codec {

#define CHECK_MAGIC_VALID(PTR)                                               \
    if (!CheckMagic(PTR)) {                                                  \
        LOG_ENGINE_DEBUG_ << "Wrong Magic bytes";                            \
        throw Exception(SERVER_FILE_MAGIC_BYTES_ERROR, "Wrong magic bytes"); \
    }

#define CHECK_SUM_VALID(PTR)                                                                    \
    if (!CheckSum(PTR)) {                                                                       \
        LOG_ENGINE_DEBUG_ << "Wrong sum bytes, file has been changed";                          \
        throw Exception(SERVER_FILE_SUM_BYTES_ERROR, "Wrong sum bytes, file has been changed"); \
    }

#define WRITE_MAGIC(PTR)                           \
    try {                                          \
        WriteMagic(PTR);                           \
    } catch (...) {                                \
        LOG_ENGINE_DEBUG_ << "Write Magic failed"; \
        throw "Write Magic failed";                \
    }
#define WRITE_HEADER(PTR, KV)                       \
    try {                                           \
        WriteHeaderValues(PTR, KV);                 \
    } catch (...) {                                 \
        LOG_ENGINE_DEBUG_ << "Write header failed"; \
        throw "Write header failed";                \
    }

#define WRITE_SUM(PTR, HEADER, NUM_BYTES, DATA)  \
    try {                                        \
        WriteSum(PTR, HEADER, NUM_BYTES, DATA);  \
    } catch (...) {                              \
        LOG_ENGINE_DEBUG_ << "Write sum failed"; \
        throw "Write sum failed";                \
    }

void
WriteMagic(const storage::FSHandlerPtr& fs_ptr);

bool
CheckMagic(const storage::FSHandlerPtr& fs_ptr);

bool
CheckSum(const storage::FSHandlerPtr& fs_ptr);

void
WriteSum(const storage::FSHandlerPtr& fs_ptr, std::string header, char* data, size_t data_size);

std::uint32_t
CalculateSum(const storage::FSHandlerPtr& fs_ptr, bool written = false);

std::uint32_t
CalculateSum(char* data, size_t size);

std::string
ReadHeaderValue(const storage::FSHandlerPtr& fs_ptr, const std::string& key);

std::unordered_map<std::string, std::string>
ReadHeaderValues(const storage::FSHandlerPtr& fs_ptr);

bool
WriteHeaderValues(const storage::FSHandlerPtr& fs_ptr, const std::string& kv);

std::string
HeaderWrapper(const std::unordered_map<std::string, std::string>& maps);

using HeaderMap = std::unordered_map<std::string, std::string>;
}  // namespace codec
}  // namespace milvus
