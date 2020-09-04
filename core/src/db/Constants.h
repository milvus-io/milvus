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

#include <stdint.h>

namespace milvus {
namespace engine {

constexpr int64_t KB = 1LL << 10;
constexpr int64_t MB = 1LL << 20;
constexpr int64_t GB = 1LL << 30;
constexpr int64_t TB = 1LL << 40;

constexpr int64_t MAX_MEM_SEGMENT_SIZE = 128 * MB;  // max data size of one segment in insert buffer

constexpr int64_t MAX_NAME_LENGTH = 255;                    // max string length for collection/partition/field name
constexpr int64_t MAX_DIMENSION = 32768;                    // max dimension of vector field
constexpr int32_t MAX_SEGMENT_ROW_COUNT = 4 * 1024 * 1024;  // max row count of one segment
constexpr int64_t DEFAULT_SEGMENT_ROW_COUNT = 512 * 1024;   // default row count per segment when creating collection
constexpr int64_t MAX_INSERT_DATA_SIZE = 256 * MB;          // max data size in one insert action
constexpr int64_t MAX_WAL_FILE_SIZE = 256 * MB;             // max file size of wal file
constexpr int64_t MAX_SCRIPT_FILE_SIZE = 256 * MB;          // max file size of transcript file

constexpr int64_t BUILD_INEDX_RETRY_TIMES = 3;  // retry times if build index failed

constexpr const char* DB_FOLDER = "/db";

}  // namespace engine
}  // namespace milvus
