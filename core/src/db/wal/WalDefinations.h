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
#include <stdint-gcc.h>
#include <src/db/Types.h>
#include <condition_variable>
#include <unordered_map>
#include <src/db/meta/MetaTypes.h>


namespace milvus {
namespace engine {
namespace wal {

using TableSchemaPtr = std::shared_ptr<milvus::engine::meta::TableSchema>;
using TableMetaPtr = std::shared_ptr<std::unordered_map<std::string, TableSchemaPtr> >;
extern std::condition_variable reader_cv;
extern std::mutex reader_mutex;
extern bool reader_is_waiting;

#define WAL_BUFFER_MAX_SIZE (2 * 1024 * 1024 * 1024)
#define WAL_BUFFER_MIN_SIZE (64 * 1024 * 1024)
#define LSN_OFFSET_MASK 0x00000000ffffffff
#define WAL_META_AMOUNT 2
#ifdef offsetof
#undef offsetof
#endif
#define offsetof(type, field) ((long) &((type *)0)->field)

enum class MXLogType {
    InsertBinary,
    InsertVector,
    Delete,
    Update,
    Flush,
    None,
};

struct WALRecord {
    uint64_t lsn;
    MXLogType type;
    std::string table_id;
    size_t length;
    const IDNumber* ids;
    size_t dim;
    const void* data;
};

struct MXLogConfiguration {
    uint32_t record_size;
    uint32_t buffer_size;
    std::string mxlog_path;
};



} //wal
} //engine
} //milvus
