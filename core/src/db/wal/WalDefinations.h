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


namespace milvus {
namespace engine {
namespace wal {

#define WAL_BUFFER_MIN_SIZE 64
#define offsetof(type, field) ((long) &((type *)0)->field)

enum class MXLogType {
    Insert,
    Delete,
    Update
};

#pragma pack(push)
#pragma pack(1)

struct MXLogRecord{
    uint32_t mxl_crc;//crc 4 this record
    uint32_t mxl_size;//data length
    uint64_t mxl_lsn;//log sequence number, self-increment by 1
    uint32_t vector_num;
    uint16_t table_id_size;//
    uint16_t dim;//one record contains the same dimension vectors
    uint8_t mxl_type;//record type, insert/delete/update/...
    //mxl_data include vecter_ids[vector_num], table_id and float* vectors
    char mxl_data[];//data address
//    char* mxl_data;
};

#pragma pack(pop)

//#define SizeOfMXLogRecordHeader (offsetof(MXLogRecord, mxl_data))
#define SizeOfMXLogRecordHeader (sizeof(MXLogRecord))

struct MXLogConfiguration {
    uint32_t record_size;
    uint64_t buffer_size;
};

struct MXLogBufferHandler {
    uint64_t lsn;
    uint64_t min_lsn;
    uint64_t max_offset;
    uint64_t file_no;
    uint64_t buf_offset;
    int buf_idx;
};

} //wal
} //engine
} //milvus
