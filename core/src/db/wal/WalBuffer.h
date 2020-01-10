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

#include <atomic>
#include <mutex>
#include <memory>
#include <bits/shared_ptr.h>
#include "WalDefinations.h"
#include "WalFileHandler.h"
#include "WalMetaHandler.h"


namespace milvus {
namespace engine {
namespace wal {

#pragma pack(push)
#pragma pack(1)

struct MXLogRecordHeader{
    uint64_t mxl_lsn;//log sequence number, high 32 bits means file number which increasing by 1, low 32 bits means offset in a wal file, max 4GB
    uint32_t vector_num;
    uint16_t table_id_size;//
    uint16_t dim;//one record contains the same dimension vectors
    uint8_t mxl_type;//record type, insert/delete/update/flush...
};

#define SizeOfMXLogRecordHeader (sizeof(MXLogRecordHeader))

#pragma pack(pop)

struct MXLogBufferHandler {
    uint32_t max_offset;
    uint32_t file_no;
    uint32_t buf_offset;
    uint8_t buf_idx;
};

using BufferPtr = std::shared_ptr<char>;

class MXLogBuffer {
 public:
    MXLogBuffer(const std::string& mxlog_path,
                const uint32_t buffer_size,
                const uint64_t read_lsn,
                const uint64_t write_lsn);
    ~MXLogBuffer();

    // TODO: error code
    bool Init();

    // if failed, return 0, else return lsn
    uint64_t Append(const std::string &table_id,
                    const MXLogType record_type,
                    const size_t n,
                    const IDNumber* vector_ids,
                    const size_t dim,
                    const void *vectors);

    // if failed, return 0, else return lsn
    uint64_t Next(const uint64_t last_applied_lsn,
                  std::string &table_id,
                  MXLogType &record_type,
                  size_t& n,
                  const IDNumber* &vector_ids,
                  size_t &dim,
                  const void* &vectors);

    uint64_t GetReadLsn();

    void SetWriteLsn(uint64_t lsn);

 private:
    uint32_t SurplusSpace();
    uint32_t RecordSize(const size_t n,
                        const size_t no_type_dim,
                        const size_t table_id_size);

 private:
    uint32_t mxlog_buffer_size_; //from config
    BufferPtr buf_[2];
    std::mutex mutex_;
    MXLogBufferHandler mxlog_buffer_reader_;
    MXLogBufferHandler mxlog_buffer_writer_;
    MXLogFileHandler mxlog_writer_;
};

using MXLogBufferPtr = std::shared_ptr<MXLogBuffer>;

} // wal
} // engine
} // milvus