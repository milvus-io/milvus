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

namespace milvus {
namespace engine {
namespace wal {

using BufferPtr = std::shared_ptr<char>;

class MXLogBuffer {
 public:
    MXLogBuffer();
    ~MXLogBuffer();

    bool Append(const std::string &table_id,
                const MXLogType& record_type,
                const size_t& n,
                const size_t& dim,
                const float *vectors,
                const milvus::engine::IDNumbers& vector_ids,
                const size_t& vector_ids_offset,
                bool update_file_no,
                uint64_t& lsn);

    bool Next(std::string &table_id,
              size_t &n,
              size_t &dim,
              float *vectors,
              milvus::engine::IDNumbers &vector_ids,
              uint64_t &lsn);
    bool Next();
    void Flush(const std::string& table_id);
    void SwitchBuffer(MXLogBufferHandler &handler);//switch buffer
    void SetTableMeta(TableMetaPtr& p_table_meta);
    uint32_t GetWriterFileNo();
    void SetWriterFileNo(const uint32_t& file_no);
    void ReSet();

 private:
    bool Init();
    uint64_t SurplusSpace();
    uint64_t RecordSize(const size_t n, const size_t dim, const size_t table_id_size);
    void SetBufferSize(const uint64_t& buffer_size);
    void SetMXLogPath(const std::string& mxlog_path);


 private:
    uint64_t mxlog_buffer_size_;//from config
    std::string mxlog_path_;//from config
    BufferPtr buf_[2];
    std::mutex lock_;
    MXLogBufferHandler mxlog_buffer_reader_, mxlog_buffer_writer_;
    MXLogFileHandler mxlog_writer_;
    TableMetaPtr& p_table_meta_;
};

using MXLogBufferPtr = std::shared_ptr<MXLogBuffer>;

} // wal
} // engine
} // milvus