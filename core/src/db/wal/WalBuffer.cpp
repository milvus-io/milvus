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

#include <cstring>
#include "WalBuffer.h"
#include "WalDefinations.h"

namespace milvus {
namespace engine {
namespace wal {

MXLogBuffer::MXLogBuffer(uint64_t &buffer_size, const std::string &mxlog_path, const std::string &file_no)
: mxlog_buffer_size_(buffer_size)
, mxlog_writer_(mxlog_path, file_no)
{
    __glibcxx_assert(mxlog_buffer_size_ >= 0);
    mxlog_buffer_size_ = std::max(mxlog_buffer_size_, (uint64_t)WAL_BUFFER_MIN_SIZE * 1024 * 1024);
    if (Init()) {
        //todo: init fail, print error log
        return;
    }
}

MXLogBuffer::~MXLogBuffer() {
    /*
    if (buf_[0]){
        free(buf_[0]);
        buf_[0] = 0;
    }
    if (buf_[1]) {
        free(buf_[1]);
        buf_[1] = 0;
    }
     */
}

/**
 * alloc space 4 buffers
 * @param buffer_size
 * @return
 */
bool MXLogBuffer::Init() {
    //1:alloc space 4 two buffers
    //todo: use smart pointer
    /*
    buf_[0] = (char*)malloc(buffer_size);
    if (!buf_[0]) {
        return false;
    }
    buf_[1] = (char*)malloc(buffer_size);
    if (!buf_[1]) {
        if (buf_[0]) {
            free(buf_[0]);
            buf_[0] = 0;
        }
        return false;
    }
     */
    buf_[0] = BufferPtr(new char[mxlog_buffer_size_]);
    buf_[1] = BufferPtr(new char[mxlog_buffer_size_]);
    //2:init handlers of two buffers
    mxlog_buffer_writer_.buf_idx = mxlog_buffer_reader_.buf_idx = 0;
    mxlog_buffer_writer_.buf_offset = mxlog_buffer_reader_.buf_offset = 0;
    mxlog_buffer_writer_.file_no = write_file_no_;
    mxlog_buffer_reader_.file_no = 0;//reader file number equals 0 means read from buffer
//    mxlog_buffer_writer_.lsn = mxlog_buffer_writer_.min_lsn = ${WalManager.current_lsn};
//    mxlog_buffer_reader_.lsn = mxlog_buffer_reader_.min_lsn = ${WalManager.current_lsn};

    return true;
}

//buffer writer cares about surplus space of buffer
uint64_t MXLogBuffer::SurplusSpace() {
    return mxlog_buffer_size_ - mxlog_buffer_writer_.buf_offset;
}

uint64_t MXLogBuffer::RecordSize(const size_t n,
                                 const size_t dim,
                                 const size_t table_id_size) {
    uint64_t data_size = 0;
    data_size += n * (sizeof(IDNumber) + sizeof(float) * dim);
    data_size += table_id_size;
    return data_size + (uint64_t)SizeOfMXLogRecordHeader;
}

bool MXLogBuffer::Append(const std::string &table_id,
                         const size_t n,
                         const size_t dim,
                         const float *vectors,
                         const milvus::engine::IDNumbers& vector_ids,
                         const uint64_t lsn) {

    uint64_t record_size = RecordSize(n, dim, table_id.size());
    if (SurplusSpace() < record_size) {
        if (mxlog_buffer_writer_.buf_idx ^ mxlog_buffer_reader_.buf_idx) {//no need to switch buffer
            mxlog_buffer_writer_.buf_offset = 0;
            mxlog_writer_.ReBorn();
        } else { // swith writer buffer
            mxlog_buffer_writer_.buf_idx ^= 1;
            mxlog_buffer_writer_.buf_offset = 0;
        }
        mxlog_buffer_writer_.file_no ++;
    }
    char* current_write_buf = buf_[mxlog_buffer_writer_.buf_idx].get();
    uint64_t current_write_offset = mxlog_buffer_writer_.buf_offset;
    if (!current_write_offset)
        mxlog_buffer_writer_.min_lsn = lsn;
    memcpy(current_write_buf + current_write_offset, (char*)&lsn, 8);
    current_write_offset += 8;
    memcpy(current_write_buf + current_write_offset, (char*)&record_size, 4);
    current_write_offset += 4;
    memcpy(current_write_buf + current_write_offset, (char*)&n, 4);
    current_write_offset += 4;
    auto table_id_size = (uint16_t)table_id.size();
    memcpy(current_write_buf + current_write_offset, (char*)&table_id_size, 2);
    current_write_offset += 2;
    memcpy(current_write_buf + current_write_offset, (char*)&dim, 2);
    current_write_offset += 2;
    auto op_type = (uint8_t)MXLogType::Insert;
    memcpy(current_write_buf + current_write_offset, (char*)&op_type, 1);
    for (auto i = 0; i < n; ++ i) {
        memcpy(current_write_buf + current_write_offset, (char*)&vector_ids[i], 8);
        current_write_offset += 8;
    }
    memcpy(current_write_buf + current_write_offset, table_id.data(), table_id.size());
    current_write_offset += table_id.size();
    memcpy(current_write_buf + current_write_offset, vectors, (n * dim) << 2);
    current_write_offset += (n * dim) << 2;
    mxlog_buffer_writer_.buf_offset = current_write_offset;
    mxlog_buffer_writer_.lsn = lsn;
    mxlog_writer_.Write(mxlog_buffer_writer_, record_size);//default async flush
    return true;
}

bool MXLogBuffer::Next(std::string &table_id,
                       size_t &n,
                       size_t &dim,
                       float *vectors,
                       milvus::engine::IDNumbers &vector_ids,
                       uint64_t &lsn) {

    if (mxlog_buffer_reader_.buf_idx == mxlog_buffer_writer_.buf_idx
      && mxlog_buffer_reader_.lsn == mxlog_buffer_writer_.lsn) {
        return false;
    }

}

} // wal
} // engine
} // milvus