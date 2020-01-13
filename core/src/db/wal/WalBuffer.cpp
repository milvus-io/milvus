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
#include "utils/Log.h"

namespace milvus {
namespace engine {
namespace wal {

inline std::string
ToFileName(int32_t file_no) {
    return std::to_string(file_no) + ".wal";
}

inline void
BuildLsn(uint32_t file_no, uint32_t offset, uint64_t &lsn) {
    lsn = (uint64_t)file_no << 32 | offset;
}

inline void
ParserLsn(uint64_t lsn, uint32_t &file_no, uint32_t &offset) {
    file_no = uint32_t (lsn >> 32);
    offset = uint32_t (lsn & LSN_OFFSET_MASK);
}


MXLogBuffer::MXLogBuffer(const std::string &mxlog_path,
                         const uint32_t buffer_size)
: mxlog_buffer_size_(buffer_size)
, mxlog_writer_(mxlog_path)
{
    if (mxlog_buffer_size_ < (uint32_t)WAL_BUFFER_MIN_SIZE) {
        WAL_LOG_INFO << "config wal buffer size is too small " << mxlog_buffer_size_;
        mxlog_buffer_size_ = (uint32_t)WAL_BUFFER_MIN_SIZE;
    }
    else if (mxlog_buffer_size_ > (uint32_t)WAL_BUFFER_MAX_SIZE) {
        WAL_LOG_INFO << "config wal buffer size is too larger " << mxlog_buffer_size_;
        mxlog_buffer_size_ = (uint32_t)WAL_BUFFER_MAX_SIZE;
    }
}

MXLogBuffer::~MXLogBuffer() {

}

/**
 * alloc space for buffers
 * @param buffer_size
 * @return
 */
bool MXLogBuffer::Init(uint64_t start_lsn,
                       uint64_t end_lsn) {
    ParserLsn(start_lsn,
              mxlog_buffer_reader_.file_no,
              mxlog_buffer_reader_.buf_offset);
    ParserLsn(end_lsn,
              mxlog_buffer_writer_.file_no,
              mxlog_buffer_writer_.buf_offset);

    if (start_lsn == end_lsn) {
        // no data need recovery, start a new file_no
        if (mxlog_buffer_writer_.buf_offset != 0) {
            mxlog_buffer_writer_.file_no++;
            mxlog_buffer_writer_.buf_offset = 0;
            mxlog_buffer_reader_.file_no++;
            mxlog_buffer_reader_.buf_offset = 0;
        }
    } else {
        // to check whether buffer_size is enough
        MXLogFileHandler file_handler(mxlog_writer_.GetFilePath());

        uint32_t buffer_size_need = 0;
        for (auto i = mxlog_buffer_reader_.file_no; i < mxlog_buffer_writer_.file_no; i++) {
            file_handler.SetFileName(ToFileName(i));
            auto file_size = file_handler.GetFileSize();
            if (file_size == 0) {
                WAL_LOG_ERROR << "bad wal file " << i;
                return false;
            }
            if (file_size > buffer_size_need) {
                buffer_size_need = file_size;
            }
        }
        if (mxlog_buffer_writer_.buf_offset > buffer_size_need) {
            buffer_size_need = mxlog_buffer_writer_.buf_offset;
        }

        if (buffer_size_need > mxlog_buffer_size_) {
            mxlog_buffer_size_ = buffer_size_need;
            WAL_LOG_INFO << "recovery will need more buffer, buffer size changed "
                         << mxlog_buffer_size_;
        }
    }

    buf_[0] = BufferPtr(new char[mxlog_buffer_size_]);
    buf_[1] = BufferPtr(new char[mxlog_buffer_size_]);

    if (mxlog_buffer_reader_.file_no == mxlog_buffer_writer_.file_no) {
        // read-write buffer
        mxlog_buffer_reader_.buf_idx = 0;
        mxlog_buffer_writer_.buf_idx = 0;

        mxlog_writer_.SetFileName(ToFileName(mxlog_buffer_writer_.file_no));
        if (mxlog_buffer_writer_.buf_offset == 0) {
            mxlog_writer_.SetFileOpenMode("w");

        } else {
            mxlog_writer_.SetFileOpenMode("r+");
            if (!mxlog_writer_.FileExists()) {
                WAL_LOG_ERROR << "wal file not exist " << mxlog_buffer_writer_.file_no;
                return false;
            }

            if (!mxlog_writer_.Load(buf_[0].get() + mxlog_buffer_reader_.buf_offset,
                                    mxlog_buffer_reader_.buf_offset,
                                    mxlog_buffer_writer_.buf_offset - mxlog_buffer_reader_.buf_offset)) {
                WAL_LOG_ERROR << "load wal file error " << mxlog_buffer_reader_.buf_offset;
                return false;
            }
        }

    } else {
        // read buffer
        mxlog_buffer_reader_.buf_idx = 0;

        MXLogFileHandler file_handler(mxlog_writer_.GetFilePath());
        file_handler.SetFileName(ToFileName(mxlog_buffer_reader_.file_no));
        file_handler.SetFileOpenMode("r");
        if (!file_handler.FileExists()) {
            return false;

        }
        mxlog_buffer_reader_.max_offset = file_handler.GetFileSize();
        file_handler.Load(buf_[0].get() + mxlog_buffer_reader_.buf_offset,
                          mxlog_buffer_reader_.buf_offset,
                          mxlog_buffer_reader_.max_offset - mxlog_buffer_reader_.buf_offset);
        file_handler.CloseFile();

        // write buffer
        mxlog_buffer_writer_.buf_idx = 1;

        mxlog_writer_.SetFileName(ToFileName(mxlog_buffer_writer_.file_no));
        mxlog_writer_.SetFileOpenMode("r+");
        if (!mxlog_writer_.FileExists()) {
            WAL_LOG_ERROR << "wal file not exist " << mxlog_buffer_writer_.file_no;
            return false;
        }
        if (!mxlog_writer_.Load(buf_[1].get(), 0, mxlog_buffer_writer_.buf_offset)) {
            WAL_LOG_ERROR << "load wal file error " << mxlog_buffer_writer_.file_no;
            return false;
        }
    }

    return true;
}

void MXLogBuffer::Reset(uint64_t lsn) {
    buf_[0] = BufferPtr(new char[mxlog_buffer_size_]);
    buf_[1] = BufferPtr(new char[mxlog_buffer_size_]);

    ParserLsn(lsn,
              mxlog_buffer_writer_.file_no,
              mxlog_buffer_writer_.buf_offset);
    if (mxlog_buffer_writer_.buf_offset != 0) {
        mxlog_buffer_writer_.file_no++;
        mxlog_buffer_writer_.buf_offset = 0;
    }
    mxlog_buffer_writer_.buf_idx = 0;

    memcpy(&mxlog_buffer_reader_, &mxlog_buffer_writer_, sizeof(MXLogBufferHandler));

    mxlog_writer_.CloseFile();
    mxlog_writer_.SetFileName(ToFileName(mxlog_buffer_writer_.file_no));
    mxlog_writer_.SetFileOpenMode("w");
}

//buffer writer cares about surplus space of buffer
uint32_t MXLogBuffer::SurplusSpace() {
    return mxlog_buffer_size_ - mxlog_buffer_writer_.buf_offset;
}

uint32_t MXLogBuffer::RecordSize(const size_t n,
                                 const size_t no_type_dim,
                                 const size_t table_id_size) {
    auto data_size = uint32_t(table_id_size + n * (sizeof(IDNumber) + no_type_dim));
    return data_size + (uint32_t)SizeOfMXLogRecordHeader;
}

uint64_t MXLogBuffer::Append(const std::string &table_id,
                             const MXLogType record_type,
                             const size_t n,
                             const IDNumber* vector_ids,
                             const size_t dim,
                             const void *vectors) {

    size_t no_type_dim;
    switch (record_type) {
        case MXLogType::InsertVector:
            no_type_dim = dim * sizeof(float);
            break;
        case MXLogType::InsertBinary:
            no_type_dim = dim * sizeof(uint8_t);
            break;
        default:
            no_type_dim = 0;
            break;
    }

    uint32_t record_size = RecordSize(n, no_type_dim, table_id.size());
    if (SurplusSpace() < record_size) {
        //writer buffer has no space, switch wal file and write to a new buffer
        std::unique_lock<std::mutex> lck (mutex_);
        if (mxlog_buffer_writer_.buf_idx == mxlog_buffer_reader_.buf_idx) {
            // swith writer buffer
            mxlog_buffer_reader_.max_offset = mxlog_buffer_writer_.buf_offset;
            mxlog_buffer_writer_.buf_idx ^= 1;
        }
        mxlog_buffer_writer_.file_no ++;
        mxlog_buffer_writer_.buf_offset = 0;
        lck.unlock();

        // Reborn means close old wal file and open new wal file
        if (!mxlog_writer_.ReBorn(ToFileName(mxlog_buffer_writer_.file_no))) {
            WAL_LOG_ERROR << "ReBorn wal file error " << mxlog_buffer_writer_.file_no;
            return 0;
        }
    }

    //point to the offset of current record in wal file
    char* current_write_buf = buf_[mxlog_buffer_writer_.buf_idx].get();
    uint32_t current_write_offset = mxlog_buffer_writer_.buf_offset;

    MXLogRecordHeader head;
    BuildLsn(mxlog_buffer_writer_.file_no,
             mxlog_buffer_writer_.buf_offset + (uint32_t)record_size,
             head.mxl_lsn);
    head.vector_num = (uint32_t)n;
    head.table_id_size = (uint16_t)table_id.size();
    head.dim = (uint16_t)dim;
    head.mxl_type = (uint8_t)record_type;
    memcpy(current_write_buf + current_write_offset, &head, SizeOfMXLogRecordHeader);
    current_write_offset += SizeOfMXLogRecordHeader;

    memcpy(current_write_buf + current_write_offset, table_id.data(), table_id.size());
    current_write_offset += table_id.size();

    memcpy(current_write_buf + current_write_offset, vector_ids, n * sizeof(IDNumber));
    current_write_offset += n * sizeof(IDNumber);

    if (vectors != nullptr) {
        memcpy(current_write_buf + current_write_offset, vectors, n * no_type_dim);
        current_write_offset += n * no_type_dim;
    }

    bool write_rst = mxlog_writer_.Write(current_write_buf + mxlog_buffer_writer_.buf_offset,
                                         record_size);
    if (!write_rst) {
        WAL_LOG_ERROR << "write wal file error";
        return 0;
    }

    mxlog_buffer_writer_.buf_offset = current_write_offset;
    return head.mxl_lsn;
}

uint64_t MXLogBuffer::Next(const uint64_t last_applied_lsn,
                           std::string &table_id,
                           MXLogType &record_type,
                           size_t& n,
                           const IDNumber* &vector_ids,
                           size_t &dim,
                           const void* &vectors) {

    //reader catch up to writer, no next record, read fail
    uint64_t read_lsn;
    BuildLsn(mxlog_buffer_reader_.file_no, mxlog_buffer_reader_.buf_offset, read_lsn);
    if (read_lsn >= last_applied_lsn) {
        return 0;
    }

    //otherwise, it means there must exists next record, in buffer or wal log
    bool need_load_new = false;
    std::unique_lock<std::mutex> lck (mutex_);
    if (mxlog_buffer_reader_.file_no != mxlog_buffer_writer_.file_no) {
        if (mxlog_buffer_reader_.buf_offset == mxlog_buffer_reader_.max_offset) { // last record
            mxlog_buffer_reader_.file_no++;
            mxlog_buffer_reader_.buf_offset = 0;
            need_load_new = (mxlog_buffer_reader_.file_no != mxlog_buffer_writer_.file_no);
            if (!need_load_new) {
                // read reach write buffer
                mxlog_buffer_reader_.buf_idx = mxlog_buffer_writer_.buf_idx;
            }
        }
    }
    lck.unlock();

    if (need_load_new) {
        MXLogFileHandler mxlog_reader(mxlog_writer_.GetFilePath());
        mxlog_reader.SetFileName(ToFileName(mxlog_buffer_reader_.file_no));
        mxlog_reader.SetFileOpenMode("r");
        if (!mxlog_reader.OpenFile()) {
            WAL_LOG_ERROR << "read wal file error " << mxlog_buffer_reader_.file_no;
            return 0;
        }
        if (!mxlog_reader.Load(buf_[mxlog_buffer_reader_.buf_idx].get(),
                               0,
                               mxlog_reader.GetFileSize())) {
            WAL_LOG_ERROR << "load wal file error " << mxlog_buffer_reader_.file_no;
            return 0;
        }
        mxlog_buffer_reader_.max_offset = (uint32_t)mxlog_reader.GetFileSize();
    }


    char* current_read_buf = buf_[mxlog_buffer_reader_.buf_idx].get();
    uint64_t current_read_offset = mxlog_buffer_reader_.buf_offset;

    MXLogRecordHeader *head = (MXLogRecordHeader*)(current_read_buf + current_read_offset);
    record_type = (MXLogType)head->mxl_type;
    n = head->vector_num;
    dim = head->dim;
    current_read_offset += SizeOfMXLogRecordHeader;

    table_id.assign(current_read_buf + current_read_offset, head->table_id_size);
    current_read_offset += head->table_id_size;

    vector_ids = (IDNumber*)(current_read_buf + current_read_offset);

    if (dim != 0) {
        current_read_offset += n * sizeof(IDNumber);
        vectors = current_read_buf + current_read_offset;
    } else {
        vectors = nullptr;
    }

    mxlog_buffer_reader_.buf_offset = uint32_t (head->mxl_lsn & LSN_OFFSET_MASK);
    return head->mxl_lsn;
}

uint64_t MXLogBuffer::GetReadLsn() {
    uint64_t read_lsn;
    BuildLsn(mxlog_buffer_reader_.file_no, mxlog_buffer_reader_.buf_offset, read_lsn);
    return read_lsn;
}

bool MXLogBuffer::SetWriteLsn(uint64_t lsn) {
    int32_t old_file_no = mxlog_buffer_writer_.file_no;
    ParserLsn(lsn, mxlog_buffer_writer_.file_no, mxlog_buffer_writer_.buf_offset);
    if (old_file_no == mxlog_buffer_writer_.file_no) {
        return true;
    }

    std::unique_lock<std::mutex> lck(mutex_);
    if (mxlog_buffer_writer_.file_no == mxlog_buffer_reader_.file_no) {
        mxlog_buffer_writer_.buf_idx = mxlog_buffer_reader_.buf_idx;
        return true;
    }
    lck.unlock();

    if (!mxlog_writer_.ReBorn(ToFileName(mxlog_buffer_writer_.file_no))) {

        WAL_LOG_ERROR << "reborn file error " << mxlog_buffer_writer_.file_no;
        return false;
    }
    if (!mxlog_writer_.Load(buf_[mxlog_buffer_writer_.buf_idx].get(),
                            0,
                            mxlog_buffer_writer_.buf_offset)) {
        WAL_LOG_ERROR << "load file error";
        return false;
    }

    return true;
}

} // wal
} // engine
} // milvus