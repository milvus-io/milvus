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

#include "db/wal/WalBuffer.h"

#include <cstring>

#include "db/wal/WalDefinations.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {
namespace wal {

inline std::string
ToFileName(int32_t file_no) {
    return std::to_string(file_no) + ".wal";
}

inline void
BuildLsn(uint32_t file_no, uint32_t offset, uint64_t& lsn) {
    lsn = (uint64_t)file_no << 32 | offset;
}

inline void
ParserLsn(uint64_t lsn, uint32_t& file_no, uint32_t& offset) {
    file_no = uint32_t(lsn >> 32);
    offset = uint32_t(lsn & LSN_OFFSET_MASK);
}

MXLogBuffer::MXLogBuffer(const std::string& mxlog_path, const uint32_t buffer_size)
    : mxlog_buffer_size_(buffer_size * UNIT_MB), mxlog_writer_(mxlog_path) {
}

MXLogBuffer::~MXLogBuffer() {
}

/**
 * alloc space for buffers
 * @param buffer_size
 * @return
 */
bool
MXLogBuffer::Init(uint64_t start_lsn, uint64_t end_lsn) {
    LOG_WAL_DEBUG_ << "start_lsn " << start_lsn << " end_lsn " << end_lsn;

    ParserLsn(start_lsn, mxlog_buffer_reader_.file_no, mxlog_buffer_reader_.buf_offset);
    ParserLsn(end_lsn, mxlog_buffer_writer_.file_no, mxlog_buffer_writer_.buf_offset);

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
                LOG_WAL_ERROR_ << "bad wal file " << i;
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
            LOG_WAL_INFO_ << "recovery will need more buffer, buffer size changed " << mxlog_buffer_size_;
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
                LOG_WAL_ERROR_ << "wal file not exist " << mxlog_buffer_writer_.file_no;
                return false;
            }

            auto read_offset = mxlog_buffer_reader_.buf_offset;
            auto read_size = mxlog_buffer_writer_.buf_offset - mxlog_buffer_reader_.buf_offset;
            if (!mxlog_writer_.Load(buf_[0].get() + read_offset, read_offset, read_size)) {
                LOG_WAL_ERROR_ << "load wal file error " << read_offset << " " << read_size;
                return false;
            }
        }

    } else {
        // read buffer
        mxlog_buffer_reader_.buf_idx = 0;

        MXLogFileHandler file_handler(mxlog_writer_.GetFilePath());
        file_handler.SetFileName(ToFileName(mxlog_buffer_reader_.file_no));
        file_handler.SetFileOpenMode("r");

        auto read_offset = mxlog_buffer_reader_.buf_offset;
        auto read_size = file_handler.Load(buf_[0].get() + read_offset, read_offset);
        mxlog_buffer_reader_.max_offset = read_size + read_offset;
        file_handler.CloseFile();

        // write buffer
        mxlog_buffer_writer_.buf_idx = 1;

        mxlog_writer_.SetFileName(ToFileName(mxlog_buffer_writer_.file_no));
        mxlog_writer_.SetFileOpenMode("r+");
        if (!mxlog_writer_.FileExists()) {
            LOG_WAL_ERROR_ << "wal file not exist " << mxlog_buffer_writer_.file_no;
            return false;
        }
        if (!mxlog_writer_.Load(buf_[1].get(), 0, mxlog_buffer_writer_.buf_offset)) {
            LOG_WAL_ERROR_ << "load wal file error " << mxlog_buffer_writer_.file_no;
            return false;
        }
    }

    SetFileNoFrom(mxlog_buffer_reader_.file_no);

    return true;
}

void
MXLogBuffer::Reset(uint64_t lsn) {
    LOG_WAL_DEBUG_ << "reset lsn " << lsn;

    buf_[0] = BufferPtr(new char[mxlog_buffer_size_]);
    buf_[1] = BufferPtr(new char[mxlog_buffer_size_]);

    ParserLsn(lsn, mxlog_buffer_writer_.file_no, mxlog_buffer_writer_.buf_offset);
    if (mxlog_buffer_writer_.buf_offset != 0) {
        mxlog_buffer_writer_.file_no++;
        mxlog_buffer_writer_.buf_offset = 0;
    }
    mxlog_buffer_writer_.buf_idx = 0;

    memcpy(&mxlog_buffer_reader_, &mxlog_buffer_writer_, sizeof(MXLogBufferHandler));

    mxlog_writer_.CloseFile();
    mxlog_writer_.SetFileName(ToFileName(mxlog_buffer_writer_.file_no));
    mxlog_writer_.SetFileOpenMode("w");

    SetFileNoFrom(mxlog_buffer_reader_.file_no);
}

uint32_t
MXLogBuffer::GetBufferSize() {
    return mxlog_buffer_size_;
}

// buffer writer cares about surplus space of buffer
uint32_t
MXLogBuffer::SurplusSpace() {
    return mxlog_buffer_size_ - mxlog_buffer_writer_.buf_offset;
}

uint32_t
MXLogBuffer::RecordSize(const MXLogRecord& record) {
    return SizeOfMXLogRecordHeader + (uint32_t)record.collection_id.size() + (uint32_t)record.partition_tag.size() +
           record.length * (uint32_t)sizeof(IDNumber) + record.data_size;
}

ErrorCode
MXLogBuffer::Append(MXLogRecord& record) {
    uint32_t record_size = RecordSize(record);
    if (SurplusSpace() < record_size) {
        // writer buffer has no space, switch wal file and write to a new buffer
        std::unique_lock<std::mutex> lck(mutex_);
        if (mxlog_buffer_writer_.buf_idx == mxlog_buffer_reader_.buf_idx) {
            // swith writer buffer
            mxlog_buffer_reader_.max_offset = mxlog_buffer_writer_.buf_offset;
            mxlog_buffer_writer_.buf_idx ^= 1;
        }
        mxlog_buffer_writer_.file_no++;
        mxlog_buffer_writer_.buf_offset = 0;
        lck.unlock();

        // Reborn means close old wal file and open new wal file
        if (!mxlog_writer_.ReBorn(ToFileName(mxlog_buffer_writer_.file_no), "w")) {
            LOG_WAL_ERROR_ << "ReBorn wal file error " << mxlog_buffer_writer_.file_no;
            return WAL_FILE_ERROR;
        }
    }

    // point to the offset of current record in wal file
    char* current_write_buf = buf_[mxlog_buffer_writer_.buf_idx].get();
    uint32_t current_write_offset = mxlog_buffer_writer_.buf_offset;

    MXLogRecordHeader head;
    BuildLsn(mxlog_buffer_writer_.file_no, mxlog_buffer_writer_.buf_offset + (uint32_t)record_size, head.mxl_lsn);
    head.mxl_type = (uint8_t)record.type;
    head.table_id_size = (uint16_t)record.collection_id.size();
    head.partition_tag_size = (uint16_t)record.partition_tag.size();
    head.vector_num = record.length;
    head.data_size = record.data_size;

    memcpy(current_write_buf + current_write_offset, &head, SizeOfMXLogRecordHeader);
    current_write_offset += SizeOfMXLogRecordHeader;

    if (!record.collection_id.empty()) {
        memcpy(current_write_buf + current_write_offset, record.collection_id.data(), record.collection_id.size());
        current_write_offset += record.collection_id.size();
    }

    if (!record.partition_tag.empty()) {
        memcpy(current_write_buf + current_write_offset, record.partition_tag.data(), record.partition_tag.size());
        current_write_offset += record.partition_tag.size();
    }
    if (record.ids != nullptr && record.length > 0) {
        memcpy(current_write_buf + current_write_offset, record.ids, record.length * sizeof(IDNumber));
        current_write_offset += record.length * sizeof(IDNumber);
    }

    if (record.data != nullptr && record.data_size > 0) {
        memcpy(current_write_buf + current_write_offset, record.data, record.data_size);
        current_write_offset += record.data_size;
    }

    bool write_rst = mxlog_writer_.Write(current_write_buf + mxlog_buffer_writer_.buf_offset, record_size);
    if (!write_rst) {
        LOG_WAL_ERROR_ << "write wal file error";
        return WAL_FILE_ERROR;
    }

    mxlog_buffer_writer_.buf_offset = current_write_offset;

    record.lsn = head.mxl_lsn;
    return WAL_SUCCESS;
}

ErrorCode
MXLogBuffer::Next(const uint64_t last_applied_lsn, MXLogRecord& record) {
    // init output
    record.type = MXLogType::None;

    // reader catch up to writer, no next record, read fail
    if (GetReadLsn() >= last_applied_lsn) {
        return WAL_SUCCESS;
    }

    // otherwise, it means there must exists next record, in buffer or wal log
    bool need_load_new = false;
    std::unique_lock<std::mutex> lck(mutex_);
    if (mxlog_buffer_reader_.file_no != mxlog_buffer_writer_.file_no) {
        if (mxlog_buffer_reader_.buf_offset == mxlog_buffer_reader_.max_offset) {  // last record
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
        uint32_t file_size = mxlog_reader.Load(buf_[mxlog_buffer_reader_.buf_idx].get(), 0);
        if (file_size == 0) {
            LOG_WAL_ERROR_ << "load wal file error " << mxlog_buffer_reader_.file_no;
            return WAL_FILE_ERROR;
        }
        mxlog_buffer_reader_.max_offset = file_size;
    }

    char* current_read_buf = buf_[mxlog_buffer_reader_.buf_idx].get();
    uint64_t current_read_offset = mxlog_buffer_reader_.buf_offset;

    MXLogRecordHeader* head = (MXLogRecordHeader*)(current_read_buf + current_read_offset);
    record.type = (MXLogType)head->mxl_type;
    record.lsn = head->mxl_lsn;
    record.length = head->vector_num;
    record.data_size = head->data_size;

    current_read_offset += SizeOfMXLogRecordHeader;

    if (head->table_id_size != 0) {
        record.collection_id.assign(current_read_buf + current_read_offset, head->table_id_size);
        current_read_offset += head->table_id_size;
    } else {
        record.collection_id = "";
    }

    if (head->partition_tag_size != 0) {
        record.partition_tag.assign(current_read_buf + current_read_offset, head->partition_tag_size);
        current_read_offset += head->partition_tag_size;
    } else {
        record.partition_tag = "";
    }

    if (head->vector_num != 0) {
        record.ids = (IDNumber*)(current_read_buf + current_read_offset);
        current_read_offset += head->vector_num * sizeof(IDNumber);
    } else {
        record.ids = nullptr;
    }

    if (record.data_size != 0) {
        record.data = current_read_buf + current_read_offset;
    } else {
        record.data = nullptr;
    }

    mxlog_buffer_reader_.buf_offset = uint32_t(head->mxl_lsn & LSN_OFFSET_MASK);
    return WAL_SUCCESS;
}

uint64_t
MXLogBuffer::GetReadLsn() {
    uint64_t read_lsn;
    BuildLsn(mxlog_buffer_reader_.file_no, mxlog_buffer_reader_.buf_offset, read_lsn);
    return read_lsn;
}

bool
MXLogBuffer::ResetWriteLsn(uint64_t lsn) {
    LOG_WAL_INFO_ << "reset write lsn " << lsn;

    int32_t old_file_no = mxlog_buffer_writer_.file_no;
    ParserLsn(lsn, mxlog_buffer_writer_.file_no, mxlog_buffer_writer_.buf_offset);
    if (old_file_no == mxlog_buffer_writer_.file_no) {
        LOG_WAL_DEBUG_ << "file No. is not changed";
        return true;
    }

    std::unique_lock<std::mutex> lck(mutex_);
    if (mxlog_buffer_writer_.file_no == mxlog_buffer_reader_.file_no) {
        mxlog_buffer_writer_.buf_idx = mxlog_buffer_reader_.buf_idx;
        LOG_WAL_DEBUG_ << "file No. is the same as reader";
        return true;
    }
    lck.unlock();

    if (!mxlog_writer_.ReBorn(ToFileName(mxlog_buffer_writer_.file_no), "r+")) {
        LOG_WAL_ERROR_ << "reborn file error " << mxlog_buffer_writer_.file_no;
        return false;
    }
    if (!mxlog_writer_.Load(buf_[mxlog_buffer_writer_.buf_idx].get(), 0, mxlog_buffer_writer_.buf_offset)) {
        LOG_WAL_ERROR_ << "load file error";
        return false;
    }

    return true;
}

void
MXLogBuffer::SetFileNoFrom(uint32_t file_no) {
    file_no_from_ = file_no;

    if (file_no > 0) {
        // remove the files whose No. are less than file_no
        MXLogFileHandler file_handler(mxlog_writer_.GetFilePath());
        do {
            file_handler.SetFileName(ToFileName(--file_no));
            if (!file_handler.FileExists()) {
                break;
            }
            LOG_WAL_INFO_ << "Delete wal file " << file_no;
            file_handler.DeleteFile();
        } while (file_no > 0);
    }
}

void
MXLogBuffer::RemoveOldFiles(uint64_t flushed_lsn) {
    uint32_t file_no;
    uint32_t offset;
    ParserLsn(flushed_lsn, file_no, offset);
    if (file_no_from_ < file_no) {
        MXLogFileHandler file_handler(mxlog_writer_.GetFilePath());
        do {
            file_handler.SetFileName(ToFileName(file_no_from_));
            LOG_WAL_INFO_ << "Delete wal file " << file_no_from_;
            file_handler.DeleteFile();
        } while (++file_no_from_ < file_no);
    }
}

}  // namespace wal
}  // namespace engine
}  // namespace milvus
