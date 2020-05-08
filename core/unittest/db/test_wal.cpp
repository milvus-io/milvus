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

#include "db/wal/WalDefinations.h"
#define private public
#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>

#include <fstream>
#include <random>
#include <sstream>
#include <thread>

#include "db/meta/SqliteMetaImpl.h"
#include "db/wal/WalBuffer.h"
#include "db/wal/WalFileHandler.h"
#include "db/wal/WalManager.h"
#include "db/wal/WalMetaHandler.h"
#include "utils/Error.h"

namespace {

#define WAL_GTEST_PATH "/tmp/milvus/wal/test/"  // end with '/'

void
MakeEmptyTestPath() {
    pid_t ret;
    if (access(WAL_GTEST_PATH, 0) == 0) {
        ret = ::system("rm -rf " WAL_GTEST_PATH "*");
    } else {
        ret = ::system("mkdir -m 777 -p " WAL_GTEST_PATH);
    }
    __glibcxx_assert(ret != -1);
}

} // namespace

namespace milvus {
namespace engine {
namespace meta {

class TestWalMeta : public SqliteMetaImpl {
 public:
    explicit TestWalMeta(const DBMetaOptions& options) : SqliteMetaImpl(options) {
    }

    Status
    CreateCollection(CollectionSchema& table_schema) override {
        tables_.push_back(table_schema);
        return Status::OK();
    }

    Status
    AllCollections(std::vector<CollectionSchema>& table_schema_array) override {
        table_schema_array = tables_;
        return Status::OK();
    }

    Status
    SetGlobalLastLSN(uint64_t lsn) override {
        global_lsn_ = lsn;
        return Status::OK();
    }

    Status
    GetGlobalLastLSN(uint64_t& lsn) override {
        lsn = global_lsn_;
        return Status::OK();
    }

 private:
    std::vector<CollectionSchema> tables_;
    uint64_t global_lsn_ = 0;
};

class TestWalMetaError : public SqliteMetaImpl {
 public:
    explicit TestWalMetaError(const DBMetaOptions& options) : SqliteMetaImpl(options) {
    }

    Status
    AllCollections(std::vector<CollectionSchema>& table_schema_array) override {
        return Status(DB_ERROR, "error");
    }
};

}  // namespace meta
}  // namespace engine
}  // namespace milvus


TEST(WalTest, FILE_HANDLER_TEST) {
    MakeEmptyTestPath();

    std::string file_name = "1.wal";
    milvus::engine::wal::MXLogFileHandler file_handler(WAL_GTEST_PATH);
    file_handler.SetFilePath(WAL_GTEST_PATH);
    file_handler.SetFileName(file_name);
    file_handler.SetFileOpenMode("w");
    ASSERT_FALSE(file_handler.FileExists());

    ASSERT_TRUE(file_handler.OpenFile());
    ASSERT_EQ(0, file_handler.GetFileSize());

    std::string write_content = "hello, world!\n";
    ASSERT_TRUE(file_handler.Write(const_cast<char*>(write_content.data()), write_content.size()));
    ASSERT_TRUE(file_handler.CloseFile());

    file_handler.SetFileOpenMode("r");
    char* buf = (char*)malloc(write_content.size() + 10);
    memset(buf, 0, write_content.size() + 10);
    ASSERT_TRUE(file_handler.Load(buf, 0, write_content.size()));
    ASSERT_STREQ(buf, write_content.c_str());
    ASSERT_FALSE(file_handler.Load(buf, write_content.size()));
    free(buf);
    ASSERT_TRUE(file_handler.CloseFile());
    file_handler.DeleteFile();

    file_handler.ReBorn("2", "w");
    write_content += ", aaaaa";
    file_handler.Write(const_cast<char*>(write_content.data()), write_content.size());
    ASSERT_EQ("2", file_handler.GetFileName());
    ASSERT_TRUE(file_handler.CloseFile());
    file_handler.DeleteFile();
}

TEST(WalTest, META_HANDLER_TEST) {
    MakeEmptyTestPath();

    uint64_t wal_lsn = 0;
    uint64_t new_lsn = 103920;
    milvus::engine::wal::MXLogMetaHandler *meta_handler = nullptr;

    // first start
    meta_handler = new milvus::engine::wal::MXLogMetaHandler(WAL_GTEST_PATH);
    ASSERT_TRUE(meta_handler->GetMXLogInternalMeta(wal_lsn));
    ASSERT_EQ(wal_lsn, 0);
    delete meta_handler;

    // never write
    meta_handler = new milvus::engine::wal::MXLogMetaHandler(WAL_GTEST_PATH);
    ASSERT_TRUE(meta_handler->GetMXLogInternalMeta(wal_lsn));
    ASSERT_EQ(wal_lsn, 0);
    // write
    ASSERT_TRUE(meta_handler->SetMXLogInternalMeta(new_lsn));
    delete meta_handler;

    // read
    meta_handler = new milvus::engine::wal::MXLogMetaHandler(WAL_GTEST_PATH);
    ASSERT_TRUE(meta_handler->GetMXLogInternalMeta(wal_lsn));
    ASSERT_EQ(wal_lsn, new_lsn);
    delete meta_handler;

    // read error and nullptr point
    std::string file_full_path = WAL_GTEST_PATH;
    file_full_path += milvus::engine::wal::WAL_META_FILE_NAME;
    FILE *fi = fopen(file_full_path.c_str(), "w");
    uint64_t w[3] = {3, 4, 3};
    fwrite(w, sizeof(w), 1, fi);
    fclose(fi);

    meta_handler = new milvus::engine::wal::MXLogMetaHandler(WAL_GTEST_PATH);
    ASSERT_TRUE(meta_handler->GetMXLogInternalMeta(wal_lsn));
    ASSERT_EQ(wal_lsn, 3);

    if (meta_handler->wal_meta_fp_ != nullptr) {
        fclose(meta_handler->wal_meta_fp_);
        meta_handler->wal_meta_fp_ = nullptr;
    }
    meta_handler->SetMXLogInternalMeta(4);
    delete meta_handler;
}

TEST(WalTest, BUFFER_INIT_TEST) {
    MakeEmptyTestPath();

    FILE* fi = nullptr;
    char buff[128];
    milvus::engine::wal::MXLogBuffer buffer(WAL_GTEST_PATH, 32);

    // start_lsn == end_lsn, start_lsn == 0
    ASSERT_TRUE(buffer.Init(0, 0));
    ASSERT_EQ(buffer.mxlog_buffer_reader_.file_no, 0);
    ASSERT_EQ(buffer.mxlog_buffer_reader_.buf_offset, 0);
    ASSERT_EQ(buffer.mxlog_buffer_writer_.file_no, 0);
    ASSERT_EQ(buffer.mxlog_buffer_writer_.buf_offset, 0);
    ASSERT_EQ(buffer.file_no_from_, 0);

    // start_lsn == end_lsn, start_lsn != 0
    uint32_t file_no = 1;
    uint32_t buf_off = 32;
    uint64_t lsn = (uint64_t)file_no << 32 | buf_off;
    ASSERT_TRUE(buffer.Init(lsn, lsn));
    ASSERT_EQ(buffer.mxlog_buffer_reader_.file_no, file_no + 1);
    ASSERT_EQ(buffer.mxlog_buffer_reader_.buf_offset, 0);
    ASSERT_EQ(buffer.mxlog_buffer_writer_.file_no, file_no + 1);
    ASSERT_EQ(buffer.mxlog_buffer_writer_.buf_offset, 0);
    ASSERT_EQ(buffer.file_no_from_, file_no + 1);

    // start_lsn != end_lsn, start_file == end_file
    uint32_t start_file_no = 3;
    uint32_t start_buf_off = 32;
    uint64_t start_lsn = (uint64_t)start_file_no << 32 | start_buf_off;
    uint32_t end_file_no = 3;
    uint32_t end_buf_off = 64;
    uint64_t end_lsn = (uint64_t)end_file_no << 32 | end_buf_off;

    ASSERT_FALSE(buffer.Init(start_lsn, end_lsn)); // file not exist
    fi = fopen(WAL_GTEST_PATH "3.wal", "w");
    fclose(fi);
    ASSERT_FALSE(buffer.Init(start_lsn, end_lsn));  // file size zero
    fi = fopen(WAL_GTEST_PATH "3.wal", "w");
    fwrite(buff, 1, end_buf_off - 1, fi);
    fclose(fi);
    ASSERT_FALSE(buffer.Init(start_lsn, end_lsn));  // file size error
    fi = fopen(WAL_GTEST_PATH "3.wal", "w");
    fwrite(buff, 1, end_buf_off, fi);
    fclose(fi);
    ASSERT_TRUE(buffer.Init(start_lsn, end_lsn));  // success
    ASSERT_EQ(buffer.mxlog_buffer_reader_.file_no, start_file_no);
    ASSERT_EQ(buffer.mxlog_buffer_reader_.buf_offset, start_buf_off);
    ASSERT_EQ(buffer.mxlog_buffer_writer_.file_no, end_file_no);
    ASSERT_EQ(buffer.mxlog_buffer_writer_.buf_offset, end_buf_off);
    ASSERT_EQ(buffer.file_no_from_, start_file_no);

    // start_lsn != end_lsn, start_file != end_file
    start_file_no = 4;
    start_buf_off = 32;
    start_lsn = (uint64_t)start_file_no << 32 | start_buf_off;
    end_file_no = 5;
    end_buf_off = 64;
    end_lsn = (uint64_t)end_file_no << 32 | end_buf_off;
    ASSERT_FALSE(buffer.Init(start_lsn, end_lsn));  // file 4 not exist
    fi = fopen(WAL_GTEST_PATH "4.wal", "w");
    fwrite(buff, 1, start_buf_off, fi);
    fclose(fi);
    ASSERT_FALSE(buffer.Init(start_lsn, end_lsn));  // file 5 not exist
    fi = fopen(WAL_GTEST_PATH "5.wal", "w");
    fclose(fi);
    ASSERT_FALSE(buffer.Init(start_lsn, end_lsn));  // file 5 size error
    fi = fopen(WAL_GTEST_PATH "5.wal", "w");
    fwrite(buff, 1, end_buf_off, fi);
    fclose(fi);
    buffer.mxlog_buffer_size_ = 0;                 // to correct the buff size by buffer_size_need
    ASSERT_TRUE(buffer.Init(start_lsn, end_lsn));  // success
    ASSERT_EQ(buffer.mxlog_buffer_reader_.file_no, start_file_no);
    ASSERT_EQ(buffer.mxlog_buffer_reader_.buf_offset, start_buf_off);
    ASSERT_EQ(buffer.mxlog_buffer_writer_.file_no, end_file_no);
    ASSERT_EQ(buffer.mxlog_buffer_writer_.buf_offset, end_buf_off);
    ASSERT_EQ(buffer.file_no_from_, start_file_no);
    ASSERT_EQ(buffer.mxlog_buffer_size_, end_buf_off);
}

TEST(WalTest, BUFFER_TEST) {
    MakeEmptyTestPath();

    milvus::engine::wal::MXLogBuffer buffer(WAL_GTEST_PATH, 2048);

    uint32_t file_no = 4;
    uint32_t buf_off = 100;
    uint64_t lsn = (uint64_t)file_no << 32 | buf_off;
    buffer.mxlog_buffer_size_ = 1000;
    buffer.Reset(lsn);

    milvus::engine::wal::MXLogRecord record[4];
    milvus::engine::wal::MXLogRecord read_rst;

    // write 0
    record[0].type = milvus::engine::wal::MXLogType::InsertVector;
    record[0].collection_id = "insert_table";
    record[0].partition_tag = "parti1";
    record[0].length = 50;
    record[0].ids = (milvus::engine::IDNumber*)malloc(record[0].length * sizeof(milvus::engine::IDNumber));
    record[0].data_size = record[0].length * sizeof(float);
    record[0].data = malloc(record[0].data_size);
    ASSERT_EQ(buffer.Append(record[0]), milvus::WAL_SUCCESS);
    uint32_t new_file_no = uint32_t(record[0].lsn >> 32);
    ASSERT_EQ(new_file_no, ++file_no);

    // write 1
    record[1].type = milvus::engine::wal::MXLogType::Delete;
    record[1].collection_id = "insert_table";
    record[1].partition_tag = "parti1";
    record[1].length = 10;
    record[1].ids = (milvus::engine::IDNumber*)malloc(record[0].length * sizeof(milvus::engine::IDNumber));
    record[1].data_size = 0;
    record[1].data = nullptr;
    ASSERT_EQ(buffer.Append(record[1]), milvus::WAL_SUCCESS);
    new_file_no = uint32_t(record[1].lsn >> 32);
    ASSERT_EQ(new_file_no, file_no);

    // read 0
    ASSERT_EQ(buffer.Next(record[1].lsn, read_rst), milvus::WAL_SUCCESS);
    ASSERT_EQ(read_rst.type, record[0].type);
    ASSERT_EQ(read_rst.collection_id, record[0].collection_id);
    ASSERT_EQ(read_rst.partition_tag, record[0].partition_tag);
    ASSERT_EQ(read_rst.length, record[0].length);
    ASSERT_EQ(memcmp(read_rst.ids, record[0].ids, read_rst.length * sizeof(milvus::engine::IDNumber)), 0);
    ASSERT_EQ(read_rst.data_size, record[0].data_size);
    ASSERT_EQ(memcmp(read_rst.data, record[0].data, read_rst.data_size), 0);

    // read 1
    ASSERT_EQ(buffer.Next(record[1].lsn, read_rst), milvus::WAL_SUCCESS);
    ASSERT_EQ(read_rst.type, record[1].type);
    ASSERT_EQ(read_rst.collection_id, record[1].collection_id);
    ASSERT_EQ(read_rst.partition_tag, record[1].partition_tag);
    ASSERT_EQ(read_rst.length, record[1].length);
    ASSERT_EQ(memcmp(read_rst.ids, record[1].ids, read_rst.length * sizeof(milvus::engine::IDNumber)), 0);
    ASSERT_EQ(read_rst.data_size, 0);
    ASSERT_EQ(read_rst.data, nullptr);

    // read empty
    ASSERT_EQ(buffer.Next(record[1].lsn, read_rst), milvus::WAL_SUCCESS);
    ASSERT_EQ(read_rst.type, milvus::engine::wal::MXLogType::None);

    // write 2 (new file)
    record[2].type = milvus::engine::wal::MXLogType::InsertVector;
    record[2].collection_id = "insert_table";
    record[2].partition_tag = "parti1";
    record[2].length = 50;
    record[2].ids = (milvus::engine::IDNumber*)malloc(record[2].length * sizeof(milvus::engine::IDNumber));
    record[2].data_size = record[2].length * sizeof(float);
    record[2].data = malloc(record[2].data_size);
    ASSERT_EQ(buffer.Append(record[2]), milvus::WAL_SUCCESS);
    new_file_no = uint32_t(record[2].lsn >> 32);
    ASSERT_EQ(new_file_no, ++file_no);

    // write 3 (new file)
    record[3].type = milvus::engine::wal::MXLogType::InsertBinary;
    record[3].collection_id = "insert_table";
    record[3].partition_tag = "parti1";
    record[3].length = 100;
    record[3].ids = (milvus::engine::IDNumber*)malloc(record[3].length * sizeof(milvus::engine::IDNumber));
    record[3].data_size = record[3].length * sizeof(uint8_t);
    record[3].data = malloc(record[3].data_size);
    ASSERT_EQ(buffer.Append(record[3]), milvus::WAL_SUCCESS);
    new_file_no = uint32_t(record[3].lsn >> 32);
    ASSERT_EQ(new_file_no, ++file_no);

    // reset write lsn (record 2)
    ASSERT_TRUE(buffer.ResetWriteLsn(record[3].lsn));
    ASSERT_TRUE(buffer.ResetWriteLsn(record[2].lsn));
    ASSERT_TRUE(buffer.ResetWriteLsn(record[1].lsn));

    // write 2 and 3 again
    ASSERT_EQ(buffer.Append(record[2]), milvus::WAL_SUCCESS);
    ASSERT_EQ(buffer.Append(record[3]), milvus::WAL_SUCCESS);

    // read 2
    ASSERT_EQ(buffer.Next(record[3].lsn, read_rst), milvus::WAL_SUCCESS);
    ASSERT_EQ(read_rst.type, record[2].type);
    ASSERT_EQ(read_rst.collection_id, record[2].collection_id);
    ASSERT_EQ(read_rst.partition_tag, record[2].partition_tag);
    ASSERT_EQ(read_rst.length, record[2].length);
    ASSERT_EQ(memcmp(read_rst.ids, record[2].ids, read_rst.length * sizeof(milvus::engine::IDNumber)), 0);
    ASSERT_EQ(read_rst.data_size, record[2].data_size);
    ASSERT_EQ(memcmp(read_rst.data, record[2].data, read_rst.data_size), 0);

    // read 3
    ASSERT_EQ(buffer.Next(record[3].lsn, read_rst), milvus::WAL_SUCCESS);
    ASSERT_EQ(read_rst.type, record[3].type);
    ASSERT_EQ(read_rst.collection_id, record[3].collection_id);
    ASSERT_EQ(read_rst.partition_tag, record[3].partition_tag);
    ASSERT_EQ(read_rst.length, record[3].length);
    ASSERT_EQ(memcmp(read_rst.ids, record[3].ids, read_rst.length * sizeof(milvus::engine::IDNumber)), 0);
    ASSERT_EQ(read_rst.data_size, record[3].data_size);
    ASSERT_EQ(memcmp(read_rst.data, record[3].data, read_rst.data_size), 0);

    // test an empty record
    milvus::engine::wal::MXLogRecord empty;
    empty.type = milvus::engine::wal::MXLogType::None;
    empty.length = 0;
    empty.data_size = 0;
    ASSERT_EQ(buffer.Append(empty), milvus::WAL_SUCCESS);
    ASSERT_EQ(buffer.Next(empty.lsn, read_rst), milvus::WAL_SUCCESS);
    ASSERT_EQ(read_rst.type, milvus::engine::wal::MXLogType::None);
    ASSERT_TRUE(read_rst.collection_id.empty());
    ASSERT_TRUE(read_rst.partition_tag.empty());
    ASSERT_EQ(read_rst.length, 0);
    ASSERT_EQ(read_rst.data_size, 0);

    // remove old files
    buffer.RemoveOldFiles(record[3].lsn);
    ASSERT_EQ(buffer.file_no_from_, file_no);

    // clear writen lsn and reset failed
    buffer.mxlog_buffer_writer_.file_no = 0;
    buffer.mxlog_buffer_writer_.buf_offset = 0;
    ASSERT_FALSE(buffer.ResetWriteLsn(record[1].lsn));

    // clear writen lsn and reset failed
    FILE *fi = fopen(WAL_GTEST_PATH "5.wal", "w");
    fclose(fi);
    buffer.mxlog_buffer_writer_.file_no = 0;
    buffer.mxlog_buffer_writer_.buf_offset = 0;
    ASSERT_FALSE(buffer.ResetWriteLsn(record[1].lsn));

    for (int i = 0; i < 3; i++) {
        if (record[i].ids != nullptr) {
            free((void*)record[i].ids);
        }
        if (record[i].data != nullptr) {
            free((void*)record[i].data);
        }
    }
}

TEST(WalTest, HYBRID_BUFFFER_TEST) {
    MakeEmptyTestPath();

    milvus::engine::wal::MXLogBuffer buffer(WAL_GTEST_PATH, 2048);

    uint32_t file_no = 4;
    uint32_t buf_off = 100;
    uint64_t lsn = (uint64_t)file_no << 32 | buf_off;
    buffer.mxlog_buffer_size_ = 2000;
    buffer.Reset(lsn);

    milvus::engine::wal::MXLogRecord record[4];
    milvus::engine::wal::MXLogRecord read_rst;

    // write 0
    record[0].type = milvus::engine::wal::MXLogType::Entity;
    record[0].collection_id = "insert_hybrid_collection";
    record[0].partition_tag = "parti1";
    uint64_t length = 50;
    record[0].length = length;
    record[0].ids = (milvus::engine::IDNumber*)malloc(record[0].length * sizeof(milvus::engine::IDNumber));
    record[0].data_size = record[0].length * sizeof(float);
    record[0].data = malloc(record[0].data_size);
    record[0].field_names.resize(2);
    record[0].field_names[0] = "field_0";
    record[0].field_names[1] = "field_1";
    record[0].attr_data_size.insert(std::make_pair("field_0", length * sizeof(int64_t)));
    record[0].attr_data_size.insert(std::make_pair("field_1", length * sizeof(float)));
    record[0].attr_nbytes.insert(std::make_pair("field_0", sizeof(uint64_t)));
    record[0].attr_nbytes.insert(std::make_pair("field_1", sizeof(float)));

    std::vector<int64_t> data_0(length);
    std::default_random_engine e;
    std::uniform_int_distribution<unsigned> u(0, 1000);
    for (uint64_t i = 0; i < length; ++i) {
        data_0[i] = u(e);
    }
    std::vector<uint8_t> attr_data_0(length * sizeof(int64_t));
    memcpy(attr_data_0.data(), data_0.data(), length * sizeof(int64_t));
    record[0].attr_data.insert(std::make_pair("field_0", attr_data_0));

    std::vector<float> data_1(length);
    std::default_random_engine e1;
    std::uniform_real_distribution<float> u1(0, 1);
    for (uint64_t i = 0; i < length; ++i) {
        data_1[i] = u1(e1);
    }
    std::vector<uint8_t> attr_data_1(length * sizeof(float));
    memcpy(attr_data_1.data(), data_1.data(), length * sizeof(float));
    record[0].attr_data.insert(std::make_pair("field_1", attr_data_1));

    ASSERT_EQ(buffer.AppendEntity(record[0]), milvus::WAL_SUCCESS);
    uint32_t new_file_no = uint32_t(record[0].lsn >> 32);
    ASSERT_EQ(new_file_no, ++file_no);

    // write 1
    record[1].type = milvus::engine::wal::MXLogType::Delete;
    record[1].collection_id = "insert_hybrid_collection";
    record[1].partition_tag = "parti1";
    length = 10;
    record[1].length = length;
    record[1].ids = (milvus::engine::IDNumber*)malloc(record[0].length * sizeof(milvus::engine::IDNumber));
    record[1].data_size = 0;
    record[1].data = nullptr;
    record[1].field_names.resize(2);
    record[1].field_names[0] = "field_0";
    record[1].field_names[1] = "field_1";
    record[1].attr_data_size.insert(std::make_pair("field_0", length * sizeof(int64_t)));
    record[1].attr_data_size.insert(std::make_pair("field_1", length * sizeof(float)));
    record[1].attr_nbytes.insert(std::make_pair("field_0", sizeof(uint64_t)));
    record[1].attr_nbytes.insert(std::make_pair("field_1", sizeof(float)));

    std::vector<int64_t> data1_0(length);
    for (uint64_t i = 0; i < length; ++i) {
        data_0[i] = u(e);
    }
    std::vector<uint8_t> attr_data1_0(length * sizeof(int64_t));
    memcpy(attr_data1_0.data(), data1_0.data(), length * sizeof(int64_t));
    record[1].attr_data.insert(std::make_pair("field_0", attr_data1_0));

    std::vector<float> data1_1(length);
    for (uint64_t i = 0; i < length; ++i) {
        data_1[i] = u1(e1);
    }
    std::vector<uint8_t> attr_data1_1(length * sizeof(float));
    memcpy(attr_data1_1.data(), data1_1.data(), length * sizeof(float));
    record[1].attr_data.insert(std::make_pair("field_1", attr_data1_1));
    ASSERT_EQ(buffer.AppendEntity(record[1]), milvus::WAL_SUCCESS);
    new_file_no = uint32_t(record[1].lsn >> 32);
    ASSERT_EQ(new_file_no, file_no);

    // read 0
    ASSERT_EQ(buffer.NextEntity(record[1].lsn, read_rst), milvus::WAL_SUCCESS);
    ASSERT_EQ(read_rst.type, record[0].type);
    ASSERT_EQ(read_rst.collection_id, record[0].collection_id);
    ASSERT_EQ(read_rst.partition_tag, record[0].partition_tag);
    ASSERT_EQ(read_rst.length, record[0].length);
    ASSERT_EQ(memcmp(read_rst.ids, record[0].ids, read_rst.length * sizeof(milvus::engine::IDNumber)), 0);
    ASSERT_EQ(read_rst.data_size, record[0].data_size);
    ASSERT_EQ(memcmp(read_rst.data, record[0].data, read_rst.data_size), 0);
    ASSERT_EQ(read_rst.field_names.size(), record[0].field_names.size());
    ASSERT_EQ(read_rst.field_names[0], record[0].field_names[0]);
    ASSERT_EQ(read_rst.attr_data.at("field_0").size(), record[0].attr_data.at("field_0").size());
    ASSERT_EQ(read_rst.attr_nbytes.at("field_0"), record[0].attr_nbytes.at("field_0"));

    // read 1
    ASSERT_EQ(buffer.NextEntity(record[1].lsn, read_rst), milvus::WAL_SUCCESS);
    ASSERT_EQ(read_rst.type, record[1].type);
    ASSERT_EQ(read_rst.collection_id, record[1].collection_id);
    ASSERT_EQ(read_rst.partition_tag, record[1].partition_tag);
    ASSERT_EQ(read_rst.length, record[1].length);
    ASSERT_EQ(memcmp(read_rst.ids, record[1].ids, read_rst.length * sizeof(milvus::engine::IDNumber)), 0);
    ASSERT_EQ(read_rst.data_size, 0);
    ASSERT_EQ(read_rst.data, nullptr);
    ASSERT_EQ(read_rst.field_names.size(), record[1].field_names.size());
    ASSERT_EQ(read_rst.field_names[1], record[1].field_names[1]);
    ASSERT_EQ(read_rst.attr_data.at("field_1").size(), record[1].attr_data.at("field_1").size());
    ASSERT_EQ(read_rst.attr_nbytes.at("field_0"), record[1].attr_nbytes.at("field_0"));

    // read empty
    ASSERT_EQ(buffer.NextEntity(record[1].lsn, read_rst), milvus::WAL_SUCCESS);
    ASSERT_EQ(read_rst.type, milvus::engine::wal::MXLogType::None);

    // write 2 (new file)
    record[2].type = milvus::engine::wal::MXLogType::Entity;
    record[2].collection_id = "insert_table";
    record[2].partition_tag = "parti1";
    length = 50;
    record[2].length = length;
    record[2].ids = (milvus::engine::IDNumber*)malloc(record[2].length * sizeof(milvus::engine::IDNumber));
    record[2].data_size = record[2].length * sizeof(float);
    record[2].data = malloc(record[2].data_size);

    record[2].field_names.resize(2);
    record[2].field_names[0] = "field_0";
    record[2].field_names[1] = "field_1";
    record[2].attr_data_size.insert(std::make_pair("field_0", length * sizeof(int64_t)));
    record[2].attr_data_size.insert(std::make_pair("field_1", length * sizeof(float)));

    record[2].attr_data.insert(std::make_pair("field_0", attr_data_0));
    record[2].attr_data.insert(std::make_pair("field_1", attr_data_1));
    record[2].attr_nbytes.insert(std::make_pair("field_0", sizeof(uint64_t)));
    record[2].attr_nbytes.insert(std::make_pair("field_1", sizeof(float)));

    ASSERT_EQ(buffer.AppendEntity(record[2]), milvus::WAL_SUCCESS);
    new_file_no = uint32_t(record[2].lsn >> 32);
    ASSERT_EQ(new_file_no, ++file_no);

    // write 3 (new file)
    record[3].type = milvus::engine::wal::MXLogType::Entity;
    record[3].collection_id = "insert_table";
    record[3].partition_tag = "parti1";
    record[3].length = 10;
    record[3].ids = (milvus::engine::IDNumber*)malloc(record[3].length * sizeof(milvus::engine::IDNumber));
    record[3].data_size = record[3].length * sizeof(uint8_t);
    record[3].data = malloc(record[3].data_size);

    record[3].field_names.resize(2);
    record[3].field_names[0] = "field_0";
    record[3].field_names[1] = "field_1";
    record[3].attr_data_size.insert(std::make_pair("field_0", length * sizeof(int64_t)));
    record[3].attr_data_size.insert(std::make_pair("field_1", length * sizeof(float)));

    record[3].attr_data.insert(std::make_pair("field_0", attr_data1_0));
    record[3].attr_data.insert(std::make_pair("field_1", attr_data1_1));
    record[3].attr_nbytes.insert(std::make_pair("field_0", sizeof(uint64_t)));
    record[3].attr_nbytes.insert(std::make_pair("field_1", sizeof(float)));
    ASSERT_EQ(buffer.AppendEntity(record[3]), milvus::WAL_SUCCESS);
    new_file_no = uint32_t(record[3].lsn >> 32);
    ASSERT_EQ(new_file_no, ++file_no);

    // reset write lsn (record 2)
    ASSERT_TRUE(buffer.ResetWriteLsn(record[3].lsn));
    ASSERT_TRUE(buffer.ResetWriteLsn(record[2].lsn));
    ASSERT_TRUE(buffer.ResetWriteLsn(record[1].lsn));

    // write 2 and 3 again
    ASSERT_EQ(buffer.AppendEntity(record[2]), milvus::WAL_SUCCESS);
    ASSERT_EQ(buffer.AppendEntity(record[3]), milvus::WAL_SUCCESS);

    // read 2
    ASSERT_EQ(buffer.NextEntity(record[3].lsn, read_rst), milvus::WAL_SUCCESS);
    ASSERT_EQ(read_rst.type, record[2].type);
    ASSERT_EQ(read_rst.collection_id, record[2].collection_id);
    ASSERT_EQ(read_rst.partition_tag, record[2].partition_tag);
    ASSERT_EQ(read_rst.length, record[2].length);
    ASSERT_EQ(memcmp(read_rst.ids, record[2].ids, read_rst.length * sizeof(milvus::engine::IDNumber)), 0);
    ASSERT_EQ(read_rst.data_size, record[2].data_size);
    ASSERT_EQ(memcmp(read_rst.data, record[2].data, read_rst.data_size), 0);

    ASSERT_EQ(read_rst.field_names.size(), record[2].field_names.size());
    ASSERT_EQ(read_rst.field_names[1], record[2].field_names[1]);
    ASSERT_EQ(read_rst.attr_data.at("field_1").size(), record[2].attr_data.at("field_1").size());
    ASSERT_EQ(read_rst.attr_nbytes.at("field_0"), record[2].attr_nbytes.at("field_0"));

    // read 3
    ASSERT_EQ(buffer.NextEntity(record[3].lsn, read_rst), milvus::WAL_SUCCESS);
    ASSERT_EQ(read_rst.type, record[3].type);
    ASSERT_EQ(read_rst.collection_id, record[3].collection_id);
    ASSERT_EQ(read_rst.partition_tag, record[3].partition_tag);
    ASSERT_EQ(read_rst.length, record[3].length);
    ASSERT_EQ(memcmp(read_rst.ids, record[3].ids, read_rst.length * sizeof(milvus::engine::IDNumber)), 0);
    ASSERT_EQ(read_rst.data_size, record[3].data_size);
    ASSERT_EQ(memcmp(read_rst.data, record[3].data, read_rst.data_size), 0);

    ASSERT_EQ(read_rst.field_names.size(), record[3].field_names.size());
    ASSERT_EQ(read_rst.field_names[1], record[3].field_names[1]);
    ASSERT_EQ(read_rst.attr_nbytes.at("field_0"), record[3].attr_nbytes.at("field_0"));

    // test an empty record
    milvus::engine::wal::MXLogRecord empty;
    empty.type = milvus::engine::wal::MXLogType::None;
    empty.length = 0;
    empty.data_size = 0;
    ASSERT_EQ(buffer.AppendEntity(empty), milvus::WAL_SUCCESS);
    ASSERT_EQ(buffer.NextEntity(empty.lsn, read_rst), milvus::WAL_SUCCESS);
    ASSERT_EQ(read_rst.type, milvus::engine::wal::MXLogType::None);
    ASSERT_TRUE(read_rst.collection_id.empty());
    ASSERT_TRUE(read_rst.partition_tag.empty());
    ASSERT_EQ(read_rst.length, 0);
    ASSERT_EQ(read_rst.data_size, 0);

    // remove old files
    buffer.RemoveOldFiles(record[3].lsn);
    ASSERT_EQ(buffer.file_no_from_, file_no);

    // clear writen lsn and reset failed
    buffer.mxlog_buffer_writer_.file_no = 0;
    buffer.mxlog_buffer_writer_.buf_offset = 0;
    ASSERT_FALSE(buffer.ResetWriteLsn(record[1].lsn));

    // clear writen lsn and reset failed
    FILE *fi = fopen(WAL_GTEST_PATH "5.wal", "w");
    fclose(fi);
    buffer.mxlog_buffer_writer_.file_no = 0;
    buffer.mxlog_buffer_writer_.buf_offset = 0;
    ASSERT_FALSE(buffer.ResetWriteLsn(record[1].lsn));

    for (int i = 0; i < 3; i++) {
        if (record[i].ids != nullptr) {
            free((void*)record[i].ids);
        }
        if (record[i].data != nullptr) {
            free((void*)record[i].data);
        }
    }
}

TEST(WalTest, MANAGER_INIT_TEST) {
    MakeEmptyTestPath();

    milvus::engine::DBMetaOptions opt = {WAL_GTEST_PATH};
    milvus::engine::meta::MetaPtr meta = std::make_shared<milvus::engine::meta::TestWalMeta>(opt);

    milvus::engine::meta::CollectionSchema table_schema_1;
    table_schema_1.collection_id_ = "table1";
    table_schema_1.flush_lsn_ = (uint64_t)1 << 32 | 60;
    meta->CreateCollection(table_schema_1);

    milvus::engine::meta::CollectionSchema table_schema_2;
    table_schema_2.collection_id_ = "table2";
    table_schema_2.flush_lsn_ = (uint64_t)1 << 32 | 20;
    meta->CreateCollection(table_schema_2);

    milvus::engine::meta::CollectionSchema table_schema_3;
    table_schema_3.collection_id_ = "table3";
    table_schema_3.flush_lsn_ = (uint64_t)2 << 32 | 40;
    meta->CreateCollection(table_schema_3);

    milvus::engine::wal::MXLogConfiguration wal_config;
    wal_config.mxlog_path = WAL_GTEST_PATH;
    wal_config.mxlog_path.pop_back();
    wal_config.buffer_size = 64;
    wal_config.recovery_error_ignore = false;

    std::shared_ptr<milvus::engine::wal::WalManager> manager;
    manager = std::make_shared<milvus::engine::wal::WalManager>(wal_config);
    ASSERT_EQ(manager->Init(meta), milvus::WAL_FILE_ERROR);
    ASSERT_EQ(manager->p_buffer_->mxlog_buffer_reader_.file_no, 1);
    ASSERT_EQ(manager->p_buffer_->mxlog_buffer_reader_.buf_offset, 20);
    ASSERT_EQ(manager->p_buffer_->mxlog_buffer_writer_.file_no, 2);
    ASSERT_EQ(manager->p_buffer_->mxlog_buffer_writer_.buf_offset, 40);

    wal_config.recovery_error_ignore = true;
    manager = std::make_shared<milvus::engine::wal::WalManager>(wal_config);
    ASSERT_EQ(manager->Init(meta), milvus::WAL_SUCCESS);
    ASSERT_EQ(manager->last_applied_lsn_, table_schema_3.flush_lsn_);

    MakeEmptyTestPath();
    meta = std::make_shared<milvus::engine::meta::TestWalMetaError>(opt);
    manager = std::make_shared<milvus::engine::wal::WalManager>(wal_config);
    ASSERT_EQ(manager->Init(meta), milvus::WAL_META_ERROR);
}

TEST(WalTest, MANAGER_APPEND_FAILED) {
    MakeEmptyTestPath();

    milvus::engine::DBMetaOptions opt = {WAL_GTEST_PATH};
    milvus::engine::meta::MetaPtr meta = std::make_shared<milvus::engine::meta::TestWalMeta>(opt);

    milvus::engine::meta::CollectionSchema schema;
    schema.collection_id_ = "table1";
    schema.flush_lsn_ = 0;
    meta->CreateCollection(schema);

    milvus::engine::wal::MXLogConfiguration wal_config;
    wal_config.mxlog_path = WAL_GTEST_PATH;
    wal_config.buffer_size = 64;
    wal_config.recovery_error_ignore = true;

    std::shared_ptr<milvus::engine::wal::WalManager> manager;
    manager = std::make_shared<milvus::engine::wal::WalManager>(wal_config);
    ASSERT_EQ(manager->Init(meta), milvus::WAL_SUCCESS);

    // adjest the buffer size for test
    manager->mxlog_config_.buffer_size = 1000;
    manager->p_buffer_->mxlog_buffer_size_ = 1000;

    std::vector<int64_t> ids(1, 0);
    std::vector<float> data_float(1024, 0);
    ASSERT_FALSE(manager->Insert(schema.collection_id_, "", ids, data_float));

    ids.clear();
    data_float.clear();
    ASSERT_FALSE(manager->Insert(schema.collection_id_, "", ids, data_float));
    ASSERT_FALSE(manager->DeleteById(schema.collection_id_, ids));
}

TEST(WalTest, MANAGER_RECOVERY_TEST) {
    MakeEmptyTestPath();

    milvus::engine::DBMetaOptions opt = {WAL_GTEST_PATH};
    milvus::engine::meta::MetaPtr meta = std::make_shared<milvus::engine::meta::TestWalMeta>(opt);

    milvus::engine::wal::MXLogConfiguration wal_config;
    wal_config.mxlog_path = WAL_GTEST_PATH;
    wal_config.buffer_size = 64;
    wal_config.recovery_error_ignore = true;

    std::shared_ptr<milvus::engine::wal::WalManager> manager;
    manager = std::make_shared<milvus::engine::wal::WalManager>(wal_config);
    ASSERT_EQ(manager->Init(meta), milvus::WAL_SUCCESS);

    milvus::engine::meta::CollectionSchema schema;
    schema.collection_id_ = "collection";
    schema.flush_lsn_ = 0;
    meta->CreateCollection(schema);

    std::vector<int64_t> ids(1024, 0);
    std::vector<float> data_float(1024 * 512, 0);
    manager->CreateCollection(schema.collection_id_);
    ASSERT_TRUE(manager->Insert(schema.collection_id_, "", ids, data_float));

    // recovery
    manager = std::make_shared<milvus::engine::wal::WalManager>(wal_config);
    ASSERT_EQ(manager->Init(meta), milvus::WAL_SUCCESS);

    milvus::engine::wal::MXLogRecord record;
    while (1) {
        ASSERT_EQ(manager->GetNextRecovery(record), milvus::WAL_SUCCESS);
        if (record.type == milvus::engine::wal::MXLogType::None) {
            break;
        }
        ASSERT_EQ(record.type, milvus::engine::wal::MXLogType::InsertVector);
        ASSERT_EQ(record.collection_id, schema.collection_id_);
        ASSERT_EQ(record.partition_tag, "");
    }

    // change read, write point to let error happen
    uint32_t write_file_no = 10;
    manager->p_buffer_->mxlog_buffer_writer_.file_no = write_file_no;
    manager->p_buffer_->mxlog_buffer_writer_.buf_offset = 0;
    manager->p_buffer_->mxlog_buffer_writer_.buf_idx = 1 - manager->p_buffer_->mxlog_buffer_reader_.buf_idx;
    manager->p_buffer_->mxlog_buffer_reader_.max_offset = manager->p_buffer_->mxlog_buffer_reader_.buf_offset;
    manager->last_applied_lsn_ = (uint64_t) write_file_no << 32;
    // error happen and reset
    ASSERT_EQ(manager->GetNextRecovery(record), milvus::WAL_SUCCESS);
    ASSERT_EQ(manager->p_buffer_->mxlog_buffer_reader_.file_no, write_file_no);
    ASSERT_EQ(manager->p_buffer_->mxlog_buffer_reader_.buf_offset, 0);
    ASSERT_EQ(manager->p_buffer_->mxlog_buffer_writer_.file_no, write_file_no);
    ASSERT_EQ(manager->p_buffer_->mxlog_buffer_writer_.buf_offset, 0);
}

TEST(WalTest, MANAGER_TEST) {
    MakeEmptyTestPath();

    milvus::engine::DBMetaOptions opt = {WAL_GTEST_PATH};
    milvus::engine::meta::MetaPtr meta = std::make_shared<milvus::engine::meta::TestWalMeta>(opt);

    milvus::engine::wal::MXLogConfiguration wal_config;
    wal_config.mxlog_path = WAL_GTEST_PATH;
    wal_config.buffer_size = 64;
    wal_config.recovery_error_ignore = true;

    // first run
    std::shared_ptr<milvus::engine::wal::WalManager> manager =
        std::make_shared<milvus::engine::wal::WalManager>(wal_config);
    ASSERT_EQ(manager->Init(meta), milvus::WAL_SUCCESS);
    ASSERT_EQ(manager->p_buffer_->mxlog_buffer_reader_.file_no, 0);
    ASSERT_EQ(manager->p_buffer_->mxlog_buffer_reader_.buf_offset, 0);
    ASSERT_EQ(manager->p_buffer_->mxlog_buffer_writer_.file_no, 0);
    ASSERT_EQ(manager->p_buffer_->mxlog_buffer_writer_.buf_offset, 0);

    // adjest the buffer size for test
    manager->mxlog_config_.buffer_size = 8049;
    manager->p_buffer_->mxlog_buffer_size_ = 8049;

    std::vector<int64_t> ids(1024, 0);
    std::vector<float> data_float(1024 * 512, 0);
    std::vector<uint8_t> data_byte(1024 * 512, 0);

    // table1 create and insert
    std::string table_id_1 = "table1";
    manager->CreateCollection(table_id_1);
    ASSERT_TRUE(manager->Insert(table_id_1, "", ids, data_float));

    // table2 create and insert
    std::string table_id_2 = "table2";
    manager->CreateCollection(table_id_2);
    ASSERT_TRUE(manager->Insert(table_id_2, "", ids, data_byte));

    // table1 delete
    ASSERT_TRUE(manager->DeleteById(table_id_1, ids));

    // table3 create and insert
    std::string table_id_3 = "table3";
    manager->CreateCollection(table_id_3);
    ASSERT_TRUE(manager->Insert(table_id_3, "", ids, data_float));

    // flush table1
    auto flush_lsn = manager->Flush(table_id_1);
    ASSERT_NE(flush_lsn, 0);

    milvus::engine::wal::MXLogRecord record;
    uint64_t new_lsn = 0;

    while (1) {
        ASSERT_EQ(manager->GetNextRecord(record), milvus::WAL_SUCCESS);
        if (record.type == milvus::engine::wal::MXLogType::Flush) {
            ASSERT_EQ(record.collection_id, table_id_1);
            ASSERT_EQ(new_lsn, flush_lsn);
            manager->CollectionFlushed(table_id_1, new_lsn);
            break;

        } else {
            ASSERT_TRUE((record.type == milvus::engine::wal::MXLogType::InsertVector &&
                             record.collection_id == table_id_1) ||
                        (record.type == milvus::engine::wal::MXLogType::Delete &&
                             record.collection_id == table_id_1) ||
                        (record.type == milvus::engine::wal::MXLogType::InsertBinary &&
                             record.collection_id == table_id_2));
            new_lsn = record.lsn;
        }
    }
    manager->RemoveOldFiles(new_lsn);

    flush_lsn = manager->Flush(table_id_2);
    ASSERT_NE(flush_lsn, 0);

    ASSERT_EQ(manager->GetNextRecord(record), milvus::WAL_SUCCESS);
    ASSERT_EQ(record.type, milvus::engine::wal::MXLogType::Flush);
    ASSERT_EQ(record.collection_id, table_id_2);
    manager->CollectionFlushed(table_id_2, flush_lsn);
    ASSERT_EQ(manager->Flush(table_id_2), 0);

    flush_lsn = manager->Flush();
    ASSERT_NE(flush_lsn, 0);
    manager->DropCollection(table_id_3);

    ASSERT_EQ(manager->GetNextRecord(record), milvus::WAL_SUCCESS);
    ASSERT_EQ(record.type, milvus::engine::wal::MXLogType::Flush);
    ASSERT_TRUE(record.collection_id.empty());
}

TEST(WalTest, MANAGER_SAME_NAME_COLLECTION) {
    MakeEmptyTestPath();

    milvus::engine::DBMetaOptions opt = {WAL_GTEST_PATH};
    milvus::engine::meta::MetaPtr meta = std::make_shared<milvus::engine::meta::TestWalMeta>(opt);

    milvus::engine::wal::MXLogConfiguration wal_config;
    wal_config.mxlog_path = WAL_GTEST_PATH;
    wal_config.buffer_size = 64;
    wal_config.recovery_error_ignore = true;

    // first run
    std::shared_ptr<milvus::engine::wal::WalManager> manager =
        std::make_shared<milvus::engine::wal::WalManager>(wal_config);
    ASSERT_EQ(manager->Init(meta), milvus::WAL_SUCCESS);

    // adjest the buffer size for test
    manager->mxlog_config_.buffer_size = 16384;
    manager->p_buffer_->mxlog_buffer_size_ = 16384;

    std::string table_id_1 = "table1";
    std::string table_id_2 = "table2";
    std::vector<int64_t> ids(1024, 0);
    std::vector<uint8_t> data_byte(1024 * 512, 0);

    // create 2 tables
    manager->CreateCollection(table_id_1);
    manager->CreateCollection(table_id_2);

    // command
    ASSERT_TRUE(manager->Insert(table_id_1, "", ids, data_byte));
    ASSERT_TRUE(manager->Insert(table_id_2, "", ids, data_byte));
    ASSERT_TRUE(manager->DeleteById(table_id_1, ids));
    ASSERT_TRUE(manager->DeleteById(table_id_2, ids));

    // re-create collection
    manager->DropCollection(table_id_1);
    manager->CreateCollection(table_id_1);

    milvus::engine::wal::MXLogRecord record;
    while (1) {
        ASSERT_EQ(manager->GetNextRecord(record), milvus::WAL_SUCCESS);
        if (record.type == milvus::engine::wal::MXLogType::None) {
            break;
        }
        ASSERT_EQ(record.collection_id, table_id_2);
    }
}

#if 0
TEST(WalTest, LargeScaleRecords) {
    std::string data_path = "/home/zilliz/workspace/data/";
    milvus::engine::wal::MXLogConfiguration wal_config;
    wal_config.mxlog_path = "/tmp/milvus/wal/";
    wal_config.record_size = 2 * 1024 * 1024;
    wal_config.buffer_size = 32 * 1024 * 1024;
    wal_config.recovery_error_ignore = true;

    milvus::engine::wal::WalManager manager1(wal_config);
    manager1.mxlog_config_.buffer_size = 32 * 1024 * 1024;
    manager1.Init(nullptr);
    std::ifstream fin(data_path + "1.dat", std::ios::in);
    std::vector<milvus::engine::IDNumber> ids;
    std::vector<float> vecs;
    std::vector<uint8_t> bins;
    int type = -1;
    std::string line;

    while (getline(fin, line)) {
        std::istringstream istr(line);
        int cur_type, cur_id;
        istr >> cur_type;
        if (cur_type != type) {
            switch (type) {
                case 0:
                    manager1.Flush();
                    break;
                case 1:
                    manager1.Insert("insert_vector", "parti1", ids, vecs);
                    break;
                case 2:
                    manager1.Insert("insert_binary", "parti2", ids, bins);
                    break;
                case 3:
                    manager1.DeleteById("insert_vector", ids);
                    break;
                default:
                    std::cout << "invalid type: " << type << std::endl;
                    break;
            }
            ids.clear();
            vecs.clear();
            bins.clear();
        }
        type = cur_type;
        istr >> cur_id;
        ids.emplace_back(cur_id);
        if (cur_type == 1) {
            float v;
            for (auto i = 0; i < 10; ++i) {
                istr >> v;
                vecs.emplace_back(v);
            }
        } else if (cur_type == 2) {
            uint8_t b;
            for (auto i = 0; i < 20; ++i) {
                istr >> b;
                bins.emplace_back(b);
            }
        }
    }
    switch (type) {
        case 0:
            manager1.Flush();
            break;
        case 1:
            manager1.Insert("insert_vector", "parti1", ids, vecs);
            break;
        case 2:
            manager1.Insert("insert_binary", "parti2", ids, bins);
            break;
        case 3:
            manager1.DeleteById("insert_vector", ids);
            break;
        default:
            std::cout << "invalid type: " << type << std::endl;
            break;
    }
    fin.close();
}

TEST(WalTest, MultiThreadTest) {
    std::string data_path = "/home/zilliz/workspace/data/";
    milvus::engine::wal::MXLogConfiguration wal_config;
    wal_config.mxlog_path = "/tmp/milvus/wal/";
    wal_config.record_size = 2 * 1024 * 1024;
    wal_config.buffer_size = 32 * 1024 * 1024;
    wal_config.recovery_error_ignore = true;
    milvus::engine::wal::WalManager manager(wal_config);
    manager.mxlog_config_.buffer_size = 32 * 1024 * 1024;
    manager.Init(nullptr);
    auto read_fun = [&]() {
        std::ifstream fin(data_path + "1.dat", std::ios::in);
        std::vector<milvus::engine::IDNumber> ids;
        std::vector<float> vecs;
        std::vector<uint8_t> bins;
        int type = -1;
        std::string line;

        while (getline(fin, line)) {
            std::istringstream istr(line);
            int cur_type, cur_id;
            istr >> cur_type;
            if (cur_type != type) {
                switch (type) {
                    case 0:
                        manager.Flush();
                        break;
                    case 1:
                        manager.Insert("insert_vector", "parti1", ids, vecs);
                        break;
                    case 2:
                        manager.Insert("insert_binary", "parti2", ids, bins);
                        break;
                    case 3:
                        manager.DeleteById("insert_vector", ids);
                        break;
                    default:
                        std::cout << "invalid type: " << type << std::endl;
                        break;
                }
                ids.clear();
                vecs.clear();
                bins.clear();
            }
            type = cur_type;
            istr >> cur_id;
            ids.emplace_back(cur_id);
            if (cur_type == 1) {
                float v;
                for (auto i = 0; i < 10; ++i) {
                    istr >> v;
                    vecs.emplace_back(v);
                }
            } else if (cur_type == 2) {
                uint8_t b;
                for (auto i = 0; i < 20; ++i) {
                    istr >> b;
                    bins.emplace_back(b);
                }
            }
        }
        switch (type) {
            case 0:
                manager.Flush();
                break;
            case 1:
                manager.Insert("insert_vector", "parti1", ids, vecs);
                break;
            case 2:
                manager.Insert("insert_binary", "parti2", ids, bins);
                break;
            case 3:
                manager.DeleteById("insert_vector", ids);
                break;
            default:
                std::cout << "invalid type: " << type << std::endl;
                break;
        }
        fin.close();
    };

    auto write_fun = [&]() {
    };
    std::thread read_thread(read_fun);
    std::thread write_thread(write_fun);
    read_thread.join();
    write_thread.join();
}
#endif
