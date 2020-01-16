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

#include "db/wal/WalDefinations.h"
#include "db/wal/WalMetaHandler.h"
#include "db/wal/WalFileHandler.h"
#include "db/wal/WalManager.h"
#include "db/wal/WalBuffer.h"
#include "utils/Error.h"

#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <thread>

using namespace milvus::engine::wal;

TEST(WalTest, FILE_HANDLER_TEST) {
    std::string file_path = "/tmp/";
    std::string file_name = "1";
    std::string open_mode = "r+";
    MXLogFileHandler file_handler(file_path);
    file_handler.SetFileName(file_name);
    file_handler.SetFileOpenMode(open_mode);
    ASSERT_FALSE(file_handler.FileExists());
    ASSERT_FALSE(file_handler.IsOpen());

    file_handler.OpenFile();
    ASSERT_TRUE(file_handler.IsOpen());
    ASSERT_EQ(0, file_handler.GetFileSize());

    std::string write_content = "hello, world!\n";
    file_handler.Write(const_cast<char*>(write_content.data()), write_content.size());
    char* buf = (char*)malloc(write_content.size() + 10);
    memset(buf, 0, write_content.size() + 10);
    file_handler.Load(buf, 0, write_content.size());
    ASSERT_EQ(sizeof(buf), write_content.size());
    free(buf);

    file_handler.ReBorn("2");
    write_content += ", aaaaa";
    file_handler.Write(const_cast<char*>(write_content.data()), write_content.size());
    ASSERT_EQ("2", file_handler.GetFileName());
}

TEST(WalTest, META_HANDLER_TEST) {
    MXLogMetaHandler meta_handler("/tmp/milvus/");
    uint64_t wal_lsn = 103920;
    meta_handler.SetMXLogInternalMeta(wal_lsn);
    uint64_t internal_lsn;
    meta_handler.GetMXLogInternalMeta(internal_lsn);
    ASSERT_EQ(wal_lsn, internal_lsn);
}

TEST(WalTest, MANAGER_TEST) {
}

TEST(WalTest, BUFFER_TEST) {
    std::string log_path = "/tmp/milvus/wal/";
    uint32_t buf_size = 2 * 1024;
    MXLogBuffer buffer(log_path, buf_size);
    uint32_t start_file_no = 3;
    uint32_t start_buf_off = 230;
    uint32_t end_file_no = 5;
    uint32_t end_buf_off = 630;
    uint64_t start_lsn, end_lsn;
    start_lsn = (uint64_t)start_file_no << 32 | start_buf_off;
    end_lsn = (uint64_t)end_file_no << 32 | end_buf_off;
    buffer.Init(start_lsn, end_lsn);
    ASSERT_EQ(start_lsn, buffer.GetReadLsn());
    buffer.Reset(((uint64_t)4 << 32));

    MXLogRecord ins_rd;
    ins_rd.type = MXLogType::InsertVector;
    ins_rd.table_id = "insert_table";
    ins_rd.partition_tag = "parti1";
    ins_rd.length = 1;
    milvus::engine::IDNumber id = 2;
    ins_rd.ids = &id;
    ins_rd.data_size = 8;
    float vecs[2] = {1.2, 3.4};
    ins_rd.data = &vecs;
    buffer.Append(ins_rd);


    MXLogRecord del_rd;
    del_rd.type = MXLogType::Delete;
    del_rd.table_id = "insert_table";
    del_rd.partition_tag = "parti1";
    del_rd.length = 1;
    del_rd.ids = &id;
    buffer.Append(del_rd);

    MXLogRecord flush_rd;
    flush_rd.type = MXLogType::Flush;
    flush_rd.table_id = "insert_table";
    flush_rd.partition_tag = "parti1";
    buffer.Append(flush_rd);

    MXLogRecord none_rd;
    none_rd.type = MXLogType::None;
    none_rd.table_id = "insert_table";
    none_rd.partition_tag = "parti1";
    buffer.Append(none_rd);


    MXLogRecord read_rds[4];
    int cnt = 0;
    while(milvus::WAL_SUCCESS == buffer.Next((uint64_t)6 << 32, read_rds[cnt])) {
        cnt ++;
    }
    ASSERT_EQ(4, cnt);
    for (auto i = 0; i < cnt; ++ i) {
        ASSERT_EQ("insert_table", read_rds[i].table_id);
        ASSERT_EQ("parti1", read_rds[i].partition_tag);
    }
}

