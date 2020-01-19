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
#define private public
#include "db/wal/WalMetaHandler.h"
#include "db/wal/WalFileHandler.h"
#include "db/wal/WalManager.h"
#include "db/wal/WalBuffer.h"
#include "utils/Error.h"

#include <fstream>
#include <sstream>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <thread>

using namespace milvus::engine::wal;

TEST(WalTest, FILE_HANDLER_TEST) {
    std::string file_path = "/tmp/";
    std::string file_name = "1.wal";
    std::string open_mode = "w+";
    MXLogFileHandler file_handler(file_path);
    file_handler.SetFileName(file_name);
    file_handler.SetFileOpenMode(open_mode);
    ASSERT_FALSE(file_handler.FileExists());
    ASSERT_FALSE(file_handler.IsOpen());

    ASSERT_TRUE(file_handler.OpenFile());
    ASSERT_TRUE(file_handler.IsOpen());
    ASSERT_EQ(0, file_handler.GetFileSize());

    std::string write_content = "hello, world!\n";
    auto write_res = file_handler.Write(const_cast<char*>(write_content.data()), write_content.size());
    ASSERT_TRUE(write_res);
    char* buf = (char*)malloc(write_content.size() + 10);
    memset(buf, 0, write_content.size() + 10);
    auto load_res = file_handler.Load(buf, 0, write_content.size());
    ASSERT_TRUE(load_res);
    ASSERT_EQ(strlen(buf), write_content.size());
    free(buf);
    file_handler.DeleteFile();

    file_handler.ReBorn("2");
    write_content += ", aaaaa";
    file_handler.Write(const_cast<char*>(write_content.data()), write_content.size());
    ASSERT_EQ("2", file_handler.GetFileName());
    file_handler.DeleteFile();
}

TEST(WalTest, META_HANDLER_TEST) {
    MXLogMetaHandler meta_handler("/tmp/milvus/");
    uint64_t wal_lsn = 103920;
    meta_handler.SetMXLogInternalMeta(wal_lsn);
    uint64_t internal_lsn;
    meta_handler.GetMXLogInternalMeta(internal_lsn);
    ASSERT_EQ(wal_lsn, internal_lsn);
}

TEST(WalTest, BUFFER_TEST) {
    std::string log_path = "/tmp/milvus/wal/";
    uint32_t buf_size = 2 * 1024;
    MXLogBuffer buffer(log_path, buf_size);
    buffer.mxlog_buffer_size_ = 2 * 1024;
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

    MXLogRecord ins_vct_rd_1;
    ins_vct_rd_1.type = MXLogType::InsertVector;
    ins_vct_rd_1.table_id = "insert_table";
    ins_vct_rd_1.partition_tag = "parti1";
    ins_vct_rd_1.length = 1;
    milvus::engine::IDNumber id = 2;
    ins_vct_rd_1.ids = &id;
    ins_vct_rd_1.data_size = 2 * sizeof(float);
    float vecs[2] = {1.2, 3.4};
    ins_vct_rd_1.data = &vecs;
    ASSERT_EQ(buffer.Append(ins_vct_rd_1), milvus::WAL_SUCCESS);

    MXLogRecord del_rd;
    del_rd.type = MXLogType::Delete;
    del_rd.table_id = "insert_table";
    del_rd.partition_tag = "parti1";
    del_rd.length = 1;
    del_rd.ids = &id;
    del_rd.data_size = 0;
    del_rd.data = nullptr;
    ASSERT_EQ(buffer.Append(del_rd), milvus::WAL_SUCCESS);

    MXLogRecord ins_vct_rd_2;
    ins_vct_rd_2.type = MXLogType::InsertBinary;
    ins_vct_rd_2.table_id = "insert_table";
    ins_vct_rd_2.partition_tag = "parti1";
    ins_vct_rd_2.length = 100;
    ins_vct_rd_2.ids = (const milvus::engine::IDNumber*)
        malloc(ins_vct_rd_2.length * sizeof(milvus::engine::IDNumber));
    ins_vct_rd_2.data_size = 100 * sizeof(uint8_t);
    ins_vct_rd_2.data = malloc(ins_vct_rd_2.data_size);
    ASSERT_EQ(buffer.Append(ins_vct_rd_2), milvus::WAL_SUCCESS);

    MXLogRecord ins_vct_rd_3;
    ins_vct_rd_3.type = MXLogType::InsertVector;
    ins_vct_rd_3.table_id = "insert_table";
    ins_vct_rd_3.partition_tag = "parti1";
    ins_vct_rd_3.length = 100;
    ins_vct_rd_3.ids = (const milvus::engine::IDNumber*)
        malloc(ins_vct_rd_3.length * sizeof(milvus::engine::IDNumber));
    ins_vct_rd_3.data_size = 10 * 10 * sizeof(float);
    ins_vct_rd_3.data = malloc(ins_vct_rd_3.data_size);
    ASSERT_EQ(buffer.Append(ins_vct_rd_3), milvus::WAL_SUCCESS);

    MXLogRecord record;
    uint64_t last_lsn = (uint64_t)10 << 32;
    ASSERT_EQ(buffer.Next(last_lsn, record), milvus::WAL_SUCCESS);
    ASSERT_EQ(record.type, ins_vct_rd_1.type);
    ASSERT_EQ(record.table_id, ins_vct_rd_1.table_id);
    ASSERT_EQ(record.partition_tag, ins_vct_rd_1.partition_tag);
    ASSERT_EQ(record.length, ins_vct_rd_1.length);
    ASSERT_EQ(record.data_size, ins_vct_rd_1.data_size);

    ASSERT_EQ(buffer.Next(last_lsn, record), milvus::WAL_SUCCESS);
    ASSERT_EQ(record.type, del_rd.type);
    ASSERT_EQ(record.table_id, del_rd.table_id);
    ASSERT_EQ(record.partition_tag, del_rd.partition_tag);
    ASSERT_EQ(record.length, del_rd.length);
    ASSERT_EQ(record.data_size, del_rd.data_size);

    ASSERT_EQ(buffer.Next(last_lsn, record), milvus::WAL_SUCCESS);
    ASSERT_EQ(record.type, ins_vct_rd_2.type);
    ASSERT_EQ(record.table_id, ins_vct_rd_2.table_id);
    ASSERT_EQ(record.partition_tag, ins_vct_rd_2.partition_tag);
    ASSERT_EQ(record.length, ins_vct_rd_2.length);
    ASSERT_EQ(record.data_size, ins_vct_rd_2.data_size);


    ASSERT_EQ(buffer.Next(last_lsn, record), milvus::WAL_SUCCESS);
    ASSERT_EQ(record.type, ins_vct_rd_3.type);
    ASSERT_EQ(record.table_id, ins_vct_rd_3.table_id);
    ASSERT_EQ(record.partition_tag, ins_vct_rd_3.partition_tag);
    ASSERT_EQ(record.length, ins_vct_rd_3.length);
    ASSERT_EQ(record.data_size, ins_vct_rd_3.data_size);

    free((void*)ins_vct_rd_2.ids);
    free((void*)ins_vct_rd_2.data);
    free((void*)ins_vct_rd_3.ids);
    free((void*)ins_vct_rd_3.data);
}

TEST(WalTest, MANAGER_TEST) {
    MXLogConfiguration wal_config;
    wal_config.mxlog_path = "/tmp/milvus/wal/";
    wal_config.record_size = 2 * 1024 * 1024;
    wal_config.buffer_size = 32 * 1024 * 1024;
    wal_config.recovery_error_ignore = true;

    WalManager manager(wal_config);
    manager.Init(nullptr);
    std::string table_id = "manager_test";
    std::string partition_tag = "parti2";
    milvus::engine::IDNumbers vec_ids;
    std::vector<float> vecs;
    vec_ids.emplace_back(1);
    vec_ids.emplace_back(2);
    vecs.emplace_back(1.2);
    vecs.emplace_back(3.4);
    vecs.emplace_back(5.6);
    vecs.emplace_back(7.8);

    manager.CreateTable(table_id);
    auto ins_res = manager.Insert(table_id, partition_tag, vec_ids, vecs);
    ASSERT_TRUE(ins_res);
    milvus::engine::IDNumbers del_ids;
    del_ids.emplace_back(2);
    auto del_ret = manager.DeleteById(table_id, del_ids);
    ASSERT_TRUE(del_ret);
    manager.Flush(table_id);
}

TEST(WalTest, LargeScaleRecords) {
    std::string data_path = "/home/zilliz/workspace/data/";
    MXLogConfiguration wal_config;
    wal_config.mxlog_path = "/tmp/milvus/wal/";
    wal_config.record_size = 2 * 1024 * 1024;
    wal_config.buffer_size = 32 * 1024 * 1024;
    wal_config.recovery_error_ignore = true;

    WalManager manager1(wal_config);
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
                case 0: manager1.Flush();break;
                case 1: manager1.Insert("insert_vector", "parti1", ids, vecs); break;
                case 2: manager1.Insert("insert_binary", "parti2", ids, bins); break;
                case 3: manager1.DeleteById("insert_vector", ids); break;
                default: std::cout << "invalid type: " << type << std::endl; break;
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
            for (auto i = 0; i < 10; ++ i) {
                istr >> v;
                vecs.emplace_back(v);
            }
        } else if (cur_type == 2) {
            uint8_t b;
            for (auto i = 0; i < 20; ++ i) {
                istr >> b;
                bins.emplace_back(b);
            }
        }
    }
    switch (type) {
        case 0: manager1.Flush();break;
        case 1: manager1.Insert("insert_vector", "parti1", ids, vecs); break;
        case 2: manager1.Insert("insert_binary", "parti2", ids, bins); break;
        case 3: manager1.DeleteById("insert_vector", ids); break;
        default: std::cout << "invalid type: " << type << std::endl; break;
    }
    fin.close();
}

TEST(WalTest, MultiThreadTest) {

}
