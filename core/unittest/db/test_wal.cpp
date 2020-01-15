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

#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <thread>


TEST_F(WalTest, FILE_HANDLER_TEST) {
    std::string file_path = "/tmp/";
    std::string file_name = "1";
    std::string open_mode = "r+";
    MXLogFileHandler file_handler(file_path);
    file_handler.SetFileName(file_name);
    file_handler.SetFileOpenMode(open_name);
    ASSERT_FALSE(file_handler.FileExists());
    ASSERT_FALSE(file_handler.IsOpen());

    file_handler.OpenFile();
    ASSERT_TRUE(file_handler.IsOpen());
    ASSERT(0, file_handler.GetFileSize());

    std::string write_content = "hello, world!\n";
    file_handler.Write(write_content.data(), write_content.size());
    char* buf = (char*)malloc(write_content.size() + 10);
    memset(buf, 0, write_content.size() + 10);
    file_handler.Load(buf, 0, write_content.size());
    ASSERT_EQ(sizeof(buf), write_content.size());
    free(buf);

    file_handler.ReBorn("2");
    write_content += ", aaaaa";
    file_handler.Write(write_content.data(), write_content.size());
    ASSERT_EQ("2", file_handler.GetFileName());
}

TEST_F(WalTest, BUFFER_TEST) {

}

TEST_F(WalTest, META_HANDLER_TEST) {
    MXLogMetaHandler meta_handler("/tmp/milvus/");
    uint64_t wal_lsn = 103920;
    meta_handler.SetMXLogInternalMeta(wal_lsn);
    uint64_t internal_lsn;
    meta_handler.GetMXLogInternalMeta(internal_lsn);
    ASSERT_EQ(wal_lsn, internal_lsn);
}

TEST_F(MetaTest, MANAGER_TEST) {

}

