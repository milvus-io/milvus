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

#include <fiu-control.h>
#include <fiu/fiu-local.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <string>
#include <experimental/filesystem>

#include "db/utils.h"
#include "db/wal/WalManager.h"
#include "db/wal/WalFile.h"
#include "db/wal/WalOperationCodec.h"

TEST_F(WalTest, WalFileTest) {
    std::string path = "/tmp/milvus_wal/file_test";
    milvus::engine::idx_t last_id = 12345;

    {
        milvus::engine::WalFile file;
        ASSERT_FALSE(file.IsOpened());
        ASSERT_EQ(file.Size(), 0);

        int64_t k = 0;
        int64_t bytes = file.Write<int64_t>(&k);
        ASSERT_EQ(bytes, 0);

        bytes = file.Read<int64_t>(&k);
        ASSERT_EQ(bytes, 0);

        auto status = file.CloseFile();
        ASSERT_TRUE(status.ok());
    }

    {
        milvus::engine::WalFile file;
        auto status = file.OpenFile(path, milvus::engine::WalFile::APPEND_WRITE);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(file.IsOpened());

        int64_t max_size = milvus::engine::MAX_WAL_FILE_SIZE;
        ASSERT_FALSE(file.ExceedMaxSize(max_size));

        int64_t total_bytes = 0;
        int8_t len = path.size();
        int64_t bytes = file.Write<int8_t>(&len);
        ASSERT_EQ(bytes, sizeof(int8_t));
        total_bytes += bytes;

        ASSERT_TRUE(file.ExceedMaxSize(max_size));

        bytes = file.Write(path.data(), len);
        ASSERT_EQ(bytes, len);
        total_bytes += bytes;

        bytes = file.Write<milvus::engine::idx_t>(&last_id);
        ASSERT_EQ(bytes, sizeof(last_id));
        total_bytes += bytes;

        int64_t file_size = file.Size();
        ASSERT_EQ(total_bytes, file_size);

        std::string file_path = file.Path();
        ASSERT_EQ(file_path, path);

        file.Flush();
        file.CloseFile();
        ASSERT_FALSE(file.IsOpened());
    }

    {
        milvus::engine::WalFile file;
        auto status = file.OpenFile(path, milvus::engine::WalFile::READ);
        ASSERT_TRUE(status.ok());

        int8_t len = 0;
        int64_t bytes = file.Read<int8_t>(&len);
        ASSERT_EQ(bytes, sizeof(int8_t));

        std::string str;
        bytes = file.ReadStr(str, len);
        ASSERT_EQ(bytes, len);
        ASSERT_EQ(str, path);

        milvus::engine::idx_t id_read = 0;
        bytes = file.Read<int64_t>(&id_read);
        ASSERT_EQ(bytes, sizeof(id_read));
        ASSERT_EQ(id_read, last_id);

        milvus::engine::idx_t op_id = file.ReadLastOpId();
        ASSERT_EQ(op_id, last_id);
    }
}

TEST_F(WalTest, WalFileCodecTest) {
    std::string path = "/tmp/milvus_wal/file_test";

    {
        milvus::engine::WalFile file;
        auto status = file.OpenFile(path, milvus::engine::WalFile::APPEND_WRITE);
        ASSERT_TRUE(status.ok());
    }
}

TEST_F(WalTest, WalManagerTest) {

}