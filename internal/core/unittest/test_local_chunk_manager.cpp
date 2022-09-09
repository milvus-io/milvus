// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>

#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "storage/LocalChunkManager.h"

using namespace std;
using namespace milvus;
using namespace milvus::storage;

class LocalChunkManagerTest : public testing::Test {
 public:
    LocalChunkManagerTest() {
    }
    ~LocalChunkManagerTest() {
    }

    virtual void
    SetUp() {
        ChunkMangerConfig::SetLocalBucketName("/tmp/local-test-dir");
    }
};

TEST_F(LocalChunkManagerTest, DirPositive) {
    auto& lcm = LocalChunkManager::GetInstance();
    string path_prefix = lcm.GetPathPrefix();
    lcm.RemoveDir(path_prefix);
    lcm.CreateDir(path_prefix);

    bool exist = lcm.DirExist(path_prefix);
    EXPECT_EQ(exist, true);

    lcm.RemoveDir(path_prefix);
    exist = lcm.DirExist(path_prefix);
    EXPECT_EQ(exist, false);
}

TEST_F(LocalChunkManagerTest, FilePositive) {
    auto& lcm = LocalChunkManager::GetInstance();
    string path_prefix = lcm.GetPathPrefix();

    string file = "/tmp/local-test-dir/test-file";
    auto exist = lcm.Exist(file);
    EXPECT_EQ(exist, false);
    lcm.CreateFile(file);
    exist = lcm.Exist(file);
    EXPECT_EQ(exist, true);

    lcm.Remove(file);
    exist = lcm.Exist(file);
    EXPECT_EQ(exist, false);

    lcm.RemoveDir(path_prefix);
    exist = lcm.DirExist(path_prefix);
    EXPECT_EQ(exist, false);
}

TEST_F(LocalChunkManagerTest, WritePositive) {
    auto& lcm = LocalChunkManager::GetInstance();
    string path_prefix = lcm.GetPathPrefix();

    string file = "/tmp/local-test-dir/test-write-positive";
    auto exist = lcm.Exist(file);
    EXPECT_EQ(exist, false);
    lcm.CreateFile(file);

    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    lcm.Write(file, data, sizeof(data));

    exist = lcm.Exist(file);
    EXPECT_EQ(exist, true);
    auto size = lcm.Size(file);
    EXPECT_EQ(size, 5);

    int datasize = 10000;
    uint8_t* bigdata = new uint8_t[datasize];
    srand((unsigned)time(NULL));
    for (int i = 0; i < datasize; ++i) {
        bigdata[i] = rand() % 256;
    }
    lcm.Write(file, bigdata, datasize);
    size = lcm.Size(file);
    EXPECT_EQ(size, datasize);
    delete[] bigdata;

    lcm.RemoveDir(path_prefix);
    exist = lcm.DirExist(path_prefix);
    EXPECT_EQ(exist, false);
}

TEST_F(LocalChunkManagerTest, ReadPositive) {
    auto& lcm = LocalChunkManager::GetInstance();
    string path_prefix = lcm.GetPathPrefix();

    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    string path = "/tmp/local-test-dir/test-read-positive";
    lcm.CreateFile(path);
    lcm.Write(path, data, sizeof(data));
    bool exist = lcm.Exist(path);
    EXPECT_EQ(exist, true);
    auto size = lcm.Size(path);
    EXPECT_EQ(size, 5);

    uint8_t readdata[20] = {0};
    size = lcm.Read(path, readdata, 20);
    EXPECT_EQ(size, 5);
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x45);
    EXPECT_EQ(readdata[3], 0x34);
    EXPECT_EQ(readdata[4], 0x23);

    size = lcm.Read(path, readdata, 3);
    EXPECT_EQ(size, 3);
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x45);

    uint8_t dataWithNULL[] = {0x17, 0x32, 0x00, 0x34, 0x23};
    lcm.Write(path, dataWithNULL, sizeof(dataWithNULL));
    exist = lcm.Exist(path);
    EXPECT_EQ(exist, true);
    size = lcm.Size(path);
    EXPECT_EQ(size, 5);
    size = lcm.Read(path, readdata, 20);
    EXPECT_EQ(size, 5);
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x00);
    EXPECT_EQ(readdata[3], 0x34);
    EXPECT_EQ(readdata[4], 0x23);

    lcm.RemoveDir(path_prefix);
    exist = lcm.DirExist(path_prefix);
    EXPECT_EQ(exist, false);
}

TEST_F(LocalChunkManagerTest, WriteOffset) {
    auto& lcm = LocalChunkManager::GetInstance();
    string path_prefix = lcm.GetPathPrefix();

    string file = "/tmp/local-test-dir/test-write-offset";
    auto exist = lcm.Exist(file);
    EXPECT_EQ(exist, false);
    lcm.CreateFile(file);
    exist = lcm.Exist(file);
    EXPECT_EQ(exist, true);

    int offset = 0;
    uint8_t data[5] = {0x17, 0x32, 0x00, 0x34, 0x23};
    lcm.Write(file, offset, data, sizeof(data));

    exist = lcm.Exist(file);
    EXPECT_EQ(exist, true);
    auto size = lcm.Size(file);
    EXPECT_EQ(size, 5);

    offset = 5;
    lcm.Write(file, offset, data, sizeof(data));
    size = lcm.Size(file);
    EXPECT_EQ(size, 10);

    uint8_t read_data[20] = {0};
    size = lcm.Read(file, read_data, 20);
    EXPECT_EQ(size, 10);
    EXPECT_EQ(read_data[0], 0x17);
    EXPECT_EQ(read_data[1], 0x32);
    EXPECT_EQ(read_data[2], 0x00);
    EXPECT_EQ(read_data[3], 0x34);
    EXPECT_EQ(read_data[4], 0x23);
    EXPECT_EQ(read_data[5], 0x17);
    EXPECT_EQ(read_data[6], 0x32);
    EXPECT_EQ(read_data[7], 0x00);
    EXPECT_EQ(read_data[8], 0x34);
    EXPECT_EQ(read_data[9], 0x23);

    lcm.RemoveDir(path_prefix);
    exist = lcm.DirExist(path_prefix);
    EXPECT_EQ(exist, false);
}

TEST_F(LocalChunkManagerTest, ReadOffset) {
    auto& lcm = LocalChunkManager::GetInstance();
    string path_prefix = lcm.GetPathPrefix();

    string file = "/tmp/local-test-dir/test-read-offset";
    lcm.CreateFile(file);
    auto exist = lcm.Exist(file);
    EXPECT_EQ(exist, true);

    uint8_t data[] = {0x17, 0x32, 0x00, 0x34, 0x23, 0x23, 0x87, 0x98};
    lcm.Write(file, data, sizeof(data));

    exist = lcm.Exist(file);
    EXPECT_EQ(exist, true);

    uint8_t read_data[20];
    auto size = lcm.Read(file, 0, read_data, 3);
    EXPECT_EQ(size, 3);
    EXPECT_EQ(read_data[0], 0x17);
    EXPECT_EQ(read_data[1], 0x32);
    EXPECT_EQ(read_data[2], 0x00);
    size = lcm.Read(file, 3, read_data, 4);
    EXPECT_EQ(size, 4);
    EXPECT_EQ(read_data[0], 0x34);
    EXPECT_EQ(read_data[1], 0x23);
    EXPECT_EQ(read_data[2], 0x23);
    EXPECT_EQ(read_data[3], 0x87);
    size = lcm.Read(file, 7, read_data, 4);
    EXPECT_EQ(size, 1);
    EXPECT_EQ(read_data[0], 0x98);

    lcm.RemoveDir(path_prefix);
    exist = lcm.DirExist(path_prefix);
    EXPECT_EQ(exist, false);
}
