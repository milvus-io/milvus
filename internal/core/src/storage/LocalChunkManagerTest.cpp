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
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "storage/ChunkManager.h"
#include "storage/LocalChunkManager.h"
#include "test_utils/Constants.h"

using namespace std;
using namespace milvus;
using namespace milvus::storage;

class LocalChunkManagerTest : public testing::Test {};

namespace {

void
CreateEmptyFile(const std::string& path) {
    std::filesystem::create_directories(
        std::filesystem::path(path).parent_path());
    std::ofstream(path, std::ios::binary).close();
}

}  // namespace

TEST_F(LocalChunkManagerTest, DirPositive) {
    auto lcm = std::make_shared<LocalChunkManager>(TestLocalPath);
    string test_dir = lcm->GetRootPath() + "/local-test-dir/";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    bool exist = std::filesystem::exists(test_dir);
    EXPECT_EQ(exist, true);

    std::filesystem::remove_all(test_dir);
    exist = std::filesystem::exists(test_dir);
    EXPECT_EQ(exist, false);
}

TEST_F(LocalChunkManagerTest, FilePositive) {
    auto lcm = std::make_shared<LocalChunkManager>(TestLocalPath);
    string test_dir = lcm->GetRootPath() + "/local-test-dir";

    string file = test_dir + "/test-file";
    auto exist = lcm->Exist(file);
    EXPECT_EQ(exist, false);
    CreateEmptyFile(file);
    exist = lcm->Exist(file);
    EXPECT_EQ(exist, true);

    lcm->Remove(file);
    exist = lcm->Exist(file);
    EXPECT_EQ(exist, false);

    std::filesystem::remove_all(test_dir);
    exist = std::filesystem::exists(test_dir);
    EXPECT_EQ(exist, false);
}

TEST_F(LocalChunkManagerTest, WritePositive) {
    auto lcm = std::make_shared<LocalChunkManager>(TestLocalPath);
    string test_dir = lcm->GetRootPath() + "/local-test-dir";

    string file = test_dir + "/test-write-positive";
    auto exist = lcm->Exist(file);
    EXPECT_EQ(exist, false);
    CreateEmptyFile(file);

    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    lcm->Write(file, data, sizeof(data));

    exist = lcm->Exist(file);
    EXPECT_EQ(exist, true);
    auto size = lcm->Size(file);
    EXPECT_EQ(size, 5);

    int datasize = 10000;
    uint8_t* bigdata = new uint8_t[datasize];
    srand((unsigned)time(NULL));
    for (int i = 0; i < datasize; ++i) {
        bigdata[i] = rand() % 256;
    }
    lcm->Write(file, bigdata, datasize);
    size = lcm->Size(file);
    EXPECT_EQ(size, datasize);
    delete[] bigdata;

    std::filesystem::remove_all(test_dir);
    exist = std::filesystem::exists(test_dir);
    EXPECT_EQ(exist, false);
}

TEST_F(LocalChunkManagerTest, ReadPositive) {
    auto lcm = std::make_shared<LocalChunkManager>(TestLocalPath);
    string test_dir = lcm->GetRootPath() + "/local-test-dir";

    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    string path = test_dir + "/test-read-positive";
    CreateEmptyFile(path);
    lcm->Write(path, data, sizeof(data));
    bool exist = lcm->Exist(path);
    EXPECT_EQ(exist, true);
    auto size = lcm->Size(path);
    EXPECT_EQ(size, 5);

    uint8_t readdata[20] = {0};
    size = lcm->Read(path, readdata, 20);
    EXPECT_EQ(size, 5);
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x45);
    EXPECT_EQ(readdata[3], 0x34);
    EXPECT_EQ(readdata[4], 0x23);

    size = lcm->Read(path, readdata, 3);
    EXPECT_EQ(size, 3);
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x45);

    uint8_t dataWithNULL[] = {0x17, 0x32, 0x00, 0x34, 0x23};
    lcm->Write(path, dataWithNULL, sizeof(dataWithNULL));
    exist = lcm->Exist(path);
    EXPECT_EQ(exist, true);
    size = lcm->Size(path);
    EXPECT_EQ(size, 5);
    size = lcm->Read(path, readdata, 20);
    EXPECT_EQ(size, 5);
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x00);
    EXPECT_EQ(readdata[3], 0x34);
    EXPECT_EQ(readdata[4], 0x23);

    std::filesystem::remove_all(test_dir);
    exist = std::filesystem::exists(test_dir);
    EXPECT_EQ(exist, false);
}

TEST_F(LocalChunkManagerTest, WriteOffset) {
    auto lcm = std::make_shared<LocalChunkManager>(TestLocalPath);
    string test_dir = lcm->GetRootPath() + "/local-test-dir";

    string file = test_dir + "/test-write-offset";
    auto exist = lcm->Exist(file);
    EXPECT_EQ(exist, false);
    CreateEmptyFile(file);
    exist = lcm->Exist(file);
    EXPECT_EQ(exist, true);

    int offset = 0;
    uint8_t data[5] = {0x17, 0x32, 0x00, 0x34, 0x23};
    lcm->Write(file, offset, data, sizeof(data));

    exist = lcm->Exist(file);
    EXPECT_EQ(exist, true);
    auto size = lcm->Size(file);
    EXPECT_EQ(size, 5);

    offset = 5;
    lcm->Write(file, offset, data, sizeof(data));
    size = lcm->Size(file);
    EXPECT_EQ(size, 10);

    uint8_t read_data[20] = {0};
    size = lcm->Read(file, read_data, 20);
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

    std::filesystem::remove_all(test_dir);
    exist = std::filesystem::exists(test_dir);
    EXPECT_EQ(exist, false);
}

TEST_F(LocalChunkManagerTest, ReadOffset) {
    auto lcm = std::make_shared<LocalChunkManager>(TestLocalPath);
    string test_dir = lcm->GetRootPath() + "/local-test-dir";

    string file = test_dir + "/test-read-offset";
    CreateEmptyFile(file);
    auto exist = lcm->Exist(file);
    EXPECT_EQ(exist, true);

    uint8_t data[] = {0x17, 0x32, 0x00, 0x34, 0x23, 0x23, 0x87, 0x98};
    lcm->Write(file, data, sizeof(data));

    exist = lcm->Exist(file);
    EXPECT_EQ(exist, true);

    uint8_t read_data[20];
    auto size = lcm->Read(file, 0, read_data, 3);
    EXPECT_EQ(size, 3);
    EXPECT_EQ(read_data[0], 0x17);
    EXPECT_EQ(read_data[1], 0x32);
    EXPECT_EQ(read_data[2], 0x00);
    size = lcm->Read(file, 3, read_data, 4);
    EXPECT_EQ(size, 4);
    EXPECT_EQ(read_data[0], 0x34);
    EXPECT_EQ(read_data[1], 0x23);
    EXPECT_EQ(read_data[2], 0x23);
    EXPECT_EQ(read_data[3], 0x87);
    size = lcm->Read(file, 7, read_data, 4);
    EXPECT_EQ(size, 1);
    EXPECT_EQ(read_data[0], 0x98);

    std::filesystem::remove_all(test_dir);
    exist = std::filesystem::exists(test_dir);
    EXPECT_EQ(exist, false);
}

TEST_F(LocalChunkManagerTest, ListWithPrefix) {
    auto lcm = std::make_shared<LocalChunkManager>(TestLocalPath);
    auto test_dir = lcm->GetRootPath() + "/local-list-prefix";
    std::filesystem::remove_all(test_dir);

    uint8_t data[] = {0x17, 0x32, 0x00, 0x34, 0x23, 0x23, 0x87, 0x98};
    auto first = test_dir + "/alpha/one";
    auto second = test_dir + "/alpha/two";
    auto sibling = test_dir + "/alpha-extra/four";
    auto other = test_dir + "/beta/three";
    lcm->Write(first, data, sizeof(data));
    lcm->Write(second, data, sizeof(data));
    lcm->Write(sibling, data, sizeof(data));
    lcm->Write(other, data, sizeof(data));

    EXPECT_EQ(lcm->ListWithPrefix(test_dir + "/alpha"),
              (std::vector<std::string>{sibling, first, second}));
    EXPECT_EQ(lcm->ListWithPrefix("local-list-prefix/alpha"),
              (std::vector<std::string>{sibling, first, second}));
    EXPECT_EQ(lcm->ListWithPrefix(test_dir + "/alpha/o"),
              (std::vector<std::string>{first}));
    EXPECT_TRUE(lcm->ListWithPrefix(test_dir + "/missing").empty());

    std::filesystem::remove_all(test_dir);
}
