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

#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <yaml-cpp/yaml.h>

#include "storage/MinioChunkManager.h"

using namespace std;
using namespace milvus;
using namespace milvus::storage;
using namespace boost::filesystem;

class MinioChunkManagerTest : public testing::Test {
 public:
    MinioChunkManagerTest() {
    }
    ~MinioChunkManagerTest() {
    }

    bool
    FindFile(const path& dir, const string& file_name, path& path_found) {
        const recursive_directory_iterator end;
        boost::system::error_code err;
        auto iter = recursive_directory_iterator(dir, err);
        while (iter != end) {
            try {
                if ((*iter).path().filename() == file_name) {
                    path_found = (*iter).path();
                    return true;
                }
                iter++;
            } catch (filesystem_error& e) {
            } catch (std::exception& e) {
                // ignore error
            }
        }
        return false;
    }

    string
    GetConfig() {
        char testPath[100];
        auto pwd = string(getcwd(testPath, sizeof(testPath)));
        path filepath;
        auto currentPath = path(pwd);
        while (!FindFile(currentPath, "milvus.yaml", filepath)) {
            currentPath = currentPath.append("../");
        }
        return filepath.string();
    }

    virtual void
    SetUp() {
        auto configPath = GetConfig();
        cout << configPath << endl;
        YAML::Node config;
        config = YAML::LoadFile(configPath);
        auto minioConfig = config["minio"];
        auto address = minioConfig["address"].as<string>();
        auto port = minioConfig["port"].as<string>();
        auto endpoint = address + ":" + port;
        auto accessKey = minioConfig["accessKeyID"].as<string>();
        auto accessValue = minioConfig["secretAccessKey"].as<string>();
        auto useSSL = minioConfig["useSSL"].as<bool>();
        auto bucketName = minioConfig["bucketName"].as<string>();

        ChunkMangerConfig::SetAddress(endpoint);
        ChunkMangerConfig::SetAccessKey(accessKey);
        ChunkMangerConfig::SetAccessValue(accessValue);
        ChunkMangerConfig::SetBucketName(bucketName);
        ChunkMangerConfig::SetUseSSL(useSSL);
    }
};

TEST_F(MinioChunkManagerTest, BucketPositive) {
    auto& chunk_manager = MinioChunkManager::GetInstance();
    string testBucketName = "test-bucket";
    chunk_manager.SetBucketName(testBucketName);
    chunk_manager.DeleteBucket(testBucketName);
    bool exist = chunk_manager.BucketExists(testBucketName);
    EXPECT_EQ(exist, false);
    chunk_manager.CreateBucket(testBucketName);
    exist = chunk_manager.BucketExists(testBucketName);
    EXPECT_EQ(exist, true);
}

TEST_F(MinioChunkManagerTest, BucketNegtive) {
    auto& chunk_manager = MinioChunkManager::GetInstance();
    string testBucketName = "test-bucket-ng";
    chunk_manager.SetBucketName(testBucketName);
    chunk_manager.DeleteBucket(testBucketName);

    // create already exist bucket
    chunk_manager.CreateBucket(testBucketName);
    try {
        chunk_manager.CreateBucket(testBucketName);
    } catch (S3ErrorException& e) {
        EXPECT_TRUE(std::string(e.what()).find("BucketAlreadyOwnedByYou") != string::npos);
    }
}

TEST_F(MinioChunkManagerTest, ObjectExist) {
    auto& chunk_manager = MinioChunkManager::GetInstance();
    string testBucketName = "test-objexist";
    string objPath = "1/3";
    chunk_manager.SetBucketName(testBucketName);
    if (!chunk_manager.BucketExists(testBucketName)) {
        chunk_manager.CreateBucket(testBucketName);
    }

    bool exist = chunk_manager.Exist(objPath);
    EXPECT_EQ(exist, false);
}

TEST_F(MinioChunkManagerTest, WritePositive) {
    auto& chunk_manager = MinioChunkManager::GetInstance();
    string testBucketName = "test-write";
    chunk_manager.SetBucketName(testBucketName);
    EXPECT_EQ(chunk_manager.GetBucketName(), testBucketName);

    if (!chunk_manager.BucketExists(testBucketName)) {
        chunk_manager.CreateBucket(testBucketName);
    }
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    string path = "1/3/5";
    chunk_manager.Write(path, data, sizeof(data));

    bool exist = chunk_manager.Exist(path);
    EXPECT_EQ(exist, true);

    auto size = chunk_manager.Size(path);
    EXPECT_EQ(size, 5);

    int datasize = 10000;
    uint8_t* bigdata = new uint8_t[datasize];
    srand((unsigned)time(NULL));
    for (int i = 0; i < datasize; ++i) {
        bigdata[i] = rand() % 256;
    }
    chunk_manager.Write(path, bigdata, datasize);
    size = chunk_manager.Size(path);
    EXPECT_EQ(size, datasize);
    delete[] bigdata;
}

TEST_F(MinioChunkManagerTest, ReadPositive) {
    auto& chunk_manager = MinioChunkManager::GetInstance();
    string testBucketName = "test-read";
    chunk_manager.SetBucketName(testBucketName);
    EXPECT_EQ(chunk_manager.GetBucketName(), testBucketName);

    if (!chunk_manager.BucketExists(testBucketName)) {
        chunk_manager.CreateBucket(testBucketName);
    }
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    string path = "1/4/6";
    chunk_manager.Write(path, data, sizeof(data));
    bool exist = chunk_manager.Exist(path);
    EXPECT_EQ(exist, true);
    auto size = chunk_manager.Size(path);
    EXPECT_EQ(size, 5);

    uint8_t readdata[20] = {0};
    size = chunk_manager.Read(path, readdata, 20);
    EXPECT_EQ(size, 5);
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x45);
    EXPECT_EQ(readdata[3], 0x34);
    EXPECT_EQ(readdata[4], 0x23);

    size = chunk_manager.Read(path, readdata, 3);
    EXPECT_EQ(size, 3);
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x45);

    uint8_t dataWithNULL[] = {0x17, 0x32, 0x00, 0x34, 0x23};
    chunk_manager.Write(path, dataWithNULL, sizeof(dataWithNULL));
    exist = chunk_manager.Exist(path);
    EXPECT_EQ(exist, true);
    size = chunk_manager.Size(path);
    EXPECT_EQ(size, 5);
    size = chunk_manager.Read(path, readdata, 20);
    EXPECT_EQ(size, 5);
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x00);
    EXPECT_EQ(readdata[3], 0x34);
    EXPECT_EQ(readdata[4], 0x23);
}

TEST_F(MinioChunkManagerTest, RemovePositive) {
    auto& chunk_manager = MinioChunkManager::GetInstance();
    string testBucketName = "test-remove";
    chunk_manager.SetBucketName(testBucketName);
    EXPECT_EQ(chunk_manager.GetBucketName(), testBucketName);

    if (!chunk_manager.BucketExists(testBucketName)) {
        chunk_manager.CreateBucket(testBucketName);
    }
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    string path = "1/7/8";
    chunk_manager.Write(path, data, sizeof(data));

    bool exist = chunk_manager.Exist(path);
    EXPECT_EQ(exist, true);

    chunk_manager.Remove(path);

    exist = chunk_manager.Exist(path);
    EXPECT_EQ(exist, false);
}

TEST_F(MinioChunkManagerTest, ListWithPrefixPositive) {
    auto& chunk_manager = MinioChunkManager::GetInstance();
    string testBucketName = "test-listprefix";
    chunk_manager.SetBucketName(testBucketName);
    EXPECT_EQ(chunk_manager.GetBucketName(), testBucketName);

    if (!chunk_manager.BucketExists(testBucketName)) {
        chunk_manager.CreateBucket(testBucketName);
    }

    string path1 = "1/7/8";
    string path2 = "1/7/4";
    string path3 = "1/4/8";
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    chunk_manager.Write(path1, data, sizeof(data));
    chunk_manager.Write(path2, data, sizeof(data));
    chunk_manager.Write(path3, data, sizeof(data));

    vector<string> objs = chunk_manager.ListWithPrefix("1/7");
    EXPECT_EQ(objs.size(), 2);
    std::sort(objs.begin(), objs.end());
    EXPECT_EQ(objs[0], "1/7/4");
    EXPECT_EQ(objs[1], "1/7/8");

    objs = chunk_manager.ListWithPrefix("//1/7");
    EXPECT_EQ(objs.size(), 2);

    objs = chunk_manager.ListWithPrefix("1");
    EXPECT_EQ(objs.size(), 3);
    std::sort(objs.begin(), objs.end());
    EXPECT_EQ(objs[0], "1/4/8");
    EXPECT_EQ(objs[1], "1/7/4");
}
