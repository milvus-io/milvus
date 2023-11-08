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
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "storage/AzureChunkManager.h"
#include "storage/Util.h"

using namespace std;
using namespace milvus;
using namespace milvus::storage;

StorageConfig
get_default_storage_config() {
    auto endpoint = "core.windows.net";
    auto accessKey = "devstoreaccount1";
    auto accessValue =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
        "K1SZFPTOtr/KBHBeksoGMGw==";
    auto rootPath = "files";
    auto useSSL = false;
    auto useIam = false;
    auto iamEndPoint = "";
    auto bucketName = "a-bucket";

    return StorageConfig{endpoint,
                         bucketName,
                         accessKey,
                         accessValue,
                         rootPath,
                         "remote",
                         "azure",
                         iamEndPoint,
                         "error",
                         "",
                         useSSL,
                         useIam};
}

class AzureChunkManagerTest : public testing::Test {
 public:
    AzureChunkManagerTest() {
    }
    ~AzureChunkManagerTest() {
    }

    virtual void
    SetUp() {
        configs_ = get_default_storage_config();
        chunk_manager_ = make_unique<AzureChunkManager>(configs_);
        chunk_manager_ptr_ = CreateChunkManager(configs_);
    }

 protected:
    AzureChunkManagerPtr chunk_manager_;
    ChunkManagerPtr chunk_manager_ptr_;
    StorageConfig configs_;
};

TEST_F(AzureChunkManagerTest, AzureLogger) {
    AzureLogger(Azure::Core::Diagnostics::Logger::Level::Error, "");
    AzureLogger(Azure::Core::Diagnostics::Logger::Level::Warning, "");
    AzureLogger(Azure::Core::Diagnostics::Logger::Level::Informational, "");
    AzureLogger(Azure::Core::Diagnostics::Logger::Level::Verbose, "");
}

TEST_F(AzureChunkManagerTest, BasicFunctions) {
    EXPECT_TRUE(chunk_manager_->GetName() == "AzureChunkManager");
    EXPECT_TRUE(chunk_manager_ptr_->GetName() == "AzureChunkManager");
    EXPECT_TRUE(chunk_manager_->GetRootPath() == "files");

    string path = "test";
    uint8_t readdata[20] = {0};
    try {
        chunk_manager_->Read(path, 0, readdata, sizeof(readdata));
    } catch (SegcoreError& e) {
        EXPECT_TRUE(string(e.what()).find("Read") != string::npos);
    }
    try {
        chunk_manager_->Write(path, 0, readdata, sizeof(readdata));
    } catch (SegcoreError& e) {
        EXPECT_TRUE(string(e.what()).find("Write") != string::npos);
    }
}

TEST_F(AzureChunkManagerTest, BucketPositive) {
    string testBucketName = "test-bucket";
    bool exist = chunk_manager_->BucketExists(testBucketName);
    EXPECT_EQ(exist, false);
    chunk_manager_->CreateBucket(testBucketName);
    exist = chunk_manager_->BucketExists(testBucketName);
    EXPECT_EQ(exist, true);
    vector<string> buckets = chunk_manager_->ListBuckets();
    EXPECT_EQ(buckets[0], testBucketName);
    chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(AzureChunkManagerTest, BucketNegtive) {
    string testBucketName = "test-bucket-ng";
    try {
        chunk_manager_->DeleteBucket(testBucketName);
    } catch (SegcoreError& e) {
        EXPECT_TRUE(string(e.what()).find("not") != string::npos);
    }

    // create already exist bucket
    chunk_manager_->CreateBucket(testBucketName);
    try {
        chunk_manager_->CreateBucket(testBucketName);
    } catch (SegcoreError& e) {
        EXPECT_TRUE(string(e.what()).find("exists") != string::npos);
    }
    chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(AzureChunkManagerTest, ObjectExist) {
    string testBucketName = configs_.bucket_name;
    string objPath = "1/3";
    if (!chunk_manager_->BucketExists(testBucketName)) {
        chunk_manager_->CreateBucket(testBucketName);
    }

    bool exist = chunk_manager_->Exist(objPath);
    EXPECT_EQ(exist, false);
    chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(AzureChunkManagerTest, WritePositive) {
    string testBucketName = configs_.bucket_name;
    EXPECT_EQ(chunk_manager_->GetBucketName(), testBucketName);

    if (!chunk_manager_->BucketExists(testBucketName)) {
        chunk_manager_->CreateBucket(testBucketName);
    }
    auto has_bucket = chunk_manager_->BucketExists(testBucketName);
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    string path = "1";
    chunk_manager_->Write(path, data, sizeof(data));

    bool exist = chunk_manager_->Exist(path);
    EXPECT_EQ(exist, true);

    auto size = chunk_manager_->Size(path);
    EXPECT_EQ(size, 5);

    int datasize = 10000;
    uint8_t* bigdata = new uint8_t[datasize];
    srand((unsigned)time(NULL));
    for (int i = 0; i < datasize; ++i) {
        bigdata[i] = rand() % 256;
    }
    chunk_manager_->Write(path, bigdata, datasize);
    size = chunk_manager_->Size(path);
    EXPECT_EQ(size, datasize);
    delete[] bigdata;

    chunk_manager_->Remove(path);
    chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(AzureChunkManagerTest, ReadPositive) {
    string testBucketName = configs_.bucket_name;
    EXPECT_EQ(chunk_manager_->GetBucketName(), testBucketName);

    if (!chunk_manager_->BucketExists(testBucketName)) {
        chunk_manager_->CreateBucket(testBucketName);
    }
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    string path = "1/4/6";
    chunk_manager_->Write(path, data, sizeof(data));
    bool exist = chunk_manager_->Exist(path);
    EXPECT_EQ(exist, true);
    auto size = chunk_manager_->Size(path);
    EXPECT_EQ(size, sizeof(data));

    uint8_t readdata[20] = {0};
    size = chunk_manager_->Read(path, readdata, sizeof(data));
    EXPECT_EQ(size, sizeof(data));
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x45);
    EXPECT_EQ(readdata[3], 0x34);
    EXPECT_EQ(readdata[4], 0x23);

    size = chunk_manager_->Read(path, readdata, 3);
    EXPECT_EQ(size, 3);
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x45);

    uint8_t dataWithNULL[] = {0x17, 0x32, 0x00, 0x34, 0x23};
    chunk_manager_->Write(path, dataWithNULL, sizeof(dataWithNULL));
    exist = chunk_manager_->Exist(path);
    EXPECT_EQ(exist, true);
    size = chunk_manager_->Size(path);
    EXPECT_EQ(size, sizeof(dataWithNULL));
    size = chunk_manager_->Read(path, readdata, sizeof(dataWithNULL));
    EXPECT_EQ(size, sizeof(dataWithNULL));
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x00);
    EXPECT_EQ(readdata[3], 0x34);
    EXPECT_EQ(readdata[4], 0x23);

    chunk_manager_->Remove(path);

    try {
        chunk_manager_->Read(path, readdata, sizeof(dataWithNULL));
    } catch (SegcoreError& e) {
        EXPECT_TRUE(string(e.what()).find("exist") != string::npos);
    }

    chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(AzureChunkManagerTest, RemovePositive) {
    string testBucketName = configs_.bucket_name;
    EXPECT_EQ(chunk_manager_->GetBucketName(), testBucketName);

    if (!chunk_manager_->BucketExists(testBucketName)) {
        chunk_manager_->CreateBucket(testBucketName);
    }
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    string path = "1/7/8";
    chunk_manager_->Write(path, data, sizeof(data));

    bool exist = chunk_manager_->Exist(path);
    EXPECT_EQ(exist, true);

    chunk_manager_->Remove(path);

    exist = chunk_manager_->Exist(path);
    EXPECT_EQ(exist, false);

    try {
        chunk_manager_->Remove(path);
    } catch (SegcoreError& e) {
        EXPECT_TRUE(string(e.what()).find("not") != string::npos);
    }

    try {
        chunk_manager_->Size(path);
    } catch (SegcoreError& e) {
        EXPECT_TRUE(string(e.what()).find("not") != string::npos);
    }

    chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(AzureChunkManagerTest, ListWithPrefixPositive) {
    string testBucketName = configs_.bucket_name;
    EXPECT_EQ(chunk_manager_->GetBucketName(), testBucketName);

    if (!chunk_manager_->BucketExists(testBucketName)) {
        chunk_manager_->CreateBucket(testBucketName);
    }

    string path1 = "1/7/8";
    string path2 = "1/7/4";
    string path3 = "1/4/8";
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    chunk_manager_->Write(path1, data, sizeof(data));
    chunk_manager_->Write(path2, data, sizeof(data));
    chunk_manager_->Write(path3, data, sizeof(data));

    vector<string> objs = chunk_manager_->ListWithPrefix("1/7");
    EXPECT_EQ(objs.size(), 2);
    sort(objs.begin(), objs.end());
    EXPECT_EQ(objs[0], "1/7/4");
    EXPECT_EQ(objs[1], "1/7/8");

    objs = chunk_manager_->ListWithPrefix("//1/7");
    EXPECT_EQ(objs.size(), 0);

    objs = chunk_manager_->ListWithPrefix("1");
    EXPECT_EQ(objs.size(), 3);
    sort(objs.begin(), objs.end());
    EXPECT_EQ(objs[0], "1/4/8");
    EXPECT_EQ(objs[1], "1/7/4");

    chunk_manager_->Remove(path1);
    chunk_manager_->Remove(path2);
    chunk_manager_->Remove(path3);
    chunk_manager_->DeleteBucket(testBucketName);
}
