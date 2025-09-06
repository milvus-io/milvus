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

#include "storage/MinioChunkManager.h"
#include "storage/Util.h"

using namespace std;
using namespace milvus;
using namespace milvus::storage;

const string
get_default_bucket_name() {
    return "a-bucket";
}

StorageConfig
get_default_remote_storage_config() {
    StorageConfig storage_config;
    storage_config.storage_type = "remote";
    storage_config.address = "localhost:9000";
    char const* tmp = getenv("MINIO_ADDRESS");
    if (tmp != NULL) {
        storage_config.address = string(tmp);
    }
    storage_config.bucket_name = get_default_bucket_name();
    storage_config.access_key_id = "minioadmin";
    storage_config.access_key_value = "minioadmin";
    storage_config.root_path = "files";
    storage_config.storage_type = "remote";
    storage_config.cloud_provider = "";
    storage_config.useSSL = false;
    storage_config.sslCACert = "";
    storage_config.useIAM = false;
    return storage_config;
}

class RemoteChunkManagerTest : public testing::Test {
 public:
    RemoteChunkManagerTest() {
    }
    ~RemoteChunkManagerTest() {
    }

    virtual void
    SetUp() {
        configs_ = get_default_remote_storage_config();
        aws_chunk_manager_ = make_unique<AwsChunkManager>(configs_);
        chunk_manager_ptr_ = CreateChunkManager(configs_);
    }

 protected:
    std::unique_ptr<AwsChunkManager> aws_chunk_manager_;
    ChunkManagerPtr chunk_manager_ptr_;
    StorageConfig configs_;
};

TEST_F(RemoteChunkManagerTest, BasicFunctions) {
    EXPECT_TRUE(aws_chunk_manager_->GetName() == "AwsChunkManager");
    EXPECT_TRUE(chunk_manager_ptr_->GetName() == "MinioChunkManager");

    ChunkManagerPtr the_chunk_manager_;
    configs_.cloud_provider = "aws";
    the_chunk_manager_ = CreateChunkManager(configs_);
    EXPECT_TRUE(the_chunk_manager_->GetName() == "AwsChunkManager");

    configs_.cloud_provider = "gcp";
    the_chunk_manager_ = CreateChunkManager(configs_);
    EXPECT_TRUE(the_chunk_manager_->GetName() == "GcpChunkManager");

    configs_.cloud_provider = "aliyun";
    the_chunk_manager_ = CreateChunkManager(configs_);
    EXPECT_TRUE(the_chunk_manager_->GetName() == "AliyunChunkManager");

#ifdef AZURE_BUILD_DIR
    configs_.cloud_provider = "azure";
    the_chunk_manager_ = CreateChunkManager(configs_);
    EXPECT_TRUE(the_chunk_manager_->GetName() == "AzureChunkManager");
#endif

#ifdef ENABLE_GCP_NATIVE
    configs_.cloud_provider = "gcpnative";
    the_chunk_manager_ = CreateChunkManager(configs_);
    EXPECT_TRUE(the_chunk_manager_->GetName() == "GcpNativeChunkManager");
#endif

    configs_.cloud_provider = "";
}

TEST_F(RemoteChunkManagerTest, BucketPositive) {
    string testBucketName = get_default_bucket_name();
    aws_chunk_manager_->SetBucketName(testBucketName);
    bool exist = aws_chunk_manager_->BucketExists(testBucketName);
    EXPECT_EQ(exist, false);
    aws_chunk_manager_->CreateBucket(testBucketName);
    exist = aws_chunk_manager_->BucketExists(testBucketName);
    EXPECT_EQ(exist, true);
    aws_chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(RemoteChunkManagerTest, BucketNegtive) {
    string testBucketName = get_default_bucket_name();
    aws_chunk_manager_->SetBucketName(testBucketName);
    aws_chunk_manager_->DeleteBucket(testBucketName);

    // create already exist bucket
    aws_chunk_manager_->CreateBucket(testBucketName);
    try {
        aws_chunk_manager_->CreateBucket(testBucketName);
    } catch (SegcoreError& e) {
        EXPECT_TRUE(std::string(e.what()).find("exists") != string::npos);
    }
    aws_chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(RemoteChunkManagerTest, ObjectExist) {
    string testBucketName = get_default_bucket_name();
    string objPath = "1/3";
    aws_chunk_manager_->SetBucketName(testBucketName);
    if (!aws_chunk_manager_->BucketExists(testBucketName)) {
        aws_chunk_manager_->CreateBucket(testBucketName);
    }

    bool exist = aws_chunk_manager_->Exist(objPath);
    EXPECT_EQ(exist, false);
    exist = chunk_manager_ptr_->Exist(objPath);
    EXPECT_EQ(exist, false);
    aws_chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(RemoteChunkManagerTest, WritePositive) {
    string testBucketName = get_default_bucket_name();
    aws_chunk_manager_->SetBucketName(testBucketName);
    EXPECT_EQ(aws_chunk_manager_->GetBucketName(), testBucketName);

    if (!aws_chunk_manager_->BucketExists(testBucketName)) {
        aws_chunk_manager_->CreateBucket(testBucketName);
    }
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    string path = "1";
    aws_chunk_manager_->Write(path, data, sizeof(data));

    bool exist = aws_chunk_manager_->Exist(path);
    EXPECT_EQ(exist, true);

    auto size = aws_chunk_manager_->Size(path);
    EXPECT_EQ(size, 5);

    int datasize = 10000;
    uint8_t* bigdata = new uint8_t[datasize];
    srand((unsigned)time(NULL));
    for (int i = 0; i < datasize; ++i) {
        bigdata[i] = rand() % 256;
    }
    aws_chunk_manager_->Write(path, bigdata, datasize);
    size = aws_chunk_manager_->Size(path);
    EXPECT_EQ(size, datasize);
    delete[] bigdata;

    aws_chunk_manager_->Remove(path);
    aws_chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(RemoteChunkManagerTest, ReadPositive) {
    string testBucketName = get_default_bucket_name();
    aws_chunk_manager_->SetBucketName(testBucketName);
    EXPECT_EQ(aws_chunk_manager_->GetBucketName(), testBucketName);

    if (!aws_chunk_manager_->BucketExists(testBucketName)) {
        aws_chunk_manager_->CreateBucket(testBucketName);
    }
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    string path = "1/4/6";
    aws_chunk_manager_->Write(path, data, sizeof(data));
    bool exist = aws_chunk_manager_->Exist(path);
    EXPECT_EQ(exist, true);
    auto size = aws_chunk_manager_->Size(path);
    EXPECT_EQ(size, sizeof(data));

    uint8_t readdata[20] = {0};
    size = aws_chunk_manager_->Read(path, readdata, sizeof(data));
    EXPECT_EQ(size, sizeof(data));
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x45);
    EXPECT_EQ(readdata[3], 0x34);
    EXPECT_EQ(readdata[4], 0x23);

    size = aws_chunk_manager_->Read(path, readdata, 3);
    EXPECT_EQ(size, 3);
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x45);

    uint8_t dataWithNULL[] = {0x17, 0x32, 0x00, 0x34, 0x23};
    aws_chunk_manager_->Write(path, dataWithNULL, sizeof(dataWithNULL));
    exist = aws_chunk_manager_->Exist(path);
    EXPECT_EQ(exist, true);
    size = aws_chunk_manager_->Size(path);
    EXPECT_EQ(size, sizeof(dataWithNULL));
    size = aws_chunk_manager_->Read(path, readdata, sizeof(dataWithNULL));
    EXPECT_EQ(size, sizeof(dataWithNULL));
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x00);
    EXPECT_EQ(readdata[3], 0x34);
    EXPECT_EQ(readdata[4], 0x23);

    aws_chunk_manager_->Remove(path);
    aws_chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(RemoteChunkManagerTest, RemovePositive) {
    string testBucketName = get_default_bucket_name();
    aws_chunk_manager_->SetBucketName(testBucketName);
    EXPECT_EQ(aws_chunk_manager_->GetBucketName(), testBucketName);

    if (!aws_chunk_manager_->BucketExists(testBucketName)) {
        aws_chunk_manager_->CreateBucket(testBucketName);
    }
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    string path = "1/7/8";
    aws_chunk_manager_->Write(path, data, sizeof(data));

    bool exist = aws_chunk_manager_->Exist(path);
    EXPECT_EQ(exist, true);

    aws_chunk_manager_->Remove(path);

    exist = aws_chunk_manager_->Exist(path);
    EXPECT_EQ(exist, false);

    aws_chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(RemoteChunkManagerTest, ListWithPrefixPositive) {
    string testBucketName = get_default_bucket_name();
    aws_chunk_manager_->SetBucketName(testBucketName);
    EXPECT_EQ(aws_chunk_manager_->GetBucketName(), testBucketName);

    if (!aws_chunk_manager_->BucketExists(testBucketName)) {
        aws_chunk_manager_->CreateBucket(testBucketName);
    }

    string path1 = "1/7/8";
    string path2 = "1/7/4";
    string path3 = "1/4/8";
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    aws_chunk_manager_->Write(path1, data, sizeof(data));
    aws_chunk_manager_->Write(path2, data, sizeof(data));
    aws_chunk_manager_->Write(path3, data, sizeof(data));

    vector<string> objs = aws_chunk_manager_->ListWithPrefix("1/7");
    EXPECT_EQ(objs.size(), 2);
    std::sort(objs.begin(), objs.end());
    EXPECT_EQ(objs[0], "1/7/4");
    EXPECT_EQ(objs[1], "1/7/8");

    objs = aws_chunk_manager_->ListWithPrefix("//1/7");
    EXPECT_EQ(objs.size(), 2);

    objs = aws_chunk_manager_->ListWithPrefix("1");
    EXPECT_EQ(objs.size(), 3);
    std::sort(objs.begin(), objs.end());
    EXPECT_EQ(objs[0], "1/4/8");
    EXPECT_EQ(objs[1], "1/7/4");

    aws_chunk_manager_->Remove(path1);
    aws_chunk_manager_->Remove(path2);
    aws_chunk_manager_->Remove(path3);
    aws_chunk_manager_->DeleteBucket(testBucketName);
}
