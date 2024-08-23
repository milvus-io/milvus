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
#include "storage/GcpNativeChunkManager.h"
#include "storage/Util.h"

using namespace std;
using namespace milvus;
using namespace milvus::storage;

StorageConfig
get_default_storage_config() {
    auto endpoint = "storage.gcs.127.0.0.1.nip.io:4443";
    auto access_key = "";
    auto access_value = "";
    auto root_path = "files";
    auto use_ssl = false;
    auto ssl_ca_cert = "";
    auto iam_endpoint = "";
    auto bucket_name = "sample-bucket";
    bool use_iam = false;
    bool gcp_native_without_auth = true;

    return StorageConfig{
        endpoint,
        bucket_name,
        access_key,
        access_value,
        root_path,
        "remote",     // storage_type
        "gcpnative",  // cloud_provider_type
        iam_endpoint,
        "error",  // log_level
        "",       // region
        use_ssl,
        ssl_ca_cert,
        use_iam,
        false,  // useVirtualHost
        1000,   // requestTimeoutMs
        gcp_native_without_auth,
        ""  // gcp_credential_json
    };
}

class GcpNativeChunkManagerTest : public testing::Test {
 public:
    GcpNativeChunkManagerTest() {
    }
    ~GcpNativeChunkManagerTest() {
    }

    virtual void
    SetUp() {
        configs_ = get_default_storage_config();
        chunk_manager_ = make_unique<GcpNativeChunkManager>(configs_);
    }

 protected:
    std::unique_ptr<GcpNativeChunkManager> chunk_manager_;
    StorageConfig configs_;
};

TEST_F(GcpNativeChunkManagerTest, BasicFunctions) {
    EXPECT_TRUE(chunk_manager_->GetName() == "GcpNativeChunkManager");
    EXPECT_TRUE(chunk_manager_->GetRootPath() == configs_.root_path);
    EXPECT_TRUE(chunk_manager_->GetBucketName() == configs_.bucket_name);
}

TEST_F(GcpNativeChunkManagerTest, BucketPositive) {
    string test_bucket_name = "bucket-not-exist";
    EXPECT_EQ(chunk_manager_->BucketExists(test_bucket_name), false);
    EXPECT_EQ(chunk_manager_->CreateBucket(test_bucket_name), true);
    EXPECT_EQ(chunk_manager_->BucketExists(test_bucket_name), true);
    vector<string> buckets = chunk_manager_->ListBuckets();
    EXPECT_TRUE(std::find(buckets.begin(), buckets.end(), test_bucket_name) !=
                buckets.end());
    EXPECT_EQ(chunk_manager_->DeleteBucket(test_bucket_name), true);
    EXPECT_EQ(chunk_manager_->BucketExists(test_bucket_name), false);
}

TEST_F(GcpNativeChunkManagerTest, BucketNegtive) {
    string test_bucket_name = configs_.bucket_name;
    EXPECT_EQ(chunk_manager_->DeleteBucket(test_bucket_name), false);
    EXPECT_EQ(chunk_manager_->CreateBucket(test_bucket_name), true);
    // Create an already existing bucket.
    EXPECT_EQ(chunk_manager_->CreateBucket(test_bucket_name), false);
    EXPECT_EQ(chunk_manager_->DeleteBucket(test_bucket_name), true);
}

TEST_F(GcpNativeChunkManagerTest, ObjectExist) {
    string test_bucket_name = configs_.bucket_name;
    string obj_path = "1/3";
    if (!chunk_manager_->BucketExists(test_bucket_name)) {
        EXPECT_EQ(chunk_manager_->CreateBucket(test_bucket_name), true);
    }
    EXPECT_EQ(chunk_manager_->Exist(obj_path), false);
    EXPECT_EQ(chunk_manager_->DeleteBucket(test_bucket_name), true);
}

TEST_F(GcpNativeChunkManagerTest, WritePositive) {
    string test_bucket_name = configs_.bucket_name;
    EXPECT_EQ(chunk_manager_->GetBucketName(), test_bucket_name);
    if (!chunk_manager_->BucketExists(test_bucket_name)) {
        EXPECT_EQ(chunk_manager_->CreateBucket(test_bucket_name), true);
    }
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    string path = "1";
    chunk_manager_->Write(path, data, sizeof(data));
    EXPECT_EQ(chunk_manager_->Exist(path), true);
    EXPECT_EQ(chunk_manager_->Size(path), 5);

    int datasize = 10000;
    uint8_t* bigdata = new uint8_t[datasize];
    srand((unsigned)time(NULL));
    for (int i = 0; i < datasize; ++i) {
        bigdata[i] = rand() % 256;
    }
    chunk_manager_->Write(path, bigdata, datasize);
    EXPECT_EQ(chunk_manager_->Size(path), datasize);
    delete[] bigdata;

    chunk_manager_->Remove(path);
    EXPECT_EQ(chunk_manager_->DeleteBucket(test_bucket_name), true);
}

TEST_F(GcpNativeChunkManagerTest, ReadPositive) {
    string test_bucket_name = configs_.bucket_name;
    chunk_manager_->SetBucketName(test_bucket_name);
    EXPECT_EQ(chunk_manager_->GetBucketName(), test_bucket_name);
    if (!chunk_manager_->BucketExists(test_bucket_name)) {
        EXPECT_EQ(chunk_manager_->CreateBucket(test_bucket_name), true);
    }

    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    string path = "1/4/6";
    chunk_manager_->Write(path, data, sizeof(data));
    EXPECT_EQ(chunk_manager_->Exist(path), true);
    EXPECT_EQ(chunk_manager_->Size(path), sizeof(data));

    uint8_t readdata[20] = {0};
    EXPECT_EQ(chunk_manager_->Read(path, readdata, sizeof(data)), sizeof(data));
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x45);
    EXPECT_EQ(readdata[3], 0x34);
    EXPECT_EQ(readdata[4], 0x23);

    EXPECT_EQ(chunk_manager_->Read(path, readdata, 3), 3);
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x45);

    uint8_t dataWithNULL[] = {0x17, 0x32, 0x00, 0x34, 0x23};
    chunk_manager_->Write(path, dataWithNULL, sizeof(dataWithNULL));
    EXPECT_EQ(chunk_manager_->Exist(path), true);
    EXPECT_EQ(chunk_manager_->Size(path), sizeof(dataWithNULL));
    EXPECT_EQ(chunk_manager_->Read(path, readdata, sizeof(dataWithNULL)),
              sizeof(dataWithNULL));
    EXPECT_EQ(readdata[0], 0x17);
    EXPECT_EQ(readdata[1], 0x32);
    EXPECT_EQ(readdata[2], 0x00);
    EXPECT_EQ(readdata[3], 0x34);
    EXPECT_EQ(readdata[4], 0x23);

    chunk_manager_->Remove(path);
    EXPECT_EQ(chunk_manager_->DeleteBucket(test_bucket_name), true);
}

TEST_F(GcpNativeChunkManagerTest, ReadNotExist) {
    string test_bucket_name = configs_.bucket_name;
    chunk_manager_->SetBucketName(test_bucket_name);
    EXPECT_EQ(chunk_manager_->GetBucketName(), test_bucket_name);

    if (!chunk_manager_->BucketExists(test_bucket_name)) {
        EXPECT_EQ(chunk_manager_->CreateBucket(test_bucket_name), true);
    }
    string path = "1/5/8";
    uint8_t readdata[20] = {0};
    EXPECT_THROW(
        try {
            chunk_manager_->Read(path, readdata, sizeof(readdata));
        } catch (SegcoreError& e) {
            EXPECT_TRUE(std::string(e.what()).find("Not Found") !=
                        string::npos);
            throw e;
        },
        SegcoreError);

    EXPECT_EQ(chunk_manager_->DeleteBucket(test_bucket_name), true);
}

TEST_F(GcpNativeChunkManagerTest, RemovePositive) {
    string test_bucket_name = configs_.bucket_name;
    chunk_manager_->SetBucketName(test_bucket_name);
    EXPECT_EQ(chunk_manager_->GetBucketName(), test_bucket_name);

    if (!chunk_manager_->BucketExists(test_bucket_name)) {
        EXPECT_EQ(chunk_manager_->CreateBucket(test_bucket_name), true);
    }
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    string path = "1/7/8";
    chunk_manager_->Write(path, data, sizeof(data));

    EXPECT_EQ(chunk_manager_->Exist(path), true);
    chunk_manager_->Remove(path);
    EXPECT_EQ(chunk_manager_->Exist(path), false);

    // test double deleted
    chunk_manager_->Remove(path);
    EXPECT_EQ(chunk_manager_->Exist(path), false);

    EXPECT_EQ(chunk_manager_->DeleteBucket(test_bucket_name), true);
}

TEST_F(GcpNativeChunkManagerTest, ListWithPrefixPositive) {
    string test_bucket_name = configs_.bucket_name;
    chunk_manager_->SetBucketName(test_bucket_name);
    EXPECT_EQ(chunk_manager_->GetBucketName(), test_bucket_name);

    if (!chunk_manager_->BucketExists(test_bucket_name)) {
        EXPECT_EQ(chunk_manager_->CreateBucket(test_bucket_name), true);
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
    std::sort(objs.begin(), objs.end());
    EXPECT_EQ(objs[0], "1/7/4");
    EXPECT_EQ(objs[1], "1/7/8");

    objs = chunk_manager_->ListWithPrefix("//1/7");
    EXPECT_EQ(objs.size(), 0);

    objs = chunk_manager_->ListWithPrefix("1");
    EXPECT_EQ(objs.size(), 3);
    std::sort(objs.begin(), objs.end());
    EXPECT_EQ(objs[0], "1/4/8");
    EXPECT_EQ(objs[1], "1/7/4");
    EXPECT_EQ(objs[2], "1/7/8");

    chunk_manager_->Remove(path1);
    chunk_manager_->Remove(path2);
    chunk_manager_->Remove(path3);
    EXPECT_EQ(chunk_manager_->DeleteBucket(test_bucket_name), true);
}

TEST_F(GcpNativeChunkManagerTest, ListWithPrefixNegative) {
    string test_bucket_name = configs_.bucket_name;
    chunk_manager_->SetBucketName(test_bucket_name);
    EXPECT_EQ(chunk_manager_->GetBucketName(), test_bucket_name);

    if (!chunk_manager_->BucketExists(test_bucket_name)) {
        EXPECT_EQ(chunk_manager_->CreateBucket(test_bucket_name), true);
    }

    string path = "1/4/8";
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    chunk_manager_->Write(path, data, sizeof(data));
    vector<string> objs = chunk_manager_->ListWithPrefix("1/6");
    EXPECT_EQ(objs.size(), 0);
    chunk_manager_->Remove(path);
    EXPECT_EQ(chunk_manager_->DeleteBucket(test_bucket_name), true);
}
