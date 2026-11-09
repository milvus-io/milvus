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

#include "storage/minio/MinioChunkManager.h"
#include "test_utils/indexbuilder_test_utils.h"

using namespace std;
using namespace milvus;
using namespace milvus::storage;

// These formatting tests do not create a client or require a storage service.
TEST(S3ErrorMessageTest, SDKRequestId) {
    Aws::S3::S3Error error(Aws::Client::AWSError<Aws::S3::S3Errors>(
        Aws::S3::S3Errors::ACCESS_DENIED, false));
    error.SetResponseCode(Aws::Http::HttpResponseCode::FORBIDDEN);
    error.SetExceptionName("AccessDenied");
    error.SetMessage("No response body.");
    error.SetRequestId("sdk-request-id");
    error.SetResponseHeaders({{"x-cos-request-id", "header-request-id"}});

    EXPECT_EQ(
        S3ErrorMessage("GetObjectSize", error, "bucket={}", "test-bucket"),
        "Error in GetObjectSize[errcode:403, exception:AccessDenied, "
        "errmessage:No response body., request_id:sdk-request-id, "
        "params:bucket=test-bucket]");
}

TEST(S3ErrorMessageTest, RequestIdFromResponseHeaders) {
    for (const auto& header : {"x-amz-request-id",
                               "x-cos-request-id",
                               "x-oss-request-id",
                               "x-obs-request-id",
                               "X-Cos-Request-Id"}) {
        SCOPED_TRACE(header);
        Aws::S3::S3Error error(Aws::Client::AWSError<Aws::S3::S3Errors>(
            Aws::S3::S3Errors::ACCESS_DENIED, false));
        error.SetResponseCode(Aws::Http::HttpResponseCode::FORBIDDEN);
        error.SetMessage("No response body.");
        error.SetResponseHeaders({{header, "server-request-id"},
                                  {"authorization", "must-not-be-logged"}});
        const auto message = S3ErrorMessage("GetObjectSize", error, "params");
        EXPECT_NE(message.find("request_id:server-request-id"),
                  std::string::npos);
        EXPECT_EQ(message.find("must-not-be-logged"), std::string::npos);
    }
}

TEST(S3ErrorMessageTest, MissingRequestId) {
    Aws::S3::S3Error error(Aws::Client::AWSError<Aws::S3::S3Errors>(
        Aws::S3::S3Errors::ACCESS_DENIED, false));
    error.SetResponseCode(Aws::Http::HttpResponseCode::FORBIDDEN);
    error.SetMessage("No response body.");
    error.SetResponseHeaders(
        {{"x-cos-request-id", ""}, {"content-type", "application/xml"}});
    EXPECT_EQ(S3ErrorMessage("GetObjectSize", error, "params"),
              "Error in GetObjectSize[errcode:403, exception:, "
              "errmessage:No response body., params:params]");
}

TEST(S3ErrorMessageTest, ClientErrorWithoutResponse) {
    Aws::S3::S3Error error(Aws::Client::AWSError<Aws::S3::S3Errors>(
        Aws::S3::S3Errors::NETWORK_CONNECTION, true));
    error.SetMessage("Connection timed out");
    const auto message = S3ErrorMessage("GetObjectBuffer", error, "params");
    EXPECT_NE(message.find("Connection timed out"), std::string::npos);
    EXPECT_EQ(message.find("request_id:"), std::string::npos);
}

class MinioChunkManagerTest : public testing::Test {
 public:
    MinioChunkManagerTest() {
    }
    ~MinioChunkManagerTest() {
    }

    virtual void
    SetUp() {
        configs_ = StorageConfig{};
        chunk_manager_ = std::make_unique<MinioChunkManager>(configs_);
    }

 protected:
    MinioChunkManagerPtr chunk_manager_;
    StorageConfig configs_;
};

//StorageConfig
//get_aliyun_cloud_storage_config() {
//    auto endpoint = "oss-cn-shanghai.aliyuncs.com:443";
//    auto accessKey = "";
//    auto accessValue = "";
//    auto rootPath = "files";
//    auto useSSL = false;
//    auto sslCACert = "";
//    auto useIam = true;
//    auto iamEndPoint = "";
//    auto bucketName = "vdc-infra-poc";
//    auto cloudProvider = "aliyun";
//    auto logLevel = "error";
//    auto region = "";
//
//    return StorageConfig{endpoint,
//                         bucketName,
//                         accessKey,
//                         accessValue,
//                         rootPath,
//                         "minio",
//                         cloudProvider,
//                         iamEndPoint,
//                         logLevel,
//                         region,
//                         useSSL,
//                         sslCACert,
//                         useIam};
//}

//class AliyunChunkManagerTest : public testing::Test {
// public:
//    AliyunChunkManagerTest() {
//    }
//    ~AliyunChunkManagerTest() {
//    }
//
//    virtual void
//    SetUp() {
//        chunk_manager_ = std::make_unique<MinioChunkManager>(
//            get_aliyun_cloud_storage_config());
//    }
//
// protected:
//    MinioChunkManagerPtr chunk_manager_;
//};

TEST_F(MinioChunkManagerTest, InitFailed) {
    auto configs = StorageConfig{};
    // wrong address
    configs.address = "1.2.3.4:9000";
    EXPECT_THROW(std::make_unique<MinioChunkManager>(configs), SegcoreError);
}

TEST_F(MinioChunkManagerTest, BucketPositive) {
    string testBucketName = "test-bucket";
    chunk_manager_->SetBucketName(testBucketName);
    bool exist = chunk_manager_->BucketExists(testBucketName);
    EXPECT_EQ(exist, false);
    chunk_manager_->CreateBucket(testBucketName);
    exist = chunk_manager_->BucketExists(testBucketName);
    EXPECT_EQ(exist, true);
    chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(MinioChunkManagerTest, BucketNegtive) {
    string testBucketName = "test-bucket-ng";
    chunk_manager_->SetBucketName(testBucketName);
    chunk_manager_->DeleteBucket(testBucketName);

    // create already exist bucket
    chunk_manager_->CreateBucket(testBucketName);
    bool created = chunk_manager_->CreateBucket(testBucketName);
    EXPECT_EQ(created, false);
    chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(MinioChunkManagerTest, ObjectExist) {
    string testBucketName = configs_.bucket_name;
    string objPath = "1/3";
    chunk_manager_->SetBucketName(testBucketName);
    if (!chunk_manager_->BucketExists(testBucketName)) {
        chunk_manager_->CreateBucket(testBucketName);
    }

    bool exist = chunk_manager_->Exist(objPath);
    EXPECT_EQ(exist, false);
    chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(MinioChunkManagerTest, WritePositive) {
    string testBucketName = configs_.bucket_name;
    chunk_manager_->SetBucketName(testBucketName);
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

TEST_F(MinioChunkManagerTest, ReadPositive) {
    string testBucketName = configs_.bucket_name;
    chunk_manager_->SetBucketName(testBucketName);
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
    chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(MinioChunkManagerTest, ReadNotExist) {
    string testBucketName = configs_.bucket_name;
    chunk_manager_->SetBucketName(testBucketName);
    EXPECT_EQ(chunk_manager_->GetBucketName(), testBucketName);

    if (!chunk_manager_->BucketExists(testBucketName)) {
        chunk_manager_->CreateBucket(testBucketName);
    }
    string path = "1/5/8";
    uint8_t readdata[20] = {0};

    EXPECT_THROW(
        try {
            chunk_manager_->Read(path, readdata, sizeof(readdata));
        } catch (SegcoreError& e) {
            EXPECT_TRUE(std::string(e.what()).find("exist") != string::npos);
            throw e;
        },
        SegcoreError);

    chunk_manager_->Remove(path);
    chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(MinioChunkManagerTest, RemovePositive) {
    string testBucketName = "test-remove";
    chunk_manager_->SetBucketName(testBucketName);
    EXPECT_EQ(chunk_manager_->GetBucketName(), testBucketName);

    if (!chunk_manager_->BucketExists(testBucketName)) {
        chunk_manager_->CreateBucket(testBucketName);
    }
    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
    string path = "1/7/8";
    chunk_manager_->Write(path, data, sizeof(data));

    bool exist = chunk_manager_->Exist(path);
    EXPECT_EQ(exist, true);

    bool deleted = chunk_manager_->Remove(path);
    EXPECT_EQ(deleted, true);

    // test double deleted
    deleted = chunk_manager_->Remove(path);
    EXPECT_EQ(deleted, false);

    exist = chunk_manager_->Exist(path);
    EXPECT_EQ(exist, false);

    chunk_manager_->DeleteBucket(testBucketName);
}

TEST_F(MinioChunkManagerTest, ListWithPrefixPositive) {
    string testBucketName = "test-listprefix";
    chunk_manager_->SetBucketName(testBucketName);
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
    std::sort(objs.begin(), objs.end());
    EXPECT_EQ(objs[0], "1/7/4");
    EXPECT_EQ(objs[1], "1/7/8");

    objs = chunk_manager_->ListWithPrefix("//1/7");
    EXPECT_EQ(objs.size(), 2);

    objs = chunk_manager_->ListWithPrefix("1");
    EXPECT_EQ(objs.size(), 3);
    std::sort(objs.begin(), objs.end());
    EXPECT_EQ(objs[0], "1/4/8");
    EXPECT_EQ(objs[1], "1/7/4");

    chunk_manager_->Remove(path1);
    chunk_manager_->Remove(path2);
    chunk_manager_->Remove(path3);
    chunk_manager_->DeleteBucket(testBucketName);
}

//TEST_F(AliyunChunkManagerTest, ReadPositive) {
//    string testBucketName = "vdc-infra-poc";
//    chunk_manager_->SetBucketName(testBucketName);
//    EXPECT_EQ(chunk_manager_->GetBucketName(), testBucketName);
//
//    uint8_t data[5] = {0x17, 0x32, 0x45, 0x34, 0x23};
//    string path = "1/4/6";
//    chunk_manager_->Write(path, data, sizeof(data));
//    bool exist = chunk_manager_->Exist(path);
//    EXPECT_EQ(exist, true);
//    auto size = chunk_manager_->Size(path);
//    EXPECT_EQ(size, 5);
//
//    uint8_t readdata[20] = {0};
//    size = chunk_manager_->Read(path, readdata, 20);
//    EXPECT_EQ(readdata[0], 0x17);
//    EXPECT_EQ(readdata[1], 0x32);
//    EXPECT_EQ(readdata[2], 0x45);
//    EXPECT_EQ(readdata[3], 0x34);
//    EXPECT_EQ(readdata[4], 0x23);
//
//    size = chunk_manager_->Read(path, readdata, 3);
//    EXPECT_EQ(size, 3);
//    EXPECT_EQ(readdata[0], 0x17);
//    EXPECT_EQ(readdata[1], 0x32);
//    EXPECT_EQ(readdata[2], 0x45);
//
//    uint8_t dataWithNULL[] = {0x17, 0x32, 0x00, 0x34, 0x23};
//    chunk_manager_->Write(path, dataWithNULL, sizeof(dataWithNULL));
//    exist = chunk_manager_->Exist(path);
//    EXPECT_EQ(exist, true);
//    size = chunk_manager_->Size(path);
//    EXPECT_EQ(size, 5);
//    size = chunk_manager_->Read(path, readdata, 20);
//    EXPECT_EQ(readdata[0], 0x17);
//    EXPECT_EQ(readdata[1], 0x32);
//    EXPECT_EQ(readdata[2], 0x00);
//    EXPECT_EQ(readdata[3], 0x34);
//    EXPECT_EQ(readdata[4], 0x23);
//
//    chunk_manager_->Remove(path);
//}
