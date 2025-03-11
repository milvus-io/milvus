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
#include "test_utils/indexbuilder_test_utils.h"

using namespace std;
using namespace milvus;
using namespace milvus::storage;

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

/*TEST_F(MinioChunkManagerTest, RemovePositive) {
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
}*/

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

#include <aws/core/utils/threading/Executor.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/Globals.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/ListObjectsRequest.h>
#include "segcore/segcore_init_c.h"

Aws::String
ConvertToAwsString(const std::string& str) {
    return Aws::String(str.c_str(), str.size());
}

TEST(AWSClient, Init){
    SegcoreInit("/home/hanchun/Documents/project/milvus-master-temp/milvus/configs/glog.conf");
    Aws::SDKOptions options;
    auto log_level = Aws::Utils::Logging::LogLevel::Trace;
    options.loggingOptions.logLevel = log_level;
    options.loggingOptions.logger_create_fn = [log_level]() {
        return std::make_shared<AwsLogger>(log_level);
    };
    Aws::InitAPI(options);
    Aws::SetDefaultTlsConnectionOptions(nullptr);

    static const char* ALLOCATION_TAG = "BucketAndObjectOperationTest";
    Aws::S3Crt::ClientConfiguration s3ClientConfig;
    s3ClientConfig.region = "us-east-1";
    s3ClientConfig.scheme = Aws::Http::Scheme::HTTP;
    s3ClientConfig.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(ALLOCATION_TAG, 4);
    s3ClientConfig.throughputTargetGbps = 2.0;
    s3ClientConfig.partSize = 5 * 1024 * 1024;
    s3ClientConfig.endpointOverride = ConvertToAwsString("http://minio.local:9000");

    Aws::Auth::AWSCredentials credentials(ConvertToAwsString("minioadmin"),
                                          ConvertToAwsString("minioadmin"));

    std::cout << "hc==start to construct client" << std::endl;
    auto client = Aws::MakeShared<Aws::S3Crt::S3CrtClient>(ALLOCATION_TAG,
                                                           credentials,
                                                  s3ClientConfig,
                                          Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always /*signPayloads*/,
                                          false);

    std::cout << "hc==construct client ok" << std::endl;

    {
        auto res = client->ListBuckets();
        std::cout << "hc==list buckets isSuccess:" << res.IsSuccess() << ",errCode:"
                  << int(res.GetError().GetResponseCode()) << ", exception:"
                  << res.GetError().GetExceptionName() << ", errMsg:"
                  << res.GetError().GetMessage()
                  << std::endl;

        for(auto bucket: res.GetResult().GetBuckets()){
            std::cout << "hc==bucket:" << string(bucket.GetName().c_str(), bucket.GetName().length()) << std::endl;
        }

        Aws::S3Crt::Model::ListObjectsRequest request;
        request.SetBucket("a-bucket");
        request.SetPrefix("files/insert_log/456616016241632061/456616016241632062/456616016241632078/1/456616016241430263");
        auto listRes = client->ListObjects(request);
        for(auto obj: listRes.GetResult().GetContents()){
            std::cout << "hc==List obj:" << obj.GetKey().c_str() << std::endl;
        }
    }
    {
        Aws::String objectKey = "files/insert_log/456616016241632061/456616016241632062/456616016241632078/1/456616016241430263";
        Aws::String encodedKey = Aws::Utils::StringUtils::URLEncode(objectKey.c_str());
        Aws::S3Crt::Model::GetObjectRequest request;
        request.SetBucket("a-bucket");
        request.SetKey(encodedKey);
        auto res = client->GetObject(request);
        std::cout << "hc==get isSuccess:" << res.IsSuccess() << ",errCode:"
            << int(res.GetError().GetResponseCode()) << ", exception:"
            << res.GetError().GetExceptionName() << ", errMsg:"
            << res.GetError().GetMessage()
            << ", contentLength:" << res.GetResult().GetContentLength()
            << std::endl;
    }

    client.reset();
    Aws::ShutdownAPI(options);
}

    /*
     * {
        Aws::S3Crt::Model::GetObjectRequest request;
        request.SetBucket("a-bucket");
        request.SetKey("/files/insert_log/456616016241632061/456616016241632062/456616016241632078/1/456616016241430263");
        auto size = 128;
        std::vector<uint8_t> buf(size);
        char* buf_data = reinterpret_cast<char*>(buf.data());
        request.SetResponseStreamFactory([buf_data, size]() {
            std::cout << "hc===write stream is called" << std::endl;
            std::unique_ptr<Aws::StringStream> stream(
                    Aws::New<Aws::StringStream>(""));
            auto str_buf = stream->rdbuf();
            auto str = str_buf->str();
            std::cout << "hc===data in content.size: " << str.size() << ", str:" << str << std::endl;
            return stream.release();
        });
        auto res = client->GetObject(request);
        std::cout << "hc===get isSuccess:" << res.IsSuccess() << ",errCode:"
                  << int(res.GetError().GetResponseCode()) << ", exception:"
                  << res.GetError().GetExceptionName() << ", errMsg:"
                  << res.GetError().GetMessage()
                  << ", contentLength:" << res.GetResult().GetContentLength()
                  << std::endl;
    }
     * */

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
