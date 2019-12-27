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


#include <gtest/gtest.h>
#include <fstream>
#include <memory>

#include "easyloggingpp/easylogging++.h"
#include "storage/IStorage.h"
#include "storage/s3/S3ClientWrapper.h"

INITIALIZE_EASYLOGGINGPP

TEST(StorageTest, S3_CLIENT_TEST) {
    std::string ip_address = "127.0.0.1";
    std::string port = "9000";

    // get from "/data/.minio.sys/config/config.json" in docker minio/minio
    std::string access_key = "minioadmin";
    std::string secret_key = "minioadmin";

    std::string bucket_name = "bucket";
    std::string object_name = "test_file";
    std::string filename_in = "/tmp/s3_test_file_in";
    std::string filename_out = "/tmp/s3_test_file_out";
    std::string content = "abcdefghijklmnopqrstuvwxyz";

    std::shared_ptr<milvus::storage::IStorage> storage_ptr = std::make_shared<milvus::storage::S3ClientWrapper>();
    milvus::Status status = storage_ptr->Create(ip_address, port, access_key, secret_key);
    ASSERT_TRUE(status.ok());

    status = storage_ptr->CreateBucket(bucket_name);
    ASSERT_TRUE(status.ok());
    status = storage_ptr->CreateBucket(bucket_name);
    ASSERT_TRUE(status.ok());

    ///////////////////////////////////////////////////////////////////////////
    /* check PutObjectFile() */
    {
        std::ofstream ofile(filename_in);
        std::stringstream ss;
        for (int i = 0; i < 1024; ++i) {
            ss << i;
        }
        ofile << ss.str() << std::endl;
        ofile.close();
        status = storage_ptr->PutObjectFile(bucket_name, object_name, filename_in);
        ASSERT_TRUE(status.ok());

        status = storage_ptr->GetObjectFile(bucket_name, object_name, filename_out);
        std::ifstream infile(filename_out);
        std::string in_buffer;
        infile >> in_buffer;
        ASSERT_STREQ(in_buffer.c_str(), ss.str().c_str());
    }

    ///////////////////////////////////////////////////////////////////////////
    /* check PutObjectStr() */
    {
        status = storage_ptr->PutObjectStr(bucket_name, object_name, content);
        ASSERT_TRUE(status.ok());

        status = storage_ptr->GetObjectFile(bucket_name, object_name, filename_out);
        std::ifstream infile(filename_out);
        std::string in_buffer;
        infile >> in_buffer;
        ASSERT_STREQ(in_buffer.c_str(), content.c_str());
    }

    ///////////////////////////////////////////////////////////////////////////
    status = storage_ptr->DeleteObject(bucket_name, object_name);
    ASSERT_TRUE(status.ok());

    status = storage_ptr->DeleteBucket(bucket_name);
    ASSERT_TRUE(status.ok());

    status = storage_ptr->Close();
    ASSERT_TRUE(status.ok());
}

