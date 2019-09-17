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

//
//#include "storage/IStorage.h"
//#include "storage/s3/S3ClientWrapper.h"
//#include <gtest/gtest.h>
//#include <memory.h>
//#include <fstream>
//
//
//using namespace zilliz::milvus::engine;
//
//TEST(s3_client_wrapper, CLIENT_WRAPPER_TEST) {
//
//    std::shared_ptr<storage::IStorage> storage_ptr = std::make_shared<storage::S3ClientWrapper>();
//
//    std::string ip_address = "127.0.0.1";
//    std::string port = "9000";
//    std::string access_key = "AKIAIOSFODNN7EXAMPLE";
//    std::string secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
//
//    Status status = storage_ptr->Create(ip_address, port, access_key, secret_key);
//    ASSERT_TRUE(status.ok());
//
//    std::string filename = "/tmp/s3_test_file";
//    std::string bucket_name = "bucktname";
//    std::string object_name = "test_file";
//
//    status = storage_ptr->CreateBucket(bucket_name);
//    std::cout << status.IsAlreadyExist() << std::endl;
//    if (status.IsAlreadyExist()) {
//        status = storage_ptr->DeleteBucket(bucket_name);
//        status = storage_ptr->CreateBucket(bucket_name);
//    }
//
//    ASSERT_TRUE(status.ok());
//
//    std::ofstream ofile(filename);
//    std::stringstream ss;
//    for (int i = 0; i < 1024; ++i) {
//        ss << i;
//    }
//    ofile << ss.str() << std::endl;
//    ofile.close();
//    status = storage_ptr->UploadFile(bucket_name, object_name, filename);
//    ASSERT_TRUE(status.ok());
//
//    status = storage_ptr->DownloadFile(bucket_name, object_name, filename);
//    std::ifstream infile(filename);
//    std::string in_buffer;
//    infile >> in_buffer;
//    ASSERT_STREQ(in_buffer.c_str(), ss.str().c_str());
//
//    status = storage_ptr->DeleteFile(bucket_name, object_name);
//    ASSERT_TRUE(status.ok());
//
//    status = storage_ptr->DeleteBucket(bucket_name);
//    ASSERT_TRUE(status.ok());
//
//    status = storage_ptr->Close();
//    ASSERT_TRUE(status.ok());
//}
//
