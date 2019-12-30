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
    const std::string filename = "/tmp/test_file_in";
    const std::string filename_out = "/tmp/test_file_out";
    const std::string object_name = "test_obj";
    const std::string content = "abcdefghijklmnopqrstuvwxyz";

    auto storage_inst = milvus::storage::S3ClientWrapper::GetInstance();
    milvus::Status status = storage_inst.StartService();
    ASSERT_TRUE(status.ok());

    ///////////////////////////////////////////////////////////////////////////
    /* check PutObjectFile() and GetObjectFile() */
    {
        std::ofstream fs_in(filename);
        std::stringstream ss_in;
        for (int i = 0; i < 1024; ++i) {
            ss_in << i;
        }
        fs_in << ss_in.str() << std::endl;
        fs_in.close();
        status = storage_inst.PutObjectFile(filename, filename);
        ASSERT_TRUE(status.ok());

        status = storage_inst.GetObjectFile(filename, filename_out);
        std::ifstream fs_out(filename_out);
        std::string str_out;
        fs_out >> str_out;
        ASSERT_TRUE(str_out == ss_in.str());
    }

    ///////////////////////////////////////////////////////////////////////////
    /* check PutObjectStr() and GetObjectStr() */
    {
        status = storage_inst.PutObjectStr(object_name, content);
        ASSERT_TRUE(status.ok());

        std::string content_out;
        status = storage_inst.GetObjectStr(object_name, content_out);
        ASSERT_TRUE(content_out == content);
    }

    ///////////////////////////////////////////////////////////////////////////
    std::vector<std::string> object_list;
    status = storage_inst.ListObjects(object_list);
    ASSERT_TRUE(status.ok());

    for (const std::string& object_name : object_list) {
        status = storage_inst.DeleteObject(object_name);
        ASSERT_TRUE(status.ok());
    }

    status = storage_inst.DeleteBucket();
    ASSERT_TRUE(status.ok());

    status = storage_inst.StopService();
    ASSERT_TRUE(status.ok());
}

