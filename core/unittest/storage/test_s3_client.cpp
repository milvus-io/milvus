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
#include "server/Config.h"
#include "storage/IStorage.h"
#include "storage/s3/S3ClientWrapper.h"
#include "storage/utils.h"

INITIALIZE_EASYLOGGINGPP

TEST_F(StorageTest, S3_CLIENT_TEST) {
    const std::string filename = "/tmp/test_file_in";
    const std::string filename_out = "/tmp/test_file_out";
    const std::string object_name = "/tmp/test_obj";
    const std::string content = "abcdefghijklmnopqrstuvwxyz";

    std::string config_path(CONFIG_PATH);
    config_path += CONFIG_FILE;
    milvus::server::Config& config = milvus::server::Config::GetInstance();
    ASSERT_TRUE(config.LoadConfigFile(config_path).ok());

    auto storage_inst = milvus::storage::S3ClientWrapper::GetInstance();
    ASSERT_TRUE(storage_inst.StartService().ok());

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
        ASSERT_TRUE(storage_inst.PutObjectFile(filename, filename).ok());

        ASSERT_TRUE(storage_inst.GetObjectFile(filename, filename_out).ok());
        std::ifstream fs_out(filename_out);
        std::string str_out;
        fs_out >> str_out;
        ASSERT_TRUE(str_out == ss_in.str());
    }

    ///////////////////////////////////////////////////////////////////////////
    /* check PutObjectStr() and GetObjectStr() */
    {
        ASSERT_TRUE(storage_inst.PutObjectStr(object_name, content).ok());

        std::string content_out;
        ASSERT_TRUE(storage_inst.GetObjectStr(object_name, content_out).ok());
        ASSERT_TRUE(content_out == content);
    }

    ///////////////////////////////////////////////////////////////////////////
    ASSERT_TRUE(storage_inst.DeleteObjects("/tmp").ok());

    ASSERT_TRUE(storage_inst.DeleteBucket().ok());

    ASSERT_TRUE(storage_inst.StopService().ok());
}

