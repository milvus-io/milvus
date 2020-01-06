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
#include "storage/s3/S3IOReader.h"
#include "storage/s3/S3IOWriter.h"
#include "storage/IStorage.h"
#include "storage/MockS3Client.h"
#include "storage/utils.h"

#define private public
#include "storage/s3/S3ClientWrapper.h"

INITIALIZE_EASYLOGGINGPP

TEST_F(StorageTest, S3_CLIENT_TEST) {
    const std::string filename = "/tmp/test_file_in";
    const std::string filename_out = "/tmp/test_file_out";
    const std::string object_name = "/tmp/test_obj";
    const std::string content = "abcdefghijklmnopqrstuvwxyz";

    auto& storage_inst = milvus::storage::S3ClientWrapper::GetInstance();
    if (!storage_inst.StartService().ok()) {
        storage_inst.client_ptr_ = std::make_shared<MockS3Client>();
    }

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

    storage_inst.StopService();
}

TEST_F(StorageTest, S3_RW_TEST) {
    const std::string index_name = "/tmp/test_index";
    const std::string content = "abcdefg";

    auto& storage_inst = milvus::storage::S3ClientWrapper::GetInstance();
    if (!storage_inst.StartService().ok()) {
        storage_inst.client_ptr_ = std::make_shared<MockS3Client>();
    }

    {
        milvus::storage::S3IOWriter writer(index_name);
        size_t len = content.length();
        writer.write(&len, sizeof(len));
        writer.write((void*)(content.data()), len);
    }

    {
        milvus::storage::S3IOReader reader(index_name);
        size_t length = reader.length();
        size_t rp = 0;
        reader.seekg(rp);
        std::string content_out;
        while (rp < length) {
            size_t len;
            reader.read(&len, sizeof(len));
            rp += sizeof(len);
            reader.seekg(rp);

            auto data = new char[len];
            reader.read(data, len);
            rp += len;
            reader.seekg(rp);

            content_out += std::string(data, len);

            delete[] data;
        }

        ASSERT_TRUE(content == content_out);
    }

    storage_inst.StopService();
}
