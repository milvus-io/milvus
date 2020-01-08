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
#include <fiu-local.h>
#include <fiu-control.h>

#include "easyloggingpp/easylogging++.h"
#include "server/Config.h"
#include "storage/s3/S3ClientWrapper.h"
#include "storage/s3/S3IOReader.h"
#include "storage/s3/S3IOWriter.h"
#include "storage/IStorage.h"
#include "storage/utils.h"

INITIALIZE_EASYLOGGINGPP

TEST_F(StorageTest, S3_CLIENT_TEST) {
    fiu_init(0);

    const std::string filename = "/tmp/test_file_in";
    const std::string filename_dummy = "/tmp/test_file_dummy";
    const std::string filename_out = "/tmp/test_file_out";
    const std::string objname = "/tmp/test_obj";
    const std::string objname_dummy = "/tmp/test_obj_dummy";
    const std::string content = "abcdefghijklmnopqrstuvwxyz";

    auto& storage_inst = milvus::storage::S3ClientWrapper::GetInstance();
    fiu_enable("S3ClientWrapper.StartService.mock_enable", 1, NULL, 0);
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
        ASSERT_TRUE(storage_inst.PutObjectStr(objname, content).ok());

        std::string content_out;
        ASSERT_TRUE(storage_inst.GetObjectStr(objname, content_out).ok());
        ASSERT_TRUE(content_out == content);
    }

    ///////////////////////////////////////////////////////////////////////////
    ASSERT_TRUE(storage_inst.DeleteObject(filename).ok());
    ASSERT_TRUE(storage_inst.DeleteObject(objname).ok());

    ASSERT_TRUE(storage_inst.DeleteObjects("/tmp").ok());

    ASSERT_TRUE(storage_inst.DeleteBucket().ok());

    storage_inst.StopService();
}

TEST_F(StorageTest, S3_RW_TEST) {
    fiu_init(0);

    const std::string index_name = "/tmp/test_index";
    const std::string content = "abcdefg";

    auto& storage_inst = milvus::storage::S3ClientWrapper::GetInstance();
    fiu_enable("S3ClientWrapper.StartService.mock_enable", 1, NULL, 0);
    ASSERT_TRUE(storage_inst.StartService().ok());

    {
        milvus::storage::S3IOWriter writer(index_name);
        size_t len = content.length();
        writer.write(&len, sizeof(len));
        writer.write((void*)(content.data()), len);
        ASSERT_TRUE(len + sizeof(len) == writer.length());
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

TEST_F(StorageTest, S3_FAIL_TEST) {
    fiu_init(0);

    const std::string filename = "/tmp/test_file_in";
    const std::string filename_dummy = "/tmp/test_file_dummy";
    const std::string filename_out = "/tmp/test_file_out";
    const std::string objname = "/tmp/test_obj";
    const std::string objname_dummy = "/tmp/test_obj_dummy";
    const std::string content = "abcdefghijklmnopqrstuvwxyz";

    auto& storage_inst = milvus::storage::S3ClientWrapper::GetInstance();

    fiu_enable("S3ClientWrapper.StartService.minio_disable", 1, NULL, 0);
    ASSERT_TRUE(storage_inst.StartService().ok());
    fiu_disable("S3ClientWrapper.StartService.minio_disable");

    fiu_enable("S3ClientWrapper.StartService.mock_enable", 1, NULL, 0);
    ASSERT_TRUE(storage_inst.StartService().ok());
    fiu_disable("S3ClientWrapper.StartService.mock_enable");

    fiu_enable("S3ClientWrapper.CreateBucket.outcome.fail", 1, NULL, 0);
    ASSERT_FALSE(storage_inst.CreateBucket().ok());
    fiu_disable("S3ClientWrapper.CreateBucket.outcome.fail");

    ///////////////////////////////////////////////////////////////////////////
    /* check PutObjectFile() and GetObjectFile() */
    {
        fiu_enable("S3ClientWrapper.PutObjectFile.outcome.fail", 1, NULL, 0);
        ASSERT_FALSE(storage_inst.PutObjectFile(filename, filename).ok());
        fiu_disable("S3ClientWrapper.PutObjectFile.outcome.fail");

        fiu_enable("S3ClientWrapper.GetObjectFile.outcome.fail", 1, NULL, 0);
        ASSERT_FALSE(storage_inst.GetObjectFile(filename, filename_out).ok());
        fiu_disable("S3ClientWrapper.GetObjectFile.outcome.fail");

        ASSERT_FALSE(storage_inst.PutObjectFile(filename_dummy, filename_dummy).ok());
        ASSERT_FALSE(storage_inst.GetObjectFile(filename_dummy, filename_out).ok());
    }

    ///////////////////////////////////////////////////////////////////////////
    /* check PutObjectStr() and GetObjectStr() */
    {
        fiu_enable("S3ClientWrapper.PutObjectStr.outcome.fail", 1, NULL, 0);
        ASSERT_FALSE(storage_inst.PutObjectStr(objname, content).ok());
        fiu_disable("S3ClientWrapper.PutObjectStr.outcome.fail");

        std::string content_out;
        fiu_enable("S3ClientWrapper.GetObjectStr.outcome.fail", 1, NULL, 0);
        ASSERT_FALSE(storage_inst.GetObjectStr(objname, content_out).ok());
        fiu_disable("S3ClientWrapper.GetObjectStr.outcome.fail");

        ASSERT_FALSE(storage_inst.GetObjectStr(objname_dummy, content_out).ok());
    }

    ///////////////////////////////////////////////////////////////////////////
    fiu_enable("S3ClientWrapper.DeleteObject.outcome.fail", 1, NULL, 0);
    ASSERT_FALSE(storage_inst.DeleteObject(filename).ok());
    fiu_disable("S3ClientWrapper.DeleteObject.outcome.fail");

    fiu_enable("S3ClientWrapper.ListObjects.outcome.fail", 1, NULL, 0);
    ASSERT_FALSE(storage_inst.DeleteObjects("/tmp").ok());
    fiu_disable("S3ClientWrapper.ListObjects.outcome.fail");
    ASSERT_TRUE(storage_inst.DeleteObjects("/tmp").ok());

    fiu_enable("S3ClientWrapper.DeleteBucket.outcome.fail", 1, NULL, 0);
    ASSERT_FALSE(storage_inst.DeleteBucket().ok());
    fiu_disable("S3ClientWrapper.DeleteBucket.outcome.fail");

    storage_inst.StopService();
}
