// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <fiu-control.h>
<<<<<<< HEAD
#include <fiu/fiu-local.h>
=======
#include <fiu-local.h>
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
#include <gtest/gtest.h>

#include "easyloggingpp/easylogging++.h"
#include "storage/disk/DiskIOReader.h"
#include "storage/disk/DiskIOWriter.h"
#include "storage/disk/DiskOperation.h"
#include "storage/utils.h"

INITIALIZE_EASYLOGGINGPP

TEST_F(StorageTest, DISK_RW_TEST) {
    const std::string index_name = "/tmp/test_index";
    const std::string content = "abcdefg";

    {
        milvus::storage::DiskIOWriter writer;
        ASSERT_TRUE(writer.Open(index_name));
        size_t len = content.length();
        writer.Write(&len, sizeof(len));
        writer.Write((void*)(content.data()), len);
        ASSERT_TRUE(len + sizeof(len) == writer.Length());
        writer.Close();
    }

    {
        milvus::storage::DiskIOReader reader;
        ASSERT_FALSE(reader.Open("/tmp/notexist"));
        ASSERT_TRUE(reader.Open(index_name));
        int64_t length = reader.Length();
        int64_t rp = 0;
        reader.Seekg(rp);
        std::string content_out;
        while (rp < length) {
            size_t len;
            reader.Read(&len, sizeof(len));
            rp += sizeof(len);
            reader.Seekg(rp);

            auto data = new char[len];
            reader.Read(data, len);
            rp += len;
            reader.Seekg(rp);

            content_out += std::string(data, len);

            delete[] data;
        }

        ASSERT_TRUE(content == content_out);
        reader.Close();
    }
}

TEST_F(StorageTest, DISK_OPERATION_TEST) {
    auto disk_operation = milvus::storage::DiskOperation("/tmp/milvus_test/milvus_disk_operation_test");

    fiu_init(0);
    fiu_enable("DiskOperation.CreateDirectory.is_directory", 1, NULL, 0);
    fiu_enable("DiskOperation.CreateDirectory.create_directory", 1, NULL, 0);
    ASSERT_ANY_THROW(disk_operation.CreateDirectory());
    fiu_disable("DiskOperation.CreateDirectory.create_directory");
    fiu_disable("DiskOperation.CreateDirectory.is_directory");

    std::vector<std::string> file_paths;
    ASSERT_NO_THROW(disk_operation.ListDirectory(file_paths));

    for (auto & path : file_paths) {
        ASSERT_TRUE(disk_operation.DeleteFile(path));
    }
}

TEST_F(StorageTest, DISK_OPERATION_TEST) {
    auto disk_operation = milvus::storage::DiskOperation("/tmp/milvus_test/milvus_disk_operation_test");

    fiu_init(0);
    fiu_enable("DiskOperation.CreateDirectory.is_directory", 1, NULL, 0);
    fiu_enable("DiskOperation.CreateDirectory.create_directory", 1, NULL, 0);
    ASSERT_ANY_THROW(disk_operation.CreateDirectory());
    fiu_disable("DiskOperation.CreateDirectory.create_directory");
    fiu_disable("DiskOperation.CreateDirectory.is_directory");

    std::vector<std::string> file_paths;
    ASSERT_NO_THROW(disk_operation.ListDirectory(file_paths));

    for (auto & path : file_paths) {
        ASSERT_TRUE(disk_operation.DeleteFile(path));
    }
}
