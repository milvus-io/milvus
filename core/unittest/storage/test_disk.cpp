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
#include <fiu-local.h>
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
        ASSERT_TRUE(writer.open(index_name));
        size_t len = content.length();
        writer.write(&len, sizeof(len));
        writer.write((void*)(content.data()), len);
        ASSERT_TRUE(len + sizeof(len) == writer.length());
        writer.close();
    }

    {
        milvus::storage::DiskIOReader reader;
        ASSERT_FALSE(reader.open("/tmp/notexist"));
        ASSERT_TRUE(reader.open(index_name));
        int64_t length = reader.length();
        int64_t rp = 0;
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
        reader.close();
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
