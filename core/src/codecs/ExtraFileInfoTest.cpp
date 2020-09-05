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

#include <cstring>
#include <unordered_map>

#include "codecs/ExtraFileInfo.h"
#include "crc32c/crc32c.h"
#include "gtest/gtest.h"
#include "utils/Log.h"

#include "storage/disk/DiskIOReader.h"
#include "storage/disk/DiskIOWriter.h"
#include "storage/disk/DiskOperation.h"

INITIALIZE_EASYLOGGINGPP

namespace milvus {
namespace codec {

/* ExtraFileInfoTest */
class ExtraFileInfoTest : public testing::Test {
 protected:
};

TEST_F(ExtraFileInfoTest, WriteFileTest) {
    std::string raw = "helloworldhelloworld";

    std::string directory = "/tmp";
    storage::IOReaderPtr reader_ptr = std::make_shared<storage::DiskIOReader>();
    storage::IOWriterPtr writer_ptr = std::make_shared<storage::DiskIOWriter>();
    storage::OperationPtr operation_ptr = std::make_shared<storage::DiskOperation>(directory);
    const storage::FSHandlerPtr fs_ptr = std::make_shared<storage::FSHandler>(reader_ptr, writer_ptr, operation_ptr);
    std::string file_path = "/tmp/test.txt";

    auto record = std::unordered_map<std::string, std::string>();
    record.insert(std::make_pair("test", "test"));
    record.insert(std::make_pair("github", "github"));
    if (!fs_ptr->writer_ptr_->Open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
    }
    WRITE_MAGIC(fs_ptr);

    size_t num_bytes = raw.size();
    record.insert(std::make_pair("size", std::to_string(num_bytes)));

    std::string header = HeaderWrapper(record);
    WriteHeaderValues(fs_ptr, header);

    fs_ptr->writer_ptr_->Write(raw.data(), num_bytes);

    WRITE_SUM(fs_ptr, header, raw.data(), num_bytes);
    fs_ptr->writer_ptr_->Close();

    if (!fs_ptr->reader_ptr_->Open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
    }

    ASSERT_TRUE(CheckMagic(fs_ptr));
    std::unordered_map<std::string, std::string> headers = ReadHeaderValues(fs_ptr);
    ASSERT_EQ(headers.at("test"), "test");
    ASSERT_EQ(headers.at("github"), "github");
    ASSERT_EQ(stol(headers.at("size")), num_bytes);

    fs_ptr->reader_ptr_->Read(raw.data(), num_bytes);

    ASSERT_TRUE(CheckSum(fs_ptr));
}
}  // namespace codec

}  // namespace milvus
