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

#include "easyloggingpp/easylogging++.h"
#include "gtest/gtest.h"

#include "ExtraFileInfo.h"
#include "crc32c/crc32c.h"
#include "storage/disk/DiskIOReader.h"
#include "storage/disk/DiskIOWriter.h"
#include "storage/disk/DiskOperation.h"

INITIALIZE_EASYLOGGINGPP

namespace milvus {
namespace storage {

/* ExtraFileInfoTest */
class ExtraFileInfoTest : public testing::Test {
 protected:
};

TEST_F(ExtraFileInfoTest, WriteFileTest) {
    std::string raw = "helloworldhelloworld";

    std::string directory = "/home/godchen/";
    storage::IOReaderPtr reader_ptr = std::make_shared<storage::DiskIOReader>();
    storage::IOWriterPtr writer_ptr = std::make_shared<storage::DiskIOWriter>();
    storage::OperationPtr operation_ptr = std::make_shared<storage::DiskOperation>(directory);
    const storage::FSHandlerPtr fs_ptr = std::make_shared<storage::FSHandler>(reader_ptr, writer_ptr, operation_ptr);
    std::string file_path = "/home/godchen/test.txt";

    auto record = std::unordered_map<std::string, std::string>();
    record.insert(std::make_pair("test", "test"));
    WriteMagic(fs_ptr, file_path);
    WriteHeaderValues(fs_ptr, file_path, record);

    if (!fs_ptr->writer_ptr_->InOpen(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
    }
    fs_ptr->writer_ptr_->Seekp(0, std::ios_base::end);

    size_t num_bytes = raw.size();
    fs_ptr->writer_ptr_->Write(&num_bytes, sizeof(size_t));
    fs_ptr->writer_ptr_->Write((void*)(raw.data()), num_bytes);
    fs_ptr->writer_ptr_->Close();

    int result_sum = CalculateSum(fs_ptr, file_path);
    WriteSum(fs_ptr, file_path, result_sum);

    ASSERT_TRUE(CheckSum(fs_ptr, file_path));
    ASSERT_TRUE(ReadHeaderValue(fs_ptr, file_path, "test") == "test");

    ASSERT_TRUE(WriteHeaderValue(fs_ptr, file_path, "github", "gaylab"));
    ASSERT_TRUE(ReadHeaderValue(fs_ptr, file_path, "github") == "gaylab");
    result_sum = CalculateSum(fs_ptr, file_path);
    WriteSum(fs_ptr, file_path, result_sum, true);
    ASSERT_TRUE(CheckMagic(fs_ptr, file_path));
    ASSERT_TRUE(CheckSum(fs_ptr, file_path));
}
}  // namespace storage

}  // namespace milvus
