// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <cerrno>
#include <cstdint>
#include <fcntl.h>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "index/Utils.h"

// A simple wrapper that removes a temporary file.
struct TmpFileWrapperIndexUtilsTest {
    int fd = -1;
    std::string filename;

    explicit TmpFileWrapperIndexUtilsTest(const std::string& _filename)
        : filename{_filename} {
        fd = open(filename.c_str(),
                  O_RDWR | O_CREAT | O_EXCL,
                  S_IRUSR | S_IWUSR | S_IXUSR);
    }
    TmpFileWrapperIndexUtilsTest(const TmpFileWrapperIndexUtilsTest&) = delete;
    TmpFileWrapperIndexUtilsTest(TmpFileWrapperIndexUtilsTest&&) = delete;
    TmpFileWrapperIndexUtilsTest&
    operator=(const TmpFileWrapperIndexUtilsTest&) = delete;
    TmpFileWrapperIndexUtilsTest&
    operator=(TmpFileWrapperIndexUtilsTest&&) = delete;
    ~TmpFileWrapperIndexUtilsTest() {
        if (fd != -1) {
            close(fd);
            remove(filename.c_str());
        }
    }
};

TEST(Util_Index, ReadFromFD) {
    auto uuid = boost::uuids::random_generator()();
    auto uuid_string = boost::uuids::to_string(uuid);
    auto file = std::string("/tmp/") + uuid_string;

    auto tmp_file = TmpFileWrapperIndexUtilsTest(file);
    ASSERT_NE(tmp_file.fd, -1);

    size_t data_size = 100 * 1024 * 1024;  // 100M
    auto index_data = std::shared_ptr<uint8_t[]>(new uint8_t[data_size]);
    auto max_loop = size_t(INT_MAX) / data_size + 1;  // insert data > 2G
    for (int i = 0; i < static_cast<int>(max_loop); ++i) {
        auto size_write = write(tmp_file.fd, index_data.get(), data_size);
        ASSERT_GE(size_write, 0);
    }

    auto read_buf =
        std::shared_ptr<uint8_t[]>(new uint8_t[data_size * max_loop]);
    EXPECT_NO_THROW(milvus::index::ReadDataFromFD(
        tmp_file.fd, read_buf.get(), data_size * max_loop));

    // On Linux, read() (and similar system calls) will transfer at most 0x7ffff000 (2,147,479,552) bytes once
    EXPECT_THROW(
        milvus::index::ReadDataFromFD(
            tmp_file.fd, read_buf.get(), data_size * max_loop, INT_MAX),
        milvus::SegcoreError);
}
