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

#include "common/Common.h"
#include "index/Utils.h"

using namespace milvus;
using namespace milvus::index;

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

TEST(UtilIndex, ReadFromFD) {
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

TEST(UtilIndex, TestGetValueFromConfig) {
    nlohmann::json cfg = nlohmann::json::parse(
        R"({"a" : 100, "b" : true, "c" : "true", "d" : 1.234, "e" : null})");
    auto a_value = GetValueFromConfig<int64_t>(cfg, "a");
    ASSERT_EQ(a_value.value(), 100);

    auto b_value = GetValueFromConfig<bool>(cfg, "b");
    ASSERT_TRUE(b_value.value());

    auto c_value = GetValueFromConfig<bool>(cfg, "c");
    ASSERT_TRUE(c_value.value());

    auto d_value = GetValueFromConfig<double>(cfg, "d");
    ASSERT_NEAR(d_value.value(), 1.234, 0.001);

    try {
        GetValueFromConfig<std::string>(cfg, "d");
    } catch (const std::exception& e) {
        std::cout << e.what() << std::endl;
        ASSERT_EQ(std::string(e.what()).find("config type error for key") !=
                      std::string::npos,
                  true);
    }

    auto e_value = GetValueFromConfig<std::string>(cfg, "e");
    ASSERT_FALSE(e_value.has_value());
}

TEST(UtilIndex, TestGetValueFromConfigWithoutTypeCheck) {
    nlohmann::json cfg = nlohmann::json::parse(
        R"({"a" : 100, "b" : true, "c" : "true", "d" : 1.234, "e" : "1.234", "f" : null})");
    SetDefaultConfigParamTypeCheck(false);
    auto a_value = GetValueFromConfig<int64_t>(cfg, "a");
    ASSERT_EQ(a_value.value(), 100);
    std::cout << "a_value: " << a_value.value() << std::endl;

    auto b_value = GetValueFromConfig<bool>(cfg, "b");
    ASSERT_TRUE(b_value.value());
    std::cout << "b_value: " << b_value.value() << std::endl;
    auto c_value = GetValueFromConfig<bool>(cfg, "c");
    ASSERT_TRUE(c_value.value());
    std::cout << "c_value: " << c_value.value() << std::endl;
    auto d_value = GetValueFromConfig<double>(cfg, "d");
    ASSERT_NEAR(d_value.value(), 1.234, 0.001);
    std::cout << "d_value: " << d_value.value() << std::endl;
    auto e_value = GetValueFromConfig<double>(cfg, "e");
    ASSERT_FALSE(e_value.has_value());
    auto f_value = GetValueFromConfig<bool>(cfg, "f");
    ASSERT_FALSE(f_value.has_value());
}