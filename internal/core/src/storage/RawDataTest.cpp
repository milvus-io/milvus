// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <vector>

#include "storage/Util.h"

namespace milvus::storage {

class RawDataTest : public testing::Test {
 protected:
    void
    SetUp() override {
        auto path =
            (std::filesystem::temp_directory_path() / "index-input-XXXXXX")
                .string();
        ASSERT_NE(::mkdtemp(path.data()), nullptr);
        directory_ = path;
        path_ = path + "/raw_data";
    }

    void
    TearDown() override {
        std::error_code ignored;
        std::filesystem::remove_all(directory_, ignored);
    }

    std::string directory_;
    std::string path_;
};

TEST_F(RawDataTest, SpillRoundTrip) {
    constexpr uint32_t rows = 131073;
    constexpr uint32_t dim = 32;
    constexpr size_t row_bytes = dim * sizeof(float);
    std::vector<uint8_t> data(rows * row_bytes);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = i % 251;
    }
    std::atomic<bool> canceled{false};
    ASSERT_TRUE(
        SpillRawData(path_, data.data(), rows, dim, row_bytes, canceled));
    EXPECT_FALSE(std::filesystem::exists(path_ + ".partial"));
    ASSERT_EQ(std::filesystem::file_size(path_), data.size() + 8);
    std::ifstream input(path_, std::ios::binary);
    uint32_t header[2]{};
    input.read(reinterpret_cast<char*>(header), sizeof(header));
    EXPECT_EQ(header[0], rows);
    EXPECT_EQ(header[1], dim);
    std::vector<uint8_t> restored(data.size());
    input.read(reinterpret_cast<char*>(restored.data()), restored.size());
    ASSERT_TRUE(input.good());
    EXPECT_EQ(restored, data);
    std::fill(restored.begin(), restored.end(), 0);
    ReadRawData(path_, restored.data(), restored.size());
    EXPECT_EQ(restored, data);
    std::filesystem::resize_file(path_, sizeof(header) + data.size() - 1);
    try {
        ReadRawData(path_, restored.data(), restored.size());
        FAIL() << "truncated spill must not expose uninitialized input";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FileReadFailed);
    }
}

TEST_F(RawDataTest, CanceledSpillDoesNotPublishInput) {
    float data[]{1, 2};
    std::atomic<bool> canceled{true};
    EXPECT_FALSE(SpillRawData(path_, data, 1, 2, sizeof(data), canceled));
    EXPECT_FALSE(std::filesystem::exists(path_));
    EXPECT_FALSE(std::filesystem::exists(path_ + ".partial"));
}

TEST_F(RawDataTest, SpillOpenFailurePreservesError) {
    float data[]{1, 2};
    std::atomic<bool> canceled{false};
    std::filesystem::create_directory(path_ + ".partial");
    try {
        SpillRawData(path_, data, 1, 2, sizeof(data), canceled);
        FAIL() << "spill must fail when its destination cannot be opened";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FileWriteFailed);
    }
    EXPECT_FALSE(std::filesystem::exists(path_));
}

}  // namespace milvus::storage
