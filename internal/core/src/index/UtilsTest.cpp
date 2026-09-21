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
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <random>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/Common.h"
#include "index/Utils.h"
#include "storage/IndexData.h"
#include "test_utils/Constants.h"

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
    auto file = TestLocalPath + uuid_string;

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

TEST(UtilIndex, AssembleIndexDataCodecConcatenatesSlices) {
    std::vector<std::string> slices = {"hello", " ", "world"};

    IndexDataCodec codec;
    for (const auto& slice : slices) {
        auto data = std::shared_ptr<uint8_t[]>(new uint8_t[slice.size()]);
        std::memcpy(data.get(), slice.data(), slice.size());
        auto index_data =
            std::make_unique<storage::IndexData>(data.get(), slice.size());
        index_data->SetData(std::move(data));
        codec.size_ += slice.size();
        codec.codecs_.push_back(std::move(index_data));
    }

    auto assembled = AssembleIndexDataCodec(codec);
    ASSERT_EQ(assembled->PayloadSize(), codec.size_);
    EXPECT_EQ(
        std::string(reinterpret_cast<const char*>(assembled->PayloadData()),
                    assembled->PayloadSize()),
        "hello world");
}

TEST(UtilIndex, AssembleIndexDataCodecMovesSingleSlice) {
    std::string slice = "hello";
    auto data = std::shared_ptr<uint8_t[]>(new uint8_t[slice.size()]);
    std::memcpy(data.get(), slice.data(), slice.size());
    auto index_data =
        std::make_unique<storage::IndexData>(data.get(), slice.size());
    index_data->SetData(std::move(data));

    auto* original_codec = index_data.get();
    IndexDataCodec codec;
    codec.size_ = slice.size();
    codec.codecs_.push_back(std::move(index_data));

    auto assembled = AssembleIndexDataCodec(std::move(codec));
    EXPECT_EQ(assembled.get(), original_codec);
    EXPECT_EQ(
        std::string(reinterpret_cast<const char*>(assembled->PayloadData()),
                    assembled->PayloadSize()),
        slice);
}

TEST(UtilIndex, SetBitsetSealedFillsContiguousBlocks) {
    TargetBitmap bitmap(256);
    std::vector<uint32_t> doc_ids;
    // a word-aligned block, a block crossing a word boundary, and a tail
    // shorter than a block
    for (uint32_t id = 0; id < 64; ++id) {
        doc_ids.push_back(id);
    }
    for (uint32_t id = 70; id < 134; ++id) {
        doc_ids.push_back(id);
    }
    for (uint32_t id = 200; id < 256; ++id) {
        doc_ids.push_back(id);
    }

    SetBitsetSealed(&bitmap, doc_ids.data(), doc_ids.size());

    for (uint32_t id = 0; id < 256; ++id) {
        const bool expected = id < 64 || (id >= 70 && id < 134) || id >= 200;
        EXPECT_EQ(static_cast<bool>(bitmap[id]), expected) << id;
    }
}

TEST(UtilIndex, SetBitsetSealedVerifiesBlockInterior) {
    // first and last id of the block are one word apart, but the interior is
    // not consecutive: the block must not be range-filled.
    TargetBitmap bitmap(128);
    std::vector<uint32_t> doc_ids;
    for (uint32_t id = 0; id < 64; ++id) {
        doc_ids.push_back(id);
    }
    doc_ids[10] = 11;

    SetBitsetSealed(&bitmap, doc_ids.data(), doc_ids.size());

    EXPECT_FALSE(bitmap[10]);
    EXPECT_EQ(bitmap.count(), 63);
}

TEST(UtilIndex, SetBitsetSealedAcceptsUnorderedIds) {
    TargetBitmap bitmap(192);
    const std::vector<uint32_t> doc_ids = {
        0, 1, 1, 63, 64, 65, 127, 128, 191, 70, 2};

    SetBitsetSealed(&bitmap, doc_ids.data(), doc_ids.size());

    for (const auto doc_id : doc_ids) {
        EXPECT_TRUE(bitmap[doc_id]);
    }
    EXPECT_EQ(bitmap.count(), 10);
}

TEST(UtilIndex, SetBitsetGrowingSkipsOutOfRangeIds) {
    TargetBitmap bitmap(70);
    const std::vector<uint32_t> doc_ids = {0, 63, 64, 69, 70, 100, 2, 64};

    SetBitsetGrowing(&bitmap, doc_ids.data(), doc_ids.size());

    EXPECT_TRUE(bitmap[0]);
    EXPECT_TRUE(bitmap[2]);
    EXPECT_TRUE(bitmap[63]);
    EXPECT_TRUE(bitmap[64]);
    EXPECT_TRUE(bitmap[69]);
    EXPECT_EQ(bitmap.count(), 5);

    // a consecutive block running past the end must not be range-filled
    TargetBitmap tail(100);
    std::vector<uint32_t> block;
    for (uint32_t id = 60; id < 124; ++id) {
        block.push_back(id);
    }

    SetBitsetGrowing(&tail, block.data(), block.size());

    EXPECT_TRUE(tail[60]);
    EXPECT_TRUE(tail[99]);
    EXPECT_EQ(tail.count(), 40);
}

TEST(UtilIndex, SetBitsetMatchesPerBitReference) {
    std::mt19937 rng(42);
    for (int round = 0; round < 64; ++round) {
        const size_t size = 1 + rng() % 2000;
        std::vector<uint32_t> doc_ids;
        // consecutive runs of random length interleaved with random ids,
        // the growing input additionally reaching past the bitset
        for (int piece = 0; piece < 8; ++piece) {
            const uint32_t start = rng() % size;
            const uint32_t len = rng() % 200;
            for (uint32_t k = 0; k < len; ++k) {
                doc_ids.push_back(start + k);
            }
            for (int k = 0; k < 16; ++k) {
                doc_ids.push_back(rng() % size);
            }
        }
        if (round % 2 == 1) {
            std::shuffle(doc_ids.begin(), doc_ids.end(), rng);
        }

        TargetBitmap expected(size);
        for (const auto id : doc_ids) {
            if (id < size) {
                expected[id] = true;
            }
        }

        TargetBitmap growing(size);
        SetBitsetGrowing(&growing, doc_ids.data(), doc_ids.size());

        std::vector<uint32_t> in_range;
        for (const auto id : doc_ids) {
            if (id < size) {
                in_range.push_back(id);
            }
        }
        TargetBitmap sealed(size);
        SetBitsetSealed(&sealed, in_range.data(), in_range.size());

        for (size_t id = 0; id < size; ++id) {
            ASSERT_EQ(static_cast<bool>(growing[id]),
                      static_cast<bool>(expected[id]))
                << round << " " << id;
            ASSERT_EQ(static_cast<bool>(sealed[id]),
                      static_cast<bool>(expected[id]))
                << round << " " << id;
        }
    }
}

TEST(UtilIndex, SetBitsetHandlesEmptyInput) {
    TargetBitmap sealed(65);
    TargetBitmap growing(65);

    SetBitsetSealed(&sealed, nullptr, 0);
    SetBitsetGrowing(&growing, nullptr, 0);

    EXPECT_EQ(sealed.count(), 0);
    EXPECT_EQ(growing.count(), 0);
}
