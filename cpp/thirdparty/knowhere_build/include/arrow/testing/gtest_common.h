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

#ifndef ARROW_TEST_COMMON_H
#define ARROW_TEST_COMMON_H

#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"

namespace arrow {

class TestBase : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = default_memory_pool();
    random_seed_ = 0;
  }

  std::shared_ptr<Buffer> MakeRandomNullBitmap(int64_t length, int64_t null_count) {
    const int64_t null_nbytes = BitUtil::BytesForBits(length);

    std::shared_ptr<Buffer> null_bitmap;
    ARROW_EXPECT_OK(AllocateBuffer(pool_, null_nbytes, &null_bitmap));
    memset(null_bitmap->mutable_data(), 255, null_nbytes);
    for (int64_t i = 0; i < null_count; i++) {
      BitUtil::ClearBit(null_bitmap->mutable_data(), i * (length / null_count));
    }
    return null_bitmap;
  }

  template <typename ArrayType>
  inline std::shared_ptr<Array> MakeRandomArray(int64_t length, int64_t null_count = 0);

 protected:
  uint32_t random_seed_;
  MemoryPool* pool_;
};

template <typename ArrayType>
std::shared_ptr<Array> TestBase::MakeRandomArray(int64_t length, int64_t null_count) {
  const int64_t data_nbytes = length * sizeof(typename ArrayType::value_type);
  std::shared_ptr<Buffer> data;
  ARROW_EXPECT_OK(AllocateBuffer(pool_, data_nbytes, &data));

  // Fill with random data
  random_bytes(data_nbytes, random_seed_++, data->mutable_data());
  std::shared_ptr<Buffer> null_bitmap = MakeRandomNullBitmap(length, null_count);

  return std::make_shared<ArrayType>(length, data, null_bitmap, null_count);
}

template <>
inline std::shared_ptr<Array> TestBase::MakeRandomArray<NullArray>(int64_t length,
                                                                   int64_t null_count) {
  return std::make_shared<NullArray>(length);
}

template <>
inline std::shared_ptr<Array> TestBase::MakeRandomArray<FixedSizeBinaryArray>(
    int64_t length, int64_t null_count) {
  const int byte_width = 10;
  std::shared_ptr<Buffer> null_bitmap = MakeRandomNullBitmap(length, null_count);
  std::shared_ptr<Buffer> data;
  ARROW_EXPECT_OK(AllocateBuffer(pool_, byte_width * length, &data));

  ::arrow::random_bytes(data->size(), 0, data->mutable_data());
  return std::make_shared<FixedSizeBinaryArray>(fixed_size_binary(byte_width), length,
                                                data, null_bitmap, null_count);
}

template <>
inline std::shared_ptr<Array> TestBase::MakeRandomArray<BinaryArray>(int64_t length,
                                                                     int64_t null_count) {
  std::vector<uint8_t> valid_bytes(length, 1);
  for (int64_t i = 0; i < null_count; i++) {
    valid_bytes[i * 2] = 0;
  }
  BinaryBuilder builder(pool_);

  const int kBufferSize = 10;
  uint8_t buffer[kBufferSize];
  for (int64_t i = 0; i < length; i++) {
    if (!valid_bytes[i]) {
      ARROW_EXPECT_OK(builder.AppendNull());
    } else {
      ::arrow::random_bytes(kBufferSize, static_cast<uint32_t>(i), buffer);
      ARROW_EXPECT_OK(builder.Append(buffer, kBufferSize));
    }
  }

  std::shared_ptr<Array> out;
  ARROW_EXPECT_OK(builder.Finish(&out));
  return out;
}

class TestBuilder : public ::testing::Test {
 public:
  void SetUp() { pool_ = default_memory_pool(); }

 protected:
  MemoryPool* pool_;
  std::shared_ptr<DataType> type_;
};

}  // namespace arrow

#endif  // ARROW_TEST_COMMON_H_
