// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/testharness.h"
#include "utilities/cassandra/serialize.h"

using namespace rocksdb::cassandra;

namespace rocksdb {
namespace cassandra {

TEST(SerializeTest, SerializeI64) {
  std::string dest;
  Serialize<int64_t>(0, &dest);
  EXPECT_EQ(
      std::string(
          {'\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00'}),
      dest);

  dest.clear();
  Serialize<int64_t>(1, &dest);
  EXPECT_EQ(
      std::string(
          {'\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x01'}),
      dest);


  dest.clear();
  Serialize<int64_t>(-1, &dest);
  EXPECT_EQ(
      std::string(
          {'\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff'}),
      dest);

  dest.clear();
  Serialize<int64_t>(9223372036854775807, &dest);
  EXPECT_EQ(
      std::string(
          {'\x7f', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff'}),
      dest);

  dest.clear();
  Serialize<int64_t>(-9223372036854775807, &dest);
  EXPECT_EQ(
      std::string(
          {'\x80', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x01'}),
      dest);
}

TEST(SerializeTest, DeserializeI64) {
  std::string dest;
  std::size_t offset = dest.size();
  Serialize<int64_t>(0, &dest);
  EXPECT_EQ(0, Deserialize<int64_t>(dest.c_str(), offset));

  offset = dest.size();
  Serialize<int64_t>(1, &dest);
  EXPECT_EQ(1, Deserialize<int64_t>(dest.c_str(), offset));

  offset = dest.size();
  Serialize<int64_t>(-1, &dest);
  EXPECT_EQ(-1, Deserialize<int64_t>(dest.c_str(), offset));

  offset = dest.size();
  Serialize<int64_t>(-9223372036854775807, &dest);
  EXPECT_EQ(-9223372036854775807, Deserialize<int64_t>(dest.c_str(), offset));

  offset = dest.size();
  Serialize<int64_t>(9223372036854775807, &dest);
  EXPECT_EQ(9223372036854775807, Deserialize<int64_t>(dest.c_str(), offset));
}

TEST(SerializeTest, SerializeI32) {
  std::string dest;
  Serialize<int32_t>(0, &dest);
  EXPECT_EQ(
      std::string(
          {'\x00', '\x00', '\x00', '\x00'}),
      dest);

  dest.clear();
  Serialize<int32_t>(1, &dest);
  EXPECT_EQ(
      std::string(
          {'\x00', '\x00', '\x00', '\x01'}),
      dest);


  dest.clear();
  Serialize<int32_t>(-1, &dest);
  EXPECT_EQ(
      std::string(
          {'\xff', '\xff', '\xff', '\xff'}),
      dest);

  dest.clear();
  Serialize<int32_t>(2147483647, &dest);
  EXPECT_EQ(
      std::string(
          {'\x7f', '\xff', '\xff', '\xff'}),
      dest);

  dest.clear();
  Serialize<int32_t>(-2147483648LL, &dest);
  EXPECT_EQ(
      std::string(
          {'\x80', '\x00', '\x00', '\x00'}),
      dest);
}

TEST(SerializeTest, DeserializeI32) {
  std::string dest;
  std::size_t offset = dest.size();
  Serialize<int32_t>(0, &dest);
  EXPECT_EQ(0, Deserialize<int32_t>(dest.c_str(), offset));

  offset = dest.size();
  Serialize<int32_t>(1, &dest);
  EXPECT_EQ(1, Deserialize<int32_t>(dest.c_str(), offset));

  offset = dest.size();
  Serialize<int32_t>(-1, &dest);
  EXPECT_EQ(-1, Deserialize<int32_t>(dest.c_str(), offset));

  offset = dest.size();
  Serialize<int32_t>(2147483647, &dest);
  EXPECT_EQ(2147483647, Deserialize<int32_t>(dest.c_str(), offset));

  offset = dest.size();
  Serialize<int32_t>(-2147483648LL, &dest);
  EXPECT_EQ(-2147483648LL, Deserialize<int32_t>(dest.c_str(), offset));
}

TEST(SerializeTest, SerializeI8) {
  std::string dest;
  Serialize<int8_t>(0, &dest);
  EXPECT_EQ(std::string({'\x00'}), dest);

  dest.clear();
  Serialize<int8_t>(1, &dest);
  EXPECT_EQ(std::string({'\x01'}), dest);


  dest.clear();
  Serialize<int8_t>(-1, &dest);
  EXPECT_EQ(std::string({'\xff'}), dest);

  dest.clear();
  Serialize<int8_t>(127, &dest);
  EXPECT_EQ(std::string({'\x7f'}), dest);

  dest.clear();
  Serialize<int8_t>(-128, &dest);
  EXPECT_EQ(std::string({'\x80'}), dest);
}

TEST(SerializeTest, DeserializeI8) {
  std::string dest;
  std::size_t offset = dest.size();
  Serialize<int8_t>(0, &dest);
  EXPECT_EQ(0, Deserialize<int8_t>(dest.c_str(), offset));

  offset = dest.size();
  Serialize<int8_t>(1, &dest);
  EXPECT_EQ(1, Deserialize<int8_t>(dest.c_str(), offset));

  offset = dest.size();
  Serialize<int8_t>(-1, &dest);
  EXPECT_EQ(-1, Deserialize<int8_t>(dest.c_str(), offset));

  offset = dest.size();
  Serialize<int8_t>(127, &dest);
  EXPECT_EQ(127, Deserialize<int8_t>(dest.c_str(), offset));

  offset = dest.size();
  Serialize<int8_t>(-128, &dest);
  EXPECT_EQ(-128, Deserialize<int8_t>(dest.c_str(), offset));
}

} // namespace cassandra
} // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
