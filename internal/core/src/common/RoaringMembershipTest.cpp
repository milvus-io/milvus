// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <roaring/roaring64map.hh>
#include <roaring/memory.h>

#include <atomic>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/RoaringMembership.h"

namespace milvus {
namespace {

std::string
FromHex(std::string_view hex) {
    if (hex.size() % 2 != 0) {
        throw std::invalid_argument("hex input must contain whole bytes");
    }
    auto nibble = [](char value) -> uint8_t {
        if (value >= '0' && value <= '9') {
            return value - '0';
        }
        if (value >= 'a' && value <= 'f') {
            return value - 'a' + 10;
        }
        throw std::invalid_argument("invalid hex digit");
    };
    std::string bytes(hex.size() / 2, '\0');
    for (size_t i = 0; i < bytes.size(); ++i) {
        bytes[i] = static_cast<char>((nibble(hex[2 * i]) << 4) |
                                     nibble(hex[2 * i + 1]));
    }
    return bytes;
}

void
PutU16(std::string& out, size_t offset, uint16_t value) {
    out[offset] = static_cast<char>(value & 0xff);
    out[offset + 1] = static_cast<char>((value >> 8) & 0xff);
}

void
PutU64(std::string& out, size_t offset, uint64_t value) {
    for (int i = 0; i < 8; ++i) {
        out[offset + i] = static_cast<char>((value >> (8 * i)) & 0xff);
    }
}

void
AppendU16(std::string& out, uint16_t value) {
    out.push_back(static_cast<char>(value & 0xff));
    out.push_back(static_cast<char>((value >> 8) & 0xff));
}

void
AppendU32(std::string& out, uint32_t value) {
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<char>((value >> (8 * i)) & 0xff));
    }
}

void
AppendU64(std::string& out, uint64_t value) {
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<char>((value >> (8 * i)) & 0xff));
    }
}

struct PortableContainerFixture {
    uint16_t key;
    uint32_t cardinality;
    std::string payload;
};

std::string
ArrayPayload(std::initializer_list<uint16_t> values) {
    std::string payload;
    for (auto value : values) {
        AppendU16(payload, value);
    }
    return payload;
}

std::string
RunPayload(std::initializer_list<std::pair<uint16_t, uint16_t>> runs) {
    std::string payload;
    AppendU16(payload, static_cast<uint16_t>(runs.size()));
    for (const auto& [start, length] : runs) {
        AppendU16(payload, start);
        AppendU16(payload, length);
    }
    return payload;
}

std::string
BitmapPayload(uint32_t set_bit_count) {
    std::string payload(8192, '\0');
    for (uint32_t bit = 0; bit < set_bit_count; ++bit) {
        payload[bit / 8] =
            static_cast<char>(static_cast<uint8_t>(payload[bit / 8]) |
                              static_cast<uint8_t>(uint8_t{1} << (bit % 8)));
    }
    return payload;
}

std::string
BuildNoRunChild(
    const std::vector<PortableContainerFixture>& containers,
    std::optional<std::vector<uint32_t>> declared_offsets = std::nullopt) {
    constexpr uint32_t kNoRunCookie = 12346;
    std::string child;
    AppendU32(child, kNoRunCookie);
    AppendU32(child, static_cast<uint32_t>(containers.size()));
    for (const auto& container : containers) {
        AppendU16(child, container.key);
        AppendU16(child, static_cast<uint16_t>(container.cardinality - 1));
    }

    uint32_t cursor = static_cast<uint32_t>(8 + containers.size() * 8);
    for (size_t i = 0; i < containers.size(); ++i) {
        const auto offset =
            declared_offsets.has_value() ? declared_offsets->at(i) : cursor;
        AppendU32(child, offset);
        cursor += static_cast<uint32_t>(containers[i].payload.size());
    }
    for (const auto& container : containers) {
        child.append(container.payload);
    }
    return child;
}

std::string
BuildRunCookieChild(const std::vector<PortableContainerFixture>& containers,
                    uint8_t run_bitmap) {
    constexpr uint16_t kRunCookie = 12347;
    std::string child;
    const auto cookie = static_cast<uint32_t>(kRunCookie) |
                        (static_cast<uint32_t>(containers.size() - 1) << 16);
    AppendU32(child, cookie);
    child.push_back(static_cast<char>(run_bitmap));
    for (const auto& container : containers) {
        AppendU16(child, container.key);
        AppendU16(child, static_cast<uint16_t>(container.cardinality - 1));
    }

    if (containers.size() >= 4) {
        uint32_t cursor = static_cast<uint32_t>(5 + containers.size() * 8);
        for (const auto& container : containers) {
            AppendU32(child, cursor);
            cursor += static_cast<uint32_t>(container.payload.size());
        }
    }
    for (const auto& container : containers) {
        child.append(container.payload);
    }
    return child;
}

std::string
BuildRunChild(const PortableContainerFixture& container) {
    return BuildRunCookieChild({container}, 0x01);
}

std::string
BuildPortableRoaring64(
    const std::vector<std::pair<uint32_t, std::string>>& children) {
    std::string body;
    AppendU64(body, children.size());
    for (const auto& [high_key, child] : children) {
        AppendU32(body, high_key);
        body.append(child);
    }
    return body;
}

std::string
BuildCompactHighContainerBody(uint64_t count) {
    constexpr uint16_t kRunCookie = 12347;
    std::string child;
    child.reserve(11);
    AppendU16(child, kRunCookie);
    AppendU16(child, 0);
    child.push_back('\0');
    AppendU16(child, 0);
    AppendU16(child, 0);
    AppendU16(child, 0);

    std::string body;
    body.reserve(8 + static_cast<size_t>(count) * 15);
    AppendU64(body, count);
    for (uint64_t i = 0; i < count; ++i) {
        AppendU32(body, static_cast<uint32_t>(i));
        body.append(child);
    }
    return body;
}

std::string
BuildManySingletonLowContainersChild(uint32_t count) {
    constexpr uint32_t kNoRunCookie = 12346;
    std::string child;
    child.reserve(8 + static_cast<size_t>(count) * 10);
    AppendU32(child, kNoRunCookie);
    AppendU32(child, count);
    for (uint32_t i = 0; i < count; ++i) {
        AppendU16(child, static_cast<uint16_t>(i));
        AppendU16(child, 0);
    }
    const auto payload_start = 8 + count * 8;
    for (uint32_t i = 0; i < count; ++i) {
        AppendU32(child, payload_start + i * 2);
    }
    for (uint32_t i = 0; i < count; ++i) {
        AppendU16(child, 0);
    }
    return child;
}

std::string
WrapMrb1(std::string body, uint64_t declared_cardinality) {
    std::string blob(RoaringMembership::kHeaderSize, '\0');
    std::memcpy(blob.data(), "MRB1", 4);
    PutU16(blob, 4, RoaringMembership::kVersion);
    PutU16(blob, 6, RoaringMembership::kFormatPortableRoaring64);
    PutU64(blob, 8, declared_cardinality);
    PutU64(blob, 16, body.size());
    PutU64(blob, 24, 0);
    blob.append(body);
    return blob;
}

std::string
BuildMrb1(const std::vector<int64_t>& values) {
    roaring::Roaring64Map bitmap;
    for (auto value : values) {
        bitmap.add(static_cast<uint64_t>(value));
    }
    bitmap.runOptimize();
    std::string body(bitmap.getSizeInBytes(true), '\0');
    EXPECT_EQ(bitmap.write(body.data(), true), body.size());

    std::string blob(RoaringMembership::kHeaderSize + body.size(), '\0');
    std::memcpy(blob.data(), "MRB1", 4);
    PutU16(blob, 4, RoaringMembership::kVersion);
    PutU16(blob, 6, RoaringMembership::kFormatPortableRoaring64);
    PutU64(blob, 8, bitmap.cardinality());
    PutU64(blob, 16, body.size());
    PutU64(blob, 24, 0);
    std::memcpy(
        blob.data() + RoaringMembership::kHeaderSize, body.data(), body.size());
    return blob;
}

ErrorCode
CatchCode(const std::function<void()>& fn) {
    try {
        fn();
    } catch (const SegcoreError& error) {
        return error.get_error_code();
    }
    return ErrorCode::Success;
}

TEST(RoaringMembershipTest, ParseAndContainsSignedBoundaries) {
    const std::vector<int64_t> values = {std::numeric_limits<int64_t>::min(),
                                         -1,
                                         0,
                                         1,
                                         42,
                                         std::numeric_limits<int64_t>::max(),
                                         42};
    const auto blob = BuildMrb1(values);
    auto membership = RoaringMembership::Parse(blob);
    ASSERT_NE(membership, nullptr);
    EXPECT_EQ(membership->cardinality(), 6);
    EXPECT_EQ(membership->serialized_size(),
              blob.size() - RoaringMembership::kHeaderSize);
    for (auto value : values) {
        EXPECT_TRUE(membership->Contains(value)) << value;
    }
    EXPECT_FALSE(membership->Contains(2));
}

TEST(RoaringMembershipTest, ParsesGoGeneratedPortableFixture) {
    // Generated by client/v3/membership/roaringfilter.Build in Go from:
    // MinInt64, -1, 0, 1, 42, MaxInt64. Keeping the bytes here makes the
    // cross-language compatibility check independent of the C++ writer.
    const auto blob = FromHex(
        "4d52423101000100060000000000000064000000000000000000000000000000"
        "0400000000000000000000003a30000001000000000002001000000000000100"
        "2a00ffffff7f3a30000001000000ffff000010000000ffff000000803a30000001"
        "00000000000000100000000000ffffffff3a30000001000000ffff000010000000"
        "ffff");
    auto membership = RoaringMembership::Parse(blob);
    EXPECT_EQ(membership->cardinality(), 6);
    EXPECT_EQ(membership->serialized_size(),
              blob.size() - RoaringMembership::kHeaderSize);
    EXPECT_TRUE(membership->Contains(std::numeric_limits<int64_t>::min()));
    EXPECT_TRUE(membership->Contains(-1));
    EXPECT_TRUE(membership->Contains(0));
    EXPECT_TRUE(membership->Contains(1));
    EXPECT_TRUE(membership->Contains(42));
    EXPECT_TRUE(membership->Contains(std::numeric_limits<int64_t>::max()));
    EXPECT_FALSE(membership->Contains(2));
}

// The fixtures below are the other half of the cross-language contract: Go
// writes an MRB1 blob and C++ parses it. pkg/util/roaringfilter carries the
// mirror set, blobs written by CRoaring's portable writer and parsed by Go.
// Each container shape the portable format can produce is covered once in each
// direction; the signed-boundary fixture above also carries an array container
// and multiple high-32 keys.
//
// Every one of these was byte-identical to the CRoaring-written blob for the
// same values when it was generated, which is the property the pair of suites
// exists to keep true. Regenerate with client/v3/membership/roaringfilter.Build over the
// listed values and hex-encode the result.
TEST(RoaringMembershipTest, ParsesGoGeneratedEmptyFixture) {
    // Generated by client/v3/membership/roaringfilter.Build in Go from: no values.
    const auto blob = FromHex(
        "4d52423101000100000000000000000008000000000000000000000000000000"
        "0000000000000000");
    auto membership = RoaringMembership::Parse(blob);
    EXPECT_EQ(membership->cardinality(), 0);
    EXPECT_EQ(membership->serialized_size(),
              blob.size() - RoaringMembership::kHeaderSize);
    EXPECT_FALSE(membership->Contains(0));
    EXPECT_FALSE(membership->Contains(42));
    EXPECT_FALSE(membership->Contains(std::numeric_limits<int64_t>::min()));
}

TEST(RoaringMembershipTest, ParsesGoGeneratedRunFixture) {
    // Generated by client/v3/membership/roaringfilter.Build in Go from: 0..299, which
    // Build's RunOptimize turns into a single run container.
    const auto blob = FromHex(
        "4d524231010001002c010000000000001b000000000000000000000000000000"
        "0100000000000000000000003b3000000100002b01010000002b01");
    auto membership = RoaringMembership::Parse(blob);
    EXPECT_EQ(membership->cardinality(), 300);
    EXPECT_EQ(membership->serialized_size(),
              blob.size() - RoaringMembership::kHeaderSize);
    EXPECT_TRUE(membership->Contains(0));
    EXPECT_TRUE(membership->Contains(150));
    EXPECT_TRUE(membership->Contains(299));
    EXPECT_FALSE(membership->Contains(300));
    EXPECT_FALSE(membership->Contains(-1));
}

TEST(RoaringMembershipTest, ParsesGoGeneratedBitmapFixture) {
    // Generated by client/v3/membership/roaringfilter.Build in Go from: the 5000 even
    // values 0, 2, ... 9998, dense enough to be stored as bitmap containers.
    const auto blob = FromHex(
        "4d5242310100010088130000000000001c200000000000000000000000000000"
        "0100000000000000000000003a30000001000000000087131000000055555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555555555"
        "5555555555555555555555555555555555555555555555555555555555550000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "00000000000000000000000000000000000000000000000000000000");
    auto membership = RoaringMembership::Parse(blob);
    EXPECT_EQ(membership->cardinality(), 5000);
    EXPECT_EQ(membership->serialized_size(),
              blob.size() - RoaringMembership::kHeaderSize);
    EXPECT_TRUE(membership->Contains(0));
    EXPECT_TRUE(membership->Contains(4098));
    EXPECT_TRUE(membership->Contains(9998));
    EXPECT_FALSE(membership->Contains(1));
    EXPECT_FALSE(membership->Contains(9999));
    EXPECT_FALSE(membership->Contains(10000));
}

TEST(RoaringMembershipTest, RejectsMalformedEnvelopeAndBody) {
    const auto good = BuildMrb1({-1, 0, 1});
    ASSERT_NO_THROW(RoaringMembership::Parse(good));

    auto expect_rejected = [](std::string blob, const char* label) {
        EXPECT_EQ(CatchCode([&] { RoaringMembership::Parse(blob); }),
                  ErrorCode::ExprInvalid)
            << label;
    };

    expect_rejected("MRB1", "short header");
    {
        auto blob = good;
        blob[0] = 'X';
        expect_rejected(std::move(blob), "magic");
    }
    {
        auto blob = good;
        PutU16(blob, 4, 2);
        expect_rejected(std::move(blob), "version");
    }
    {
        auto blob = good;
        PutU16(blob, 6, 2);
        expect_rejected(std::move(blob), "format");
    }
    {
        auto blob = good;
        PutU64(blob, 24, 1);
        expect_rejected(std::move(blob), "reserved");
    }
    expect_rejected(good.substr(0, good.size() - 1),
                    "declared body longer than envelope");
    expect_rejected(good + "x", "declared body shorter than envelope");
    {
        auto blob = good.substr(0, good.size() - 1);
        PutU64(blob, 16, blob.size() - RoaringMembership::kHeaderSize);
        expect_rejected(std::move(blob), "truncated portable body");
    }
    {
        auto blob = good + "x";
        PutU64(blob, 16, blob.size() - RoaringMembership::kHeaderSize);
        expect_rejected(std::move(blob), "trailing portable body");
    }
    {
        auto blob = good;
        PutU64(blob, 16, RoaringMembership::kMaxBodySize + 1);
        expect_rejected(std::move(blob), "body exceeds maximum");
    }
    {
        auto blob = good;
        PutU64(blob, 8, 4);
        expect_rejected(std::move(blob), "cardinality");
    }
}

TEST(RoaringMembershipTest, RejectsDecodedResourceAmplification) {
    const auto parse_error = [](const std::string& blob) {
        try {
            RoaringMembership::Parse(blob);
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), ErrorCode::ExprInvalid);
            return std::string(error.what());
        }
        ADD_FAILURE() << "resource-amplifying MRB1 blob must be rejected";
        return std::string{};
    };

    const auto high_error =
        parse_error(WrapMrb1(BuildCompactHighContainerBody(
                                 RoaringMembership::kMaxHighContainerCount + 1),
                             RoaringMembership::kMaxHighContainerCount + 1));
    EXPECT_NE(high_error.find("high-container count"), std::string::npos)
        << high_error;

    const auto child = BuildManySingletonLowContainersChild(uint32_t{1} << 16);
    std::vector<std::pair<uint32_t, std::string>> children;
    children.reserve(16);
    for (uint32_t high = 0; high < 16; ++high) {
        children.emplace_back(high, child);
    }
    const auto decoded_error = parse_error(WrapMrb1(
        BuildPortableRoaring64(children), uint64_t{16} * (uint64_t{1} << 16)));
    EXPECT_NE(decoded_error.find("estimated decoded size"), std::string::npos)
        << decoded_error;
}

TEST(RoaringMembershipTest, ValidateReportsDecodedResourceShape) {
    constexpr uint64_t kHighContainers = 20'000;
    const auto blob = WrapMrb1(BuildCompactHighContainerBody(kHighContainers),
                               kHighContainers);
    const auto summary = RoaringMembership::Validate(blob);

    EXPECT_EQ(summary.cardinality, kHighContainers);
    EXPECT_EQ(summary.high_container_count, kHighContainers);
    EXPECT_EQ(summary.low_container_count, kHighContainers);
    EXPECT_EQ(summary.body_size, blob.size() - RoaringMembership::kHeaderSize);
    EXPECT_EQ(summary.estimated_decoded_bytes,
              summary.body_size +
                  kHighContainers *
                      RoaringMembership::kEstimatedHighContainerOverheadBytes +
                  kHighContainers *
                      RoaringMembership::kEstimatedLowContainerOverheadBytes);
}

TEST(RoaringMembershipTest, RejectsStructurallyMalformedPortableBodies) {
    const auto one_value_child = [](uint16_t value) {
        return BuildNoRunChild({{0, 1, ArrayPayload({value})}});
    };

    const auto one_bit_bitmap = BitmapPayload(1);
    const auto bitmap_with_4098_bits = BitmapPayload(4098);
    std::string zero_container_child;
    AppendU32(zero_container_child, 12346);
    AppendU32(zero_container_child, 0);

    std::string truncated_descriptors;
    AppendU32(truncated_descriptors, 12346);
    AppendU32(truncated_descriptors, 2);
    AppendU16(truncated_descriptors, 0);
    AppendU16(truncated_descriptors, 0);

    std::string truncated_offsets;
    AppendU32(truncated_offsets, 12346);
    AppendU32(truncated_offsets, 1);
    AppendU16(truncated_offsets, 0);
    AppendU16(truncated_offsets, 0);
    truncated_offsets.append(3, '\0');

    auto truncated_array = BuildNoRunChild({{0, 2, ArrayPayload({1, 2})}});
    truncated_array.pop_back();

    struct MalformedCase {
        const char* name;
        std::string body;
        uint64_t declared_cardinality;
    };
    const std::vector<MalformedCase> cases = {
        {"truncated high-container count", std::string(7, '\0'), 0},
        {"out-of-order high keys",
         BuildPortableRoaring64(
             {{2, one_value_child(7)}, {1, one_value_child(8)}}),
         2},
        {"duplicate high keys",
         BuildPortableRoaring64(
             {{1, one_value_child(7)}, {1, one_value_child(8)}}),
         2},
        {"duplicate child keys",
         BuildPortableRoaring64(
             {{0,
               BuildNoRunChild(
                   {{1, 1, ArrayPayload({7})}, {1, 1, ArrayPayload({8})}})}}),
         2},
        {"out-of-order child keys",
         BuildPortableRoaring64(
             {{0,
               BuildNoRunChild(
                   {{2, 1, ArrayPayload({7})}, {1, 1, ArrayPayload({8})}})}}),
         2},
        {"truncated descriptor table",
         BuildPortableRoaring64({{0, truncated_descriptors}}),
         2},
        {"truncated offset table",
         BuildPortableRoaring64({{0, truncated_offsets}}),
         1},
        {"unsorted array values",
         BuildPortableRoaring64(
             {{0, BuildNoRunChild({{0, 2, ArrayPayload({2, 1})}})}}),
         2},
        {"duplicate array values",
         BuildPortableRoaring64(
             {{0, BuildNoRunChild({{0, 2, ArrayPayload({1, 1})}})}}),
         2},
        {"bitmap popcount does not match descriptor",
         BuildPortableRoaring64(
             {{0, BuildNoRunChild({{0, 4097, one_bit_bitmap}})}}),
         4097},
        {"bitmap popcount exceeds descriptor",
         BuildPortableRoaring64(
             {{0, BuildNoRunChild({{0, 4097, bitmap_with_4098_bits}})}}),
         4097},
        {"zero run count",
         BuildPortableRoaring64({{0, BuildRunChild({0, 1, RunPayload({})})}}),
         0},
        {"run endpoint overflow",
         BuildPortableRoaring64(
             {{0, BuildRunChild({0, 2, RunPayload({{65535, 1}})})}}),
         2},
        {"overlapping runs",
         BuildPortableRoaring64(
             {{0, BuildRunChild({0, 4, RunPayload({{0, 2}, {2, 0}})})}}),
         4},
        {"descending runs",
         BuildPortableRoaring64(
             {{0, BuildRunChild({0, 2, RunPayload({{10, 0}, {1, 0}})})}}),
         2},
        {"run descriptor cardinality exceeds actual",
         BuildPortableRoaring64(
             {{0, BuildRunChild({0, 3, RunPayload({{0, 1}})})}}),
         2},
        {"truncated child payload",
         BuildPortableRoaring64({{0, truncated_array}}),
         2},
        {"trailing child data",
         BuildPortableRoaring64({{0, one_value_child(7) + "x"}}),
         1},
        {"run descriptor cardinality mismatch",
         BuildPortableRoaring64(
             {{0, BuildRunChild({0, 1, RunPayload({{0, 1}})})}}),
         2},
    };

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        const auto blob =
            WrapMrb1(test_case.body, test_case.declared_cardinality);
        EXPECT_EQ(CatchCode([&] { RoaringMembership::Parse(blob); }),
                  ErrorCode::ExprInvalid);
    }
}

TEST(RoaringMembershipTest, ValidationErrorsRedactMemberKeyPrefixes) {
    const auto one_value_child = [](uint16_t value) {
        return BuildNoRunChild({{0, 1, ArrayPayload({value})}});
    };
    const auto parse_error = [](const std::string& blob) {
        try {
            RoaringMembership::Parse(blob);
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), ErrorCode::ExprInvalid);
            return std::string(error.what());
        }
        ADD_FAILURE() << "malformed MRB1 blob must be rejected";
        return std::string{};
    };

    constexpr uint32_t first_high = 4000000001U;
    constexpr uint32_t second_high = 3000000007U;
    const auto high_error = parse_error(
        WrapMrb1(BuildPortableRoaring64({{first_high, one_value_child(11)},
                                         {second_high, one_value_child(22)}}),
                 2));
    EXPECT_EQ(high_error.find(std::to_string(first_high)), std::string::npos)
        << high_error;
    EXPECT_EQ(high_error.find(std::to_string(second_high)), std::string::npos)
        << high_error;
    EXPECT_NE(high_error.find("high container 1"), std::string::npos)
        << high_error;

    constexpr uint16_t first_child = 65000;
    constexpr uint16_t second_child = 64000;
    const auto child_error = parse_error(WrapMrb1(
        BuildPortableRoaring64(
            {{0,
              BuildNoRunChild({{first_child, 1, ArrayPayload({1})},
                               {second_child, 1, ArrayPayload({2})}})}}),
        2));
    EXPECT_EQ(child_error.find(std::to_string(first_child)), std::string::npos)
        << child_error;
    EXPECT_EQ(child_error.find(std::to_string(second_child)), std::string::npos)
        << child_error;
    EXPECT_NE(child_error.find("Roaring32 container 1"), std::string::npos)
        << child_error;
}

TEST(RoaringMembershipTest, RejectsUnsupportedRoaring32CookieAfterPrefixBound) {
    std::string unsupported_cookie_child;
    AppendU32(unsupported_cookie_child, 0xdeadbeef);
    // A non-empty high entry needs at least 4 high-key bytes plus an 11-byte
    // child. Pad the invalid cookie to that minimum so the top-level
    // body-derived count bound passes and the cookie branch is exercised.
    unsupported_cookie_child.append(7, '\0');
    const auto blob =
        WrapMrb1(BuildPortableRoaring64({{0, unsupported_cookie_child}}), 0);

    try {
        RoaringMembership::Parse(blob);
        FAIL() << "unsupported Roaring32 cookie must be rejected";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::ExprInvalid);
        const std::string message = error.what();
        EXPECT_NE(message.find("unsupported Roaring32 cookie"),
                  std::string::npos)
            << message;
        EXPECT_EQ(message.find("high-container count"), std::string::npos)
            << message;
    }
}

TEST(RoaringMembershipTest, AcceptsValidPortableContainerEncodings) {
    const std::vector<std::string> bodies = {
        BuildPortableRoaring64({}),
        BuildPortableRoaring64(
            {{0, BuildNoRunChild({{0, 3, ArrayPayload({1, 2, 65535})}})}}),
        BuildPortableRoaring64(
            {{0, BuildNoRunChild({{0, 4097, BitmapPayload(4097)}})}}),
        BuildPortableRoaring64(
            {{0,
              BuildRunChild(
                  {0, 3, RunPayload({{0, 0}, {2, 0}, {65535, 0}})})}}),
        // A run cookie with fewer than four containers has no offset table.
        BuildPortableRoaring64(
            {{0,
              BuildRunCookieChild({{0, 1, ArrayPayload({1})},
                                   {1, 1, ArrayPayload({2})},
                                   {2, 1, ArrayPayload({3})}},
                                  0)}}),
        // At four containers the run-cookie offset table is mandatory.
        BuildPortableRoaring64(
            {{0,
              BuildRunCookieChild({{0, 1, ArrayPayload({1})},
                                   {1, 1, ArrayPayload({2})},
                                   {2, 1, ArrayPayload({3})},
                                   {3, 1, ArrayPayload({4})}},
                                  0)}}),
        // Legal compact child: one non-run array under a run cookie with an
        // all-zero run bitmap. Its wire size is 11 bytes, while CRoaring
        // normalizes it to an 18-byte no-run encoding after decode.
        BuildPortableRoaring64(
            {{0, BuildRunCookieChild({{0, 1, ArrayPayload({42})}}, 0)}}),
        // The compact child must not shift the following high key by its
        // normalized 18-byte decoded size.
        BuildPortableRoaring64(
            {{0, BuildRunCookieChild({{0, 1, ArrayPayload({42})}}, 0)},
             {1, BuildNoRunChild({{0, 1, ArrayPayload({7})}})}}),
    };
    const std::vector<uint64_t> cardinalities = {0, 3, 4097, 3, 3, 4, 1, 2};

    ASSERT_EQ(bodies.size(), cardinalities.size());
    for (size_t i = 0; i < bodies.size(); ++i) {
        SCOPED_TRACE(i);
        auto membership =
            RoaringMembership::Parse(WrapMrb1(bodies[i], cardinalities[i]));
        ASSERT_NE(membership, nullptr);
        EXPECT_EQ(membership->cardinality(), cardinalities[i]);
        EXPECT_EQ(membership->serialized_size(), bodies[i].size());
    }

    auto separated_runs = RoaringMembership::Parse(
        WrapMrb1(BuildPortableRoaring64(
                     {{0,
                       BuildRunChild(
                           {0, 3, RunPayload({{0, 0}, {2, 0}, {65535, 0}})})}}),
                 3));
    EXPECT_TRUE(separated_runs->Contains(0));
    EXPECT_FALSE(separated_runs->Contains(1));
    EXPECT_TRUE(separated_runs->Contains(2));
    EXPECT_TRUE(separated_runs->Contains(65535));

    auto compact_then_second_high = RoaringMembership::Parse(
        WrapMrb1(BuildPortableRoaring64(
                     {{0, BuildRunCookieChild({{0, 1, ArrayPayload({42})}}, 0)},
                      {1, BuildNoRunChild({{0, 1, ArrayPayload({7})}})}}),
                 2));
    EXPECT_TRUE(compact_then_second_high->Contains(42));
    EXPECT_TRUE(compact_then_second_high->Contains(
        static_cast<int64_t>((uint64_t{1} << 32) | 7)));
}

}  // namespace

// A blob that Milvus already validated byte-for-byte cannot fail decoding
// because of its contents. CRoaring funnels malformed input and allocation
// failure alike through the same runtime_error("failed alloc while reading"),
// so the cause cannot be established here -- but the blame can: this is not
// the caller's fault, and classifying it as an input error would cost replica
// failover.
namespace {

std::atomic<bool> g_fail_roaring_alloc{false};

void*
FailingMalloc(size_t size) {
    if (g_fail_roaring_alloc.load(std::memory_order_relaxed)) {
        return nullptr;
    }
    return std::malloc(size);
}

void*
FailingRealloc(void* p, size_t size) {
    if (g_fail_roaring_alloc.load(std::memory_order_relaxed)) {
        return nullptr;
    }
    return std::realloc(p, size);
}

void*
FailingCalloc(size_t n, size_t size) {
    if (g_fail_roaring_alloc.load(std::memory_order_relaxed)) {
        return nullptr;
    }
    return std::calloc(n, size);
}

void
PassthroughFree(void* p) {
    std::free(p);
}

void*
FailingAlignedMalloc(size_t alignment, size_t size) {
    if (g_fail_roaring_alloc.load(std::memory_order_relaxed)) {
        return nullptr;
    }
    void* p = nullptr;
    if (posix_memalign(&p, alignment, size) != 0) {
        return nullptr;
    }
    return p;
}

void
PassthroughAlignedFree(void* p) {
    std::free(p);
}

}  // namespace

TEST(RoaringMembershipTest, AllocationFailureIsNotInputError) {
    const auto blob = BuildMrb1({-9223372036854775807L - 1, -1, 0, 1, 42});
    // Sanity: the blob is good when the allocator behaves.
    ASSERT_NO_THROW(RoaringMembership::Parse(blob));

    roaring_memory_t hooks{FailingMalloc,
                           FailingRealloc,
                           FailingCalloc,
                           PassthroughFree,
                           FailingAlignedMalloc,
                           PassthroughAlignedFree};
    roaring_init_memory_hook(hooks);
    g_fail_roaring_alloc.store(true);

    ErrorCode observed = ErrorCode::Success;
    try {
        RoaringMembership::Parse(blob);
        ADD_FAILURE() << "decode must fail while the allocator refuses";
    } catch (const SegcoreError& error) {
        observed = error.get_error_code();
    }

    g_fail_roaring_alloc.store(false);
    roaring_memory_t restore{std::malloc,
                             std::realloc,
                             std::calloc,
                             PassthroughFree,
                             FailingAlignedMalloc,
                             PassthroughAlignedFree};
    roaring_init_memory_hook(restore);

    // The point is that it is NOT ExprInvalid: 2028 maps to a Go InputError,
    // which makes the proxy blame the client and skip replica failover.
    // UnexpectedError stays a generic system error, so the request can still
    // be retried elsewhere.
    EXPECT_NE(observed, ErrorCode::ExprInvalid);
    EXPECT_EQ(observed, ErrorCode::UnexpectedError);

    // The allocator is healthy again: no state was corrupted by the injection.
    EXPECT_NO_THROW(RoaringMembership::Parse(blob));
}

// Malformed bytes must keep their input classification: the reclassification
// above must not blanket-convert genuine bad requests into retriable errors.
TEST(RoaringMembershipTest, MalformedBodyStaysExprInvalid) {
    auto blob = BuildMrb1({1, 2, 3});
    ASSERT_GT(blob.size(), RoaringMembership::kHeaderSize + 8);
    blob[blob.size() - 1] ^= 0xFF;
    blob[blob.size() - 2] ^= 0xFF;
    blob[RoaringMembership::kHeaderSize + 4] ^= 0xFF;

    try {
        RoaringMembership::Parse(blob);
        // Some corruptions are still structurally legal; that is fine.
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::ExprInvalid)
            << "corrupt input must not be reported as a resource failure";
    }
}

TEST(RoaringMembershipTest, IgnoresOffsetTableContents) {
    // The offset table is redundant with the container layout, and both
    // roaring/v2 and CRoaring skip its contents on read, so a body whose
    // declared offsets disagree with the positions we compute must still be
    // accepted.
    const auto wrong_offsets = WrapMrb1(
        BuildPortableRoaring64({{0,
                                 BuildNoRunChild({{0, 1, ArrayPayload({42})}},
                                                 std::vector<uint32_t>{0})}}),
        1);
    auto with_wrong_offsets = RoaringMembership::Parse(wrong_offsets);
    ASSERT_NE(with_wrong_offsets, nullptr);
    EXPECT_EQ(with_wrong_offsets->cardinality(), 1u);
    EXPECT_TRUE(with_wrong_offsets->Contains(42));
}

TEST(RoaringMembershipTest, AcceptsUnspecifiedRunBitmapPaddingBits) {
    // The run bitmap is (container_count + 7) / 8 bytes and only its first
    // container_count bits are defined. 0x81 sets bit 0 (the single container
    // is a run container) and bit 7, which is padding. roaring/v2 and CRoaring
    // both ignore it, so treating it as malformed would reject a body those
    // writers can produce. Mirrors the Go fixture of the same name.
    const auto blob = WrapMrb1(
        BuildPortableRoaring64(
            {{0, BuildRunCookieChild({{0, 1, RunPayload({{0, 0}})}}, 0x81)}}),
        1);

    auto membership = RoaringMembership::Parse(blob);
    ASSERT_NE(membership, nullptr);
    EXPECT_EQ(membership->cardinality(), 1u);
    EXPECT_TRUE(membership->Contains(0));
    EXPECT_FALSE(membership->Contains(1));
}

TEST(RoaringMembershipTest, AcceptsEmptyRoaring32Child) {
    // CRoaring can write a high entry whose Roaring32 child holds zero
    // containers: a 4-byte high key plus the no-run cookie and a container
    // count of zero. Both CRoaring and roaring/v2 consume the resulting body as
    // an empty set, so the validators must admit it. Mirrors the Go fixture of
    // the same name.
    std::string empty_child;
    AppendU32(empty_child, 12346);  // no-run cookie
    AppendU32(empty_child, 0);      // zero containers
    const auto blob = WrapMrb1(BuildPortableRoaring64({{0, empty_child}}), 0);

    auto membership = RoaringMembership::Parse(blob);
    ASSERT_NE(membership, nullptr);
    EXPECT_EQ(membership->cardinality(), 0u);
    EXPECT_FALSE(membership->Contains(0));
}

TEST(RoaringMembershipTest, AcceptsAdjacentRunIntervals) {
    // (start 0, len 0) then (start 1, len 0) encodes {0, 1} without coalescing
    // the two runs. The portable format allows it; only overlap and descending
    // order are invalid. Mirrors the Go validator fixture of the same name.
    const auto body = BuildPortableRoaring64(
        {{0, BuildRunChild({0, 2, RunPayload({{0, 0}, {1, 0}})})}});
    const auto blob = WrapMrb1(body, 2);

    auto membership = RoaringMembership::Parse(blob);
    ASSERT_NE(membership, nullptr);
    EXPECT_EQ(membership->cardinality(), 2u);
    EXPECT_TRUE(membership->Contains(0));
    EXPECT_TRUE(membership->Contains(1));
    EXPECT_FALSE(membership->Contains(2));
}

}  // namespace milvus
