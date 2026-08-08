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

// Tests for the canonical Decimal wire encoding (common/Decimal.h): the signed
// unscaled integer as exactly 8 bytes, little-endian, two's complement.
//
// Golden-vector conformance consumes the same fixture as the Go tests in
// pkg/util/parameterutil, so both sides are pinned to byte-identical output —
// a divergence here means Go and C++ would silently exchange different values
// for the same decimal literal.

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include "common/Decimal.h"

using namespace milvus;

namespace {

// Co-located with this test (internal/core/unittest/testdata/decimal) so the
// C++ unittest environment can always find it. The authoritative copy
// consumed by the Go tests is
// pkg/util/parameterutil/testdata/decimal_golden_vectors.json; the two files
// are identical and must be kept in sync.
std::filesystem::path
GoldenVectorsPath() {
    auto path =
        std::filesystem::path(__FILE__).parent_path()  // internal/core/unittest
        / "testdata" / "decimal" / "golden_vectors.json";
    if (!std::filesystem::exists(path)) {
        if (const char* env = std::getenv("MILVUS_DECIMAL_GOLDEN_VECTORS")) {
            path = env;
        }
    }
    return path;
}

std::string
HexDecode(const std::string& hex) {
    EXPECT_EQ(hex.size() % 2, 0);
    auto nibble = [](char c) -> int {
        if (c >= '0' && c <= '9')
            return c - '0';
        if (c >= 'a' && c <= 'f')
            return c - 'a' + 10;
        if (c >= 'A' && c <= 'F')
            return c - 'A' + 10;
        ADD_FAILURE() << "invalid hex char: " << c;
        return 0;
    };
    std::string out;
    out.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2) {
        out.push_back(
            static_cast<char>((nibble(hex[i]) << 4) | nibble(hex[i + 1])));
    }
    return out;
}

}  // namespace

TEST(DecimalWireFormat, EncodeDecodeRoundTrip) {
    const int64_t values[] = {0,
                              1,
                              -1,
                              199900,
                              -199900,
                              12345,
                              999999999999999999LL,
                              -999999999999999999LL};
    for (auto value : values) {
        auto bytes = EncodeDecimalBytes(value);
        ASSERT_EQ(bytes.size(), kDecimalBytesLen)
            << "wire encoding must be fixed-width";
        EXPECT_EQ(DecodeDecimalBytes(bytes), value);
    }
}

TEST(DecimalWireFormat, EncodingIsLittleEndian) {
    // 1 must land in the first byte, not the last: this is the single
    // assertion that would fail if the encoding were ever flipped to
    // big-endian, which the proto contract explicitly forbids.
    auto one = EncodeDecimalBytes(1);
    ASSERT_EQ(one.size(), kDecimalBytesLen);
    EXPECT_EQ(static_cast<unsigned char>(one[0]), 0x01);
    for (size_t i = 1; i < kDecimalBytesLen; ++i) {
        EXPECT_EQ(static_cast<unsigned char>(one[i]), 0x00);
    }

    // -1 is all ones in two's complement.
    auto minus_one = EncodeDecimalBytes(-1);
    for (size_t i = 0; i < kDecimalBytesLen; ++i) {
        EXPECT_EQ(static_cast<unsigned char>(minus_one[i]), 0xFF);
    }
}

TEST(DecimalWireFormat, GoldenVectorsMatchGo) {
    auto path = GoldenVectorsPath();
    ASSERT_TRUE(std::filesystem::exists(path))
        << "golden vectors not found at " << path;

    std::ifstream input(path);
    ASSERT_TRUE(input.is_open());
    nlohmann::json fixture;
    input >> fixture;

    const auto& cases = fixture.at("cases");
    ASSERT_FALSE(cases.empty());

    for (const auto& entry : cases) {
        const auto name = entry.at("name").get<std::string>();
        const auto unscaled = entry.at("unscaled").get<int64_t>();
        const auto expected_bytes =
            HexDecode(entry.at("bytes_le_hex").get<std::string>());

        SCOPED_TRACE(name);
        EXPECT_EQ(EncodeDecimalBytes(unscaled), expected_bytes);
        EXPECT_EQ(DecodeDecimalBytes(expected_bytes), unscaled);
    }
}
