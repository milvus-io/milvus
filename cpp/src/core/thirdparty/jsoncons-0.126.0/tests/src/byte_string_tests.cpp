// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <new>
#include <catch/catch.hpp>

using namespace jsoncons;

// https://tools.ietf.org/html/rfc4648#section-4 test vectors

void check_encode_base64(const std::vector<uint8_t>& input, const std::string& expected)
{
    std::string result;
    encode_base64(input.begin(),input.end(),result);
    REQUIRE(result.size() == expected.size());
    for (size_t i = 0; i < result.size(); ++i)
    {
        CHECK(result[i] == expected[i]);
    }

    std::vector<uint8_t> output;
    decode_base64(result.begin(), result.end(), output);
    REQUIRE(output.size() == input.size());
    for (size_t i = 0; i < output.size(); ++i)
    {
        CHECK(output[i] == input[i]);
    }
}

void check_encode_base64url(const std::vector<uint8_t>& input, const std::string& expected)
{
    std::string result;
    encode_base64url(input.begin(),input.end(),result);
    REQUIRE(result.size() == expected.size());
    for (size_t i = 0; i < result.size(); ++i)
    {
        CHECK(result[i] == expected[i]);
    }

    std::vector<uint8_t> output; 
    decode_base64url(result.begin(), result.end(), output);
    REQUIRE(output.size() == input.size());
    for (size_t i = 0; i < output.size(); ++i)
    {
        CHECK(output[i] == input[i]);
    }
}

void check_encode_base16(const std::vector<uint8_t>& input, const std::string& expected)
{
    std::string result;
    encode_base16(input.begin(),input.end(),result);
    REQUIRE(result.size() == expected.size());
    for (size_t i = 0; i < result.size(); ++i)
    {
        CHECK(result[i] == expected[i]);
    }

    std::vector<uint8_t> output;
    decode_base16(result.begin(), result.end(), output);
    REQUIRE(output.size() == input.size());
    for (size_t i = 0; i < output.size(); ++i)
    {
        CHECK(output[i] == input[i]);
    }
}
TEST_CASE("test_base64_conversion")
{
    check_encode_base64({}, "");
    check_encode_base64({'f'}, "Zg==");
    check_encode_base64({'f','o'}, "Zm8=");
    check_encode_base64({'f','o','o'}, "Zm9v");
    check_encode_base64({'f','o','o','b'}, "Zm9vYg==");
    check_encode_base64({'f','o','o','b','a'}, "Zm9vYmE=");
    check_encode_base64({'f','o','o','b','a','r'}, "Zm9vYmFy");
}
 
TEST_CASE("test_base64url_conversion")
{
    check_encode_base64url({}, "");
    check_encode_base64url({'f'}, "Zg");
    check_encode_base64url({'f','o'}, "Zm8");
    check_encode_base64url({'f','o','o'}, "Zm9v");
    check_encode_base64url({'f','o','o','b'}, "Zm9vYg");
    check_encode_base64url({'f','o','o','b','a'}, "Zm9vYmE");
    check_encode_base64url({'f','o','o','b','a','r'}, "Zm9vYmFy");
}
 
TEST_CASE("test_base16_conversion")
{
    check_encode_base16({}, "");
    check_encode_base16({'f'}, "66");
    check_encode_base16({'f','o'}, "666F");
    check_encode_base16({'f','o','o'}, "666F6F");
    check_encode_base16({'f','o','o','b'}, "666F6F62");
    check_encode_base16({'f','o','o','b','a'}, "666F6F6261");
    check_encode_base16({'f','o','o','b','a','r'}, "666F6F626172");
}

