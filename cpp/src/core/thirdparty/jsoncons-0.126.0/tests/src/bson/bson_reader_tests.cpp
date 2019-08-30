// Copyright 2016 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons_ext/bson/bson.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <limits>
#include <catch/catch.hpp>

using namespace jsoncons;

void check_decode_bson(const std::vector<uint8_t>& v, const json& expected)
{
    json result = bson::decode_bson<json>(v);
    REQUIRE(result == expected);

    std::string s;
    for (auto c : v)
    {
        s.push_back(c);
    }
    std::istringstream is(s);
    json j2 = bson::decode_bson<json>(is);
    REQUIRE(j2 == expected);
}

TEST_CASE("bson hello world")
{
    check_decode_bson({0x16,0x00,0x00,0x00, // total document size
                       0x02, // string
                       'h','e','l','l','o', 0x00, // field name 
                       0x06,0x00,0x00,0x00, // size of value
                       'w','o','r','l','d',0x00, // field value and null terminator
                       0x00 // end of document
                      },json::parse("{\"hello\":\"world\"}"));
}

