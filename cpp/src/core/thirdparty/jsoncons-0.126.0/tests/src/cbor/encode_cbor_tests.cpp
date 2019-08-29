// Copyright 2016 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <limits>
#include <catch/catch.hpp>

using namespace jsoncons;

// test vectors from tinycbor https://github.com/01org/tinycbor tst_encoder.cpp
// MIT license

void check_encode_cbor(const std::vector<uint8_t>& expected, const json& j)
{
    std::vector<uint8_t> result;
    cbor::encode_cbor(j,result);
    REQUIRE(result.size() == expected.size());
    for (size_t i = 0; i < expected.size(); ++i)
    {
        REQUIRE(result[i] == expected[i]);
    }
}

TEST_CASE("cbor_encoder_test")
{
    // unsigned integer
    check_encode_cbor({0x00},json(0U));
    check_encode_cbor({0x01},json(1U));
    check_encode_cbor({0x0a},json(10U));
    check_encode_cbor({0x17},json(23U));
    check_encode_cbor({0x18,0x18},json(24U));
    check_encode_cbor({0x18,0xff},json(255U));
    check_encode_cbor({0x19,0x01,0x00},json(256U));
    check_encode_cbor({0x19,0xff,0xff},json(65535U));
    check_encode_cbor({0x1a,0,1,0x00,0x00},json(65536U));
    check_encode_cbor({0x1a,0xff,0xff,0xff,0xff},json(4294967295U));
    check_encode_cbor({0x1b,0,0,0,1,0,0,0,0},json(4294967296U));
    check_encode_cbor({0x1b,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff},json((std::numeric_limits<uint64_t>::max)()));

    // positive signed integer
    check_encode_cbor({0x00},json(0));
    check_encode_cbor({0x01},json(1));
    check_encode_cbor({0x0a},json(10));
    check_encode_cbor({0x17},json(23));
    check_encode_cbor({0x18,0x18},json(24));
    check_encode_cbor({0x18,0xff},json(255));
    check_encode_cbor({0x19,0x01,0x00},json(256));
    check_encode_cbor({0x19,0xff,0xff},json(65535));
    check_encode_cbor({0x1a,0,1,0x00,0x00},json(65536));
    check_encode_cbor({0x1a,0xff,0xff,0xff,0xff},json(4294967295));
    check_encode_cbor({0x1b,0,0,0,1,0,0,0,0},json(4294967296));
    check_encode_cbor({0x1b,0x7f,0xff,0xff,0xff,0xff,0xff,0xff,0xff},json((std::numeric_limits<int64_t>::max)()));

    // negative integers
    check_encode_cbor({0x20},json(-1));
    check_encode_cbor({0x21},json(-2));
    check_encode_cbor({0x37},json(-24));
    check_encode_cbor({0x38,0x18},json(-25));
    check_encode_cbor({0x38,0xff},json(-256));
    check_encode_cbor({0x39,0x01,0x00},json(-257));
    check_encode_cbor({0x39,0xff,0xff},json(-65536));
    check_encode_cbor({0x3a,0,1,0x00,0x00},json(-65537));
    check_encode_cbor({0x3a,0xff,0xff,0xff,0xff},json(-4294967296));
    check_encode_cbor({0x3b,0,0,0,1,0,0,0,0},json(-4294967297));

    // null, true, false
    check_encode_cbor({0xf6},json::null());
    check_encode_cbor({0xf5},json(true));
    check_encode_cbor({0xf4},json(false));

    // floating point
    check_encode_cbor({0xfa,0,0,0,0},json(0.0));
    check_encode_cbor({0xfa,0xbf,0x80,0,0},json(-1.0));

    SECTION("-16777215.0")
    {
        double val = -16777215.0;
        float valf = (float)val;
        CHECK((double)valf == val);
        check_encode_cbor({0xfa,0xcb,0x7f,0xff,0xff},json(val));
    }
    // From https://en.wikipedia.org/wiki/Double-precision_floating-point_format
    SECTION("0.333333333333333314829616256247390992939472198486328125")
    {
        double val = 0.333333333333333314829616256247390992939472198486328125;
        float valf = (float)val;
        CHECK((double)valf != val);
        check_encode_cbor({0xfb,0x3F,0xD5,0x55,0x55,0x55,0x55,0x55,0x55},json(val));
    }

    // byte string
    check_encode_cbor({0x40},json(byte_string()));
    check_encode_cbor({0x41,' '},json(byte_string({' '})));
    check_encode_cbor({0x41,0},json(byte_string({0})));
    check_encode_cbor({0x45,'H','e','l','l','o'},json(byte_string("Hello")));
    check_encode_cbor({0x58,0x18,'1','2','3','4','5','6','7','8','9','0','1','2','3','4','5','6','7','8','9','0','1','2','3','4'},
                 json(byte_string("123456789012345678901234")));

    // text string
    check_encode_cbor({0x60},json(""));
    check_encode_cbor({0x61,' '},json(" "));
    check_encode_cbor({0x78,0x18,'1','2','3','4','5','6','7','8','9','0','1','2','3','4','5','6','7','8','9','0','1','2','3','4'},
                 json("123456789012345678901234"));

}

TEST_CASE("cbor_arrays_and_maps")
{
    check_encode_cbor({ 0x80 }, json::array());
    check_encode_cbor({ 0xa0 }, json::object());

    check_encode_cbor({ 0x81,'\0' }, json::parse("[0]"));
    check_encode_cbor({ 0x82,'\0','\0' }, json::array({ 0,0 }));
    check_encode_cbor({ 0x82,0x81,'\0','\0' }, json::parse("[[0],0]"));
    check_encode_cbor({ 0x81,0x65,'H','e','l','l','o' }, json::parse("[\"Hello\"]"));

    // big float
    json j("0x6AB3p-2", semantic_tag::bigfloat);
    CHECK(j.get_semantic_tag() == semantic_tag::bigfloat);
    json j2 = j;
    CHECK(j2.get_semantic_tag() == semantic_tag::bigfloat);
    json j3;
    j3 = j;
    CHECK(j3.get_semantic_tag() == semantic_tag::bigfloat);

    check_encode_cbor({ 0xc5, // Tag 5 
                         0x82, // Array of length 2
                           0x21, // -2 
                             0x19, 0x6a, 0xb3 // 27315 
        }, json("0x6AB3p-2", semantic_tag::bigfloat));

    check_encode_cbor({ 0xa1,0x62,'o','c',0x81,'\0' }, json::parse("{\"oc\": [0]}"));
    check_encode_cbor({ 0xa1,0x62,'o','c',0x84,'\0','\1','\2','\3' }, json::parse("{\"oc\": [0, 1, 2, 3]}"));
}

