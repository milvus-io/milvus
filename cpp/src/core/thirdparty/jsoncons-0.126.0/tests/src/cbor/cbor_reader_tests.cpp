// Copyright 2016 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h"
#endif
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/cbor/cbor_reader.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <limits>

using namespace jsoncons;
using namespace jsoncons::cbor;

void check_parse_cbor(const std::vector<uint8_t>& v, const json& expected)
{
    try
    {
        std::error_code ec;

        jsoncons::json_decoder<json> decoder;
        cbor_bytes_reader parser(v, decoder);
        parser.read(ec);

        json result = decoder.get_result();

        if (!(result == expected))
        {
            std::cout << "v: ";
            for (auto b : v)
            {
                std::cout << "0x" << std::hex << (int)b;
            }
            std::cout << "\n";
            std::cout << "result: " << result << "\n";
            std::cout << "expected: " << expected << "\n";
        }

        REQUIRE(result == expected);
        CHECK(result.get_semantic_tag() == expected.get_semantic_tag());

        std::string s;
        for (auto c : v)
        {
            s.push_back(c);
        }
        std::istringstream is(s);
        json j2 = decode_cbor<json>(is);
        REQUIRE(j2 == expected);
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << std::endl;
        std::cout << expected.to_string() << std::endl;
    }
}
TEST_CASE("test_cbor_parsing")
{
    // unsigned integer
    check_parse_cbor({0x00},json(0U));
    check_parse_cbor({0x01},json(1U));
    check_parse_cbor({0x0a},json(10U));
    check_parse_cbor({0x17},json(23U));
    check_parse_cbor({0x18,0x18},json(24U));
    check_parse_cbor({0x18,0xff},json(255U));
    check_parse_cbor({0x19,0x01,0x00},json(256U));
    check_parse_cbor({0x19,0xff,0xff},json(65535U));
    check_parse_cbor({0x1a,0,1,0x00,0x00},json(65536U));
    check_parse_cbor({0x1a,0xff,0xff,0xff,0xff},json(4294967295U));
    check_parse_cbor({0x1b,0,0,0,1,0,0,0,0},json(4294967296U));
    check_parse_cbor({0x1b,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff},json((std::numeric_limits<uint64_t>::max)()));

    // positive signed integer
    check_parse_cbor({0x00},json(0));
    check_parse_cbor({0x01},json(1));
    check_parse_cbor({0x0a},json(10));
    check_parse_cbor({0x17},json(23));
    check_parse_cbor({0x18,0x18},json(24));
    check_parse_cbor({0x18,0xff},json(255));
    check_parse_cbor({0x19,0x01,0x00},json(256));
    check_parse_cbor({0x19,0xff,0xff},json(65535));
    check_parse_cbor({0x1a,0,1,0x00,0x00},json(65536));
    check_parse_cbor({0x1a,0xff,0xff,0xff,0xff},json(4294967295));
    check_parse_cbor({0x1b,0,0,0,1,0,0,0,0},json(4294967296));
    check_parse_cbor({0x1b,0x7f,0xff,0xff,0xff,0xff,0xff,0xff,0xff},json((std::numeric_limits<int64_t>::max)()));
    // negative integers
    check_parse_cbor({0x20},json(-1));
    check_parse_cbor({0x21},json(-2));
    check_parse_cbor({0x37},json(-24));
    check_parse_cbor({0x38,0x18},json(-25));
    check_parse_cbor({0x38,0xff},json(-256));
    check_parse_cbor({0x39,0x01,0x00},json(-257));
    check_parse_cbor({0x39,0xff,0xff},json(-65536));
    check_parse_cbor({0x3a,0,1,0x00,0x00},json(-65537));

    check_parse_cbor({0x3a,0xff,0xff,0xff,0xff},json(-4294967296));
    check_parse_cbor({0x3b,0,0,0,1,0,0,0,0},json(-4294967297));

    // null, undefined, true, false
    check_parse_cbor({0xf6},json::null());
    check_parse_cbor({0xf7},json{null_type(),semantic_tag::undefined});
    check_parse_cbor({0xf5},json(true));
    check_parse_cbor({0xf4},json(false));

    // floating point
    check_parse_cbor({0xfb,0,0,0,0,0,0,0,0},json(0.0));
    check_parse_cbor({0xfb,0xbf,0xf0,0,0,0,0,0,0},json(-1.0));
    check_parse_cbor({0xfb,0xc1,0x6f,0xff,0xff,0xe0,0,0,0},json(-16777215.0));
    check_parse_cbor({0xfa,0xcb,0x7f,0xff,0xff},json(-16777215.0));

    // byte string
    std::vector<uint8_t> v;
    check_parse_cbor({0x40},json(byte_string_view(v.data(),v.size())));
    v = {' '};
    check_parse_cbor({0x41,' '},json(byte_string_view(v.data(),v.size())));
    v = {0};
    check_parse_cbor({0x41,0},json(byte_string_view(v.data(),v.size())));
    v = {'H','e','l','l','o'};
    check_parse_cbor({0x45,'H','e','l','l','o'},json(byte_string_view(v.data(),v.size())));
    v = {'1','2','3','4','5','6','7','8','9','0','1','2','3','4','5','6','7','8','9','0','1','2','3','4'};
    check_parse_cbor({0x58,0x18,'1','2','3','4','5','6','7','8','9','0','1','2','3','4','5','6','7','8','9','0','1','2','3','4'},
                 json(byte_string_view(v.data(),v.size())));

    // string
    check_parse_cbor({0x60},json(""));
    check_parse_cbor({0x61,' '},json(" "));
    check_parse_cbor({0x78,0x18,'1','2','3','4','5','6','7','8','9','0','1','2','3','4','5','6','7','8','9','0','1','2','3','4'},
                 json("123456789012345678901234"));

    // byte strings with undefined length
    check_parse_cbor({0x5f,0xff}, json(byte_string()));
    check_parse_cbor({0x5f,0x40,0xff}, json(byte_string()));
    check_parse_cbor({0x5f,0x40,0x40,0xff}, json(byte_string()));

    check_parse_cbor({0x5f,0x43,'H','e','l',0x42,'l','o',0xff}, json(byte_string("Hello")));
    check_parse_cbor({0x5f,0x41,'H',0x41,'e',0x41,'l',0x41,'l',0x41,'o',0xff}, json(byte_string("Hello")));
    check_parse_cbor({0x5f,0x41,'H',0x41,'e',0x40,0x41,'l',0x41,'l',0x41,'o',0xff}, json(byte_string("Hello")));

    // text strings with undefined length

    check_parse_cbor({0x7f,0xff}, json(""));

    check_parse_cbor({0x7f,0x60,0xff}, json(""));
    check_parse_cbor({0x7f,0x60,0x60,0xff}, json(""));
    check_parse_cbor({0x7f,0x63,'H','e','l',0x62,'l','o',0xff}, json("Hello"));
    check_parse_cbor({0x7f,0x61,'H',0x61,'e',0x61,'l',0x61,'l',0x61,'o',0xff}, json("Hello"));
    check_parse_cbor({0x7f,0x61,'H',0x61,'e',0x61,'l',0x60,0x61,'l',0x61,'o',0xff}, json("Hello"));

    SECTION ("arrays with definite length")
    {
        check_parse_cbor({0x80},json::array());
        check_parse_cbor({0x81,'\0'},json::parse("[0]"));
        check_parse_cbor({0x82,'\0','\0'},json::array({0,0}));
        check_parse_cbor({0x82,0x81,'\0','\0'}, json::parse("[[0],0]"));
        check_parse_cbor({0x81,0x65,'H','e','l','l','o'},json::parse("[\"Hello\"]"));

        check_parse_cbor({0x83,0x01,0x82,0x02,0x03,0x82,0x04,0x05},json::parse("[1, [2, 3], [4, 5]]"));
        check_parse_cbor({0x82,
                       0x7f,0xff,
                       0x7f,0xff},
                      json::parse("[\"\",\"\"]"));

        check_parse_cbor({0x82,
                       0x5f,0xff,
                       0x5f,0xff},
                      json::array{json(byte_string()),json(byte_string())});
    }

    SECTION("arrays with indefinite length")
    {
        //check_parse_cbor({0x9f,0xff},json::array());
        check_parse_cbor({0x9f,0x9f,0xff,0xff},json::parse("[[]]"));

        check_parse_cbor({0x9f,0x01,0x82,0x02,0x03,0x9f,0x04,0x05,0xff,0xff},json::parse("[1, [2, 3], [4, 5]]"));
        check_parse_cbor({0x9f,0x01,0x82,0x02,0x03,0x82,0x04,0x05,0xff},json::parse("[1, [2, 3], [4, 5]]"));

        check_parse_cbor({0x83,0x01,0x82,0x02,0x03,0x9f,0x04,0x05,0xff},json::parse("[1, [2, 3], [4, 5]]"));
        check_parse_cbor({0x83,             // Array of length 3
                           0x01,         // 1
                               0x9f,     // Start indefinite-length array
                                  0x02,  // 2
                                  0x03,  // 3
                                  0xff,  // "break"
                                0x82,    // Array of length 2
                                  0x04,  // 4
                                  0x05}, // 5
                      json::parse("[1, [2, 3], [4, 5]]"));

    }

    // big float

    check_parse_cbor({0xc5, // Tag 5 
                     0x82, // Array of length 2
                       0x21, // -2 
                         0x19, 0x6a, 0xb3 // 27315 
                  },json("0x6AB3p-2",semantic_tag::bigfloat));

    SECTION("maps with definite length")
    {
        //check_parse_cbor({0xa0},json::object());
        check_parse_cbor({0xa1,0x62,'o','c',0x81,'\0'}, json::parse("{\"oc\": [0]}"));
        //check_parse_cbor({0xa1,0x62,'o','c',0x84,'\0','\1','\2','\3'}, json::parse("{\"oc\": [0, 1, 2, 3]}"));
    }
    SECTION("maps with indefinite length")
    {
        check_parse_cbor({0xbf,0xff},json::object());
        check_parse_cbor({0xbf,0x64,'N','a','m','e',0xbf,0xff,0xff},json::parse("{\"Name\":{}}"));

        check_parse_cbor({0xbf,                       // Start indefinite-length map
                           0x63,                   // First key, UTF-8 string length 3
                               0x46,0x75,0x6e,     // "Fun"
                           0xf5,                   // First value, true
                               0x63,               // Second key, UTF-8 string length 3
                                   0x41,0x6d,0x74, // "Amt"
                           0x21,                   // -2
                               0xff},              // "break"
                      json::parse("{\"Fun\": true, \"Amt\": -2}"));
        check_parse_cbor({0xbf,                       // Start indefinite-length map
                           0x21,                   // First key, -2
                           0xf5,                   // First value, true
                               0xf5,               // Second key, UTF-8 string length 3
                           0x21,                   // -2
                               0xff},              // "break"
                      json::parse("{\"-2\": true, \"true\": -2}"));
    }

    SECTION("maps with non-string keys")
    {
        check_parse_cbor({0xbf,                       // Start indefinite-length map
                           0x21,                   // First key, -2
                           0xf5,                   // First value, true
                               0xf5,               // Second key, UTF-8 string length 3
                           0x21,                   // -2
                               0xff},              // "break"
                      json::parse("{\"-2\": true, \"true\": -2}"));
    }

    // bignum
    check_parse_cbor({0xc2,0x49,0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00},
                  json(bignum(1,{0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00})));

    // datetime
    check_parse_cbor({0xc0,0x78,0x19,'2','0','1','5','-','0','5','-','0','7',' ','1','2',':','4','1',':','0','7','-','0','7',':','0','0'},
                  json("2015-05-07 12:41:07-07:00", semantic_tag::datetime));

    // epoch_time
    check_parse_cbor({0xc1,0x1a,0x55,0x4b,0xbf,0xd3},
                  json(1431027667, semantic_tag::timestamp));
}

TEST_CASE("cbor decimal fraction")
{
    check_parse_cbor({0xc4, // Tag 4
                   0x82, // Array of length 2
                   0x21, // -2
                   0x19,0x6a,0xb3 // 27315
                   },
                  json("273.15", semantic_tag::bigdec));
    check_parse_cbor({0xc4, // Tag 4
                   0x82, // Array of length 2
                   0x22, // -3
                   0x19,0x6a,0xb3 // 27315
                   },
                  json("27.315", semantic_tag::bigdec));
    check_parse_cbor({0xc4, // Tag 4
                   0x82, // Array of length 2
                   0x23, // -4
                   0x19,0x6a,0xb3 // 27315
                   },
                  json("2.7315", semantic_tag::bigdec));
    check_parse_cbor({0xc4, // Tag 4
                   0x82, // Array of length 2
                   0x24, // -5
                   0x19,0x6a,0xb3 // 27315
                   },
                  json("0.27315", semantic_tag::bigdec));
    check_parse_cbor({0xc4, // Tag 4
                   0x82, // Array of length 2
                   0x25, // -6
                   0x19,0x6a,0xb3 // 27315
                   },
                  json("0.027315", semantic_tag::bigdec));

    check_parse_cbor({0xc4, // Tag 4
                   0x82, // Array of length 2
                   0x04, // 4
                   0x19,0x6a,0xb3 // 27315
                   },
                  json("273150000.0", semantic_tag::bigdec));
}

TEST_CASE("test_decimal_as_string")
{
    SECTION("-2 27315")
    {
        std::vector<uint8_t> v = {0xc4, // Tag 4,
                                  0x82, // Array of length 2
                                  0x21, // -2
                                  0x19,0x6a,0xb3 // 27315
                                  };

        json j = decode_cbor<json>(v);
        CHECK(j.as<std::string>() == std::string("273.15"));
    }
    SECTION("-6 27315")
    {
        std::vector<uint8_t> v = {0xc4, // Tag 4,
                                  0x82, // Array of length 2
                                  0x25, // -6
                                  0x19,0x6a,0xb3 // 27315
                                  };

        json j = decode_cbor<json>(v);
        CHECK(j.as<std::string>() == std::string("0.027315"));
    }
    SECTION("-5 27315")
    {
        std::vector<uint8_t> v = {0xc4, // Tag 4,
                                  0x82, // Array of length 2
                                  0x24, // -5
                                  0x19,0x6a,0xb3 // 27315
                                  };

        json j = decode_cbor<json>(v);
        CHECK(j.as<std::string>() == std::string("0.27315"));
    }
    SECTION("0 27315")
    {
        std::vector<uint8_t> v = {0xc4, // Tag 4,
                                  0x82, // Array of length 2
                                  0x00, // 0
                                  0x19,0x6a,0xb3 // 27315
                                  };

        json j = decode_cbor<json>(v);
        CHECK(j.as<std::string>() == std::string("27315.0"));
    }
    SECTION("2 27315")
    {
        std::vector<uint8_t> v = {0xc4, // Tag 4,
                                  0x82, // Array of length 2
                                  0x02, // 2
                                  0x19,0x6a,0xb3 // 27315
                                  };

        json j = decode_cbor<json>(v);
        CHECK(j.as<std::string>() == std::string("2731500.0"));
    }
    SECTION("-2 18446744073709551616")
    {
        std::vector<uint8_t> v = {0xc4, // Tag 4,
                                  0x82, // Array of length 2
                                  0x21, // -2
                                  0xc2,0x49,0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00 // 18446744073709551616
                                  };

        json j = decode_cbor<json>(v);
        CHECK(j.as<std::string>() == std::string("1.8446744073709551616e+17"));
    }
    SECTION("-2 -65537")
    {
        std::vector<uint8_t> v = {0xc4, // Tag 4,
                                  0x82, // Array of length 2
                                  0x21, // -2
                                  0x3a,0,1,0x00,0x00 // -65537
                                  };

        json j = decode_cbor<json>(v);
        CHECK(j.as<std::string>() == std::string("-655.37"));
    }
    SECTION("-5 -65537")
    {
        std::vector<uint8_t> v = {0xc4, // Tag 4,
                                  0x82, // Array of length 2
                                  0x24, // -5
                                  0x3a,0,1,0x00,0x00 // -65537
                                  };

        json j = decode_cbor<json>(v);
        CHECK(j.as<std::string>() == std::string("-0.65537"));
    }
    SECTION("-6 -65537")
    {
        std::vector<uint8_t> v = {0xc4, // Tag 4,
                                  0x82, // Array of length 2
                                  0x25, // -6
                                  0x3a,0,1,0x00,0x00 // -65537
                                  };

        json j = decode_cbor<json>(v);
        CHECK(j.as<std::string>() == std::string("-0.065537"));
    }
}

TEST_CASE("Compare CBOR packed item and jsoncons item")
{
    std::vector<uint8_t> bytes;
    cbor::cbor_bytes_encoder encoder(bytes);
    encoder.begin_array(); // indefinite length outer array
    encoder.string_value("foo");
    encoder.byte_string_value(byte_string{'b','a','r'});
    encoder.string_value("-18446744073709551617", semantic_tag::bigint);
    encoder.string_value("-273.15", semantic_tag::bigdec);
    encoder.string_value("273.15", semantic_tag::bigdec);
    encoder.string_value("18446744073709551616.15", semantic_tag::bigdec);
    encoder.string_value("-18446744073709551617.15", semantic_tag::bigdec);
    encoder.string_value("2018-10-19 12:41:07-07:00", semantic_tag::datetime) ;
    encoder.int64_value(1431027667, semantic_tag::timestamp);
    encoder.int64_value(-1431027667, semantic_tag::timestamp);
    encoder.double_value(1431027667.5, semantic_tag::timestamp);
    encoder.end_array();
    encoder.flush();

    json expected = json::array();
    expected.emplace_back("foo");
    expected.emplace_back(byte_string{ 'b','a','r' });
    expected.emplace_back("-18446744073709551617", semantic_tag::bigint);
    expected.emplace_back("-273.15", semantic_tag::bigdec);
    expected.emplace_back("273.15", semantic_tag::bigdec);
    expected.emplace_back("1.844674407370955161615e+19", semantic_tag::bigdec);
    expected.emplace_back("-1.844674407370955161715e+19", semantic_tag::bigdec);
    expected.emplace_back("2018-10-19 12:41:07-07:00", semantic_tag::datetime);
    expected.emplace_back(1431027667, semantic_tag::timestamp);
    expected.emplace_back(-1431027667, semantic_tag::timestamp);
    expected.emplace_back(1431027667.5, semantic_tag::timestamp);

    json j = cbor::decode_cbor<json>(bytes);

    REQUIRE(j == expected);
    for (size_t i = 0; i < j.size(); ++i)
    {
        CHECK(j[i].get_semantic_tag() == expected[i].get_semantic_tag()); 
    }
}

TEST_CASE("CBOR stringref tag 1")
{
    std::vector<uint8_t> v = {0xd9,0x01,0x00, // tag(256)
                                0x83, // array(3)
                                   0xa3, // map(3)
                                      0x44, // bytes(4)
                                         0x72,0x61,0x6e,0x6b, // "rank"
                                      0x04, // unsigned(4)
                                      0x45, // bytes(5)
                                         0x63,0x6f,0x75,0x6e,0x74, // "count"
                                      0x19,0x01,0xa1, // unsigned(417)
                                      0x44, // bytes(4)
                                         0x6e,0x61,0x6d,0x65, // "name"
                                      0x48, // bytes(8)
                                         0x43,0x6f,0x63,0x6b,0x74,0x61,0x69,0x6c, // "Cocktail"
                                   0xa3, // map(3)
                                      0xd8,0x19, // tag(25)
                                         0x02, // unsigned(2)
                                      0x44, // bytes(4)
                                         0x42,0x61,0x74,0x68, // "Bath"
                                      0xd8,0x19, // tag(25)
                                         0x01, // unsigned(1)
                                      0x19,0x01,0x38, // unsigned(312)
                                      0xd8,0x19, // tag(25)
                                         0x00, // unsigned(0)
                                      0x04, // unsigned(4)
                                   0xa3, // map(3)
                                      0xd8,0x19, // tag(25)
                                         0x02, // unsigned(2)
                                      0x44, // bytes(4)
                                         0x46,0x6f,0x6f,0x64, // "Food"
                                      0xd8,0x19, // tag(25)
                                         0x01, // unsigned(1)
                                      0x19,0x02,0xb3, // unsigned(691)
                                      0xd8,0x19, // tag(25)
                                         0x00, // unsigned(0)
                                      0x04 // unsigned(4)
    };

    SECTION("decode")
    {
        ojson j = decode_cbor<ojson>(v);
        //std::cout << pretty_print(j) << "\n";

        {
            auto it = j[0].object_range().begin();
            std::string key1;
            decode_base64url(it->key().begin(),it->key().end(),key1);
            CHECK(key1 == std::string("rank"));
            ++it;
            std::string key2;
            decode_base64url(it->key().begin(),it->key().end(),key2);
            CHECK(key2 == std::string("count"));
            ++it;
            std::string key3;
            decode_base64url(it->key().begin(),it->key().end(),key3);
            CHECK(key3 == std::string("name"));
        }
        {
            auto it = j[1].object_range().begin();
            std::string key3;
            decode_base64url(it->key().begin(),it->key().end(),key3);
            CHECK(key3 == std::string("name"));
            ++it;
            std::string key2;
            decode_base64url(it->key().begin(),it->key().end(),key2);
            CHECK(key2 == std::string("count"));
            ++it;
            std::string key1;
            decode_base64url(it->key().begin(),it->key().end(),key1);
            CHECK(key1 == std::string("rank"));
        }
        {
            auto it = j[2].object_range().begin();
            std::string key3;
            decode_base64url(it->key().begin(),it->key().end(),key3);
            CHECK(key3 == std::string("name"));
            ++it;
            std::string key2;
            decode_base64url(it->key().begin(),it->key().end(),key2);
            CHECK(key2 == std::string("count"));
            ++it;
            std::string key1;
            decode_base64url(it->key().begin(),it->key().end(),key1);
            CHECK(key1 == std::string("rank"));
        }
    }
}

TEST_CASE("CBOR stringref tag 2")
{
        std::vector<uint8_t> v = {0xd9, 0x01, 0x00,           // tag(256)
         0x98, 0x20,          // array(32)
         0x41,          // bytes(1)
            0x31,       // "1"
         0x43,          // bytes(3)
            0x32,0x32,0x32,   // "222"
         0x43,          // bytes(3)
            0x33,0x33,0x33,   // "333"
         0x41,          // bytes(1)
            0x34,       // "4"
         0x43,          // bytes(3)
            0x35,0x35,0x35,   // "555"
         0x43,          // bytes(3)
            0x36,0x36,0x36,   // "666"
         0x43,          // bytes(3)
            0x37,0x37,0x37,   // "777"
         0x43,          // bytes(3)
            0x38,0x38,0x38,   // "888"
         0x43,          // bytes(3)
            0x39,0x39,0x39,   // "999"
         0x43,          // bytes(3)
            0x61,0x61,0x61,   // "aaa"
         0x43,          // bytes(3)
            0x62,0x62,0x62,   // "bbb"
         0x43,          // bytes(3)
            0x63,0x63,0x63,   // "ccc"
         0x43,          // bytes(3)
            0x64,0x64,0x64,   // "ddd"
         0x43,          // bytes(3)
            0x65,0x65,0x65,   // "eee"
         0x43,          // bytes(3)
            0x66,0x66,0x66,   // "fff"
         0x43,          // bytes(3)
            0x67,0x67,0x67,   // "ggg"
         0x43,          // bytes(3)
            0x68,0x68,0x68,   // "hhh"
         0x43,          // bytes(3)
            0x69,0x69,0x69,   // "iii"
         0x43,          // bytes(3)
            0x6a,0x6a,0x6a,   // "jjj"
         0x43,          // bytes(3)
            0x6b,0x6b,0x6b,   // "kkk"
         0x43,          // bytes(3)
            0x6c,0x6c,0x6c,   // "lll"
         0x43,          // bytes(3)
            0x6d,0x6d,0x6d,   // "mmm"
         0x43,          // bytes(3)
            0x6e,0x6e,0x6e,   // "nnn"
         0x43,          // bytes(3)
            0x6f,0x6f,0x6f,   // "ooo"
         0x43,          // bytes(3)
            0x70,0x70,0x70,   // "ppp"
         0x43,          // bytes(3)
            0x71,0x71,0x71,   // "qqq"
         0x43,          // bytes(3)
            0x72,0x72,0x72,   // "rrr"
         0xd8, 0x19,       // tag(25)
            0x01,       // unsigned(1)
         0x44,          // bytes(4)
            0x73,0x73,0x73,0x73, // "ssss"
         0xd8, 0x19,       // tag(25)
            0x17,       // unsigned(23)
         0x43,          // bytes(3)
            0x72,0x72,0x72,   // "rrr"
         0xd8, 0x19,       // tag(25)
            0x18, 0x18    // unsigned(24)
        };

        ojson j = decode_cbor<ojson>(v);
        
        byte_string bs = j[0].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("1"));
        
        bs = j[1].as<byte_string>(); // 0
        CHECK(std::string(bs.begin(),bs.end()) == std::string("222"));
        
        bs = j[2].as<byte_string>(); // 1
        CHECK(std::string(bs.begin(),bs.end()) == std::string("333"));
        
        bs = j[3].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("4"));
        
        bs = j[4].as<byte_string>(); // 2
        CHECK(std::string(bs.begin(),bs.end()) == std::string("555"));
        
        bs = j[5].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("666"));
        
        bs = j[6].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("777"));
        
        bs = j[7].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("888"));
        
        bs = j[8].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("999"));

        bs = j[9].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("aaa"));

        bs = j[10].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("bbb"));

        bs = j[11].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("ccc"));

        bs = j[12].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("ddd"));

        bs = j[13].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("eee"));

        bs = j[14].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("fff"));

        bs = j[15].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("ggg"));

        bs = j[16].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("hhh"));

        bs = j[17].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("iii"));

        bs = j[18].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("jjj"));

        bs = j[19].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("kkk"));

        bs = j[20].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("lll"));

        bs = j[21].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("mmm"));

        bs = j[22].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("nnn"));

        bs = j[23].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("ooo"));

        bs = j[24].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("ppp"));

        bs = j[25].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("qqq"));

        bs = j[26].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("rrr"));

        bs = j[27].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("333"));

        bs = j[28].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("ssss"));

        bs = j[29].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("qqq"));

        bs = j[30].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("rrr"));

        bs = j[31].as<byte_string>();
        CHECK(std::string(bs.begin(),bs.end()) == std::string("ssss"));
}

TEST_CASE("CBOR stringref tag 3")
{
    std::vector<uint8_t> v = {0xd9,0x01,0x00, // tag(256)
      0x85,                 // array(5)
         0x63,              // text(3)
            0x61,0x61,0x61, // "aaa"
         0xd8, 0x19,        // tag(25)
            0x00,           // unsigned(0)
         0xd9, 0x01,0x00,   // tag(256)
            0x83,           // array(3)
               0x63,        // text(3)
                  0x62,0x62,0x62, // "bbb"
               0x63,        // text(3)
                  0x61,0x61,0x61, // "aaa"
               0xd8, 0x19,  // tag(25)
                  0x01,     // unsigned(1)
         0xd9, 0x01,0x00,   // tag(256)
            0x82,           // array(2)
               0x63,        // text(3)
                  0x63,0x63,0x63, // "ccc"
               0xd8, 0x19,  // tag(25)
                  0x00,     // unsigned(0)
         0xd8, 0x19,        // tag(25)
            0x00           // unsigned(0)
    };

    json j = cbor::decode_cbor<json>(v);

    json expected = json::parse(R"(
        ["aaa","aaa",["bbb","aaa","aaa"],["ccc","ccc"],"aaa"]
    )");

    CHECK(j == expected);
}
