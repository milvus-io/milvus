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

TEST_CASE("serialize object to bson")
{
    std::vector<uint8_t> v;
    bson::bson_bytes_encoder encoder(v);

    encoder.begin_object();
    encoder.name("null");
    encoder.null_value();
    encoder.end_object();
    encoder.flush();

    try
    {
        json result = bson::decode_bson<json>(v);
        std::cout << result << std::endl;
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << std::endl;
    }
} 

namespace jsoncons { namespace bson {

    void test_equal(const std::vector<uint8_t>& v, const std::vector<uint8_t>& expected)
    {
        REQUIRE(v.size() == expected.size());

        for (size_t i = 0; i < v.size(); ++i)
        {
            CHECK(v[i] == expected[i]);
        }
    }

    void check_equal(const std::vector<uint8_t>& v, const std::vector<uint8_t>& expected)
    {
        test_equal(v, expected);
        try
        {
            json j = bson::decode_bson<json>(v);
            std::vector<uint8_t> u;
            bson::encode_bson(j, u);
            test_equal(v,u);
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << std::endl;
        }
    }
}}

TEST_CASE("serialize to bson")
{
    SECTION("array")
    {
        std::vector<uint8_t> v;
        bson::bson_bytes_encoder encoder(v);

        encoder.begin_array();
        encoder.int64_value((std::numeric_limits<int64_t>::max)());
        encoder.uint64_value((uint64_t)(std::numeric_limits<int64_t>::max)());
        encoder.double_value((std::numeric_limits<double>::max)());
        encoder.bool_value(true);
        encoder.bool_value(false);
        encoder.null_value();
        encoder.string_value("Pussy cat");
        encoder.byte_string_value(byte_string({'h','i','s','s'}));
        encoder.end_array();
        encoder.flush();

        std::vector<uint8_t> bson = {0x4d,0x00,0x00,0x00,
                                     0x12, // int64
                                     0x30, // '0'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x7f,
                                     0x12, // int64
                                     0x31, // '1'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x7f,
                                     0x01, // double
                                     0x32, // '2'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xef,0x7f,
                                     0x08, // bool
                                     0x33, // '3'
                                     0x00, // terminator
                                     0x01,
                                     0x08, // bool
                                     0x34, // '4'
                                     0x00, // terminator
                                     0x00,
                                     0x0a, // null
                                     0x35, // '5'
                                     0x00, // terminator
                                     0x02, // string
                                     0x36, // '6'
                                     0x00, // terminator
                                     0x0a,0x00,0x00,0x00, // string length
                                     'P','u','s','s','y',' ','c','a','t',
                                     0x00, // terminator
                                     0x05, // binary
                                     0x37, // '7'
                                     0x00, // terminator
                                     0x04,0x00,0x00,0x00, // byte string length
                                     'h','i','s','s',
                                     0x00 // terminator
                                     };
        jsoncons::bson::check_equal(v,bson);

    }
    SECTION("object")
    {
        std::vector<uint8_t> v;
        bson::bson_bytes_encoder encoder(v);

        encoder.begin_object();
        encoder.name("0");
        encoder.int64_value((std::numeric_limits<int64_t>::max)());
        encoder.name("1");
        encoder.uint64_value((uint64_t)(std::numeric_limits<int64_t>::max)());
        encoder.name("2");
        encoder.double_value((std::numeric_limits<double>::max)());
        encoder.name("3");
        encoder.bool_value(true);
        encoder.name("4");
        encoder.bool_value(false);
        encoder.name("5");
        encoder.null_value();
        encoder.name("6");
        encoder.string_value("Pussy cat");
        encoder.name("7");
        encoder.byte_string_value(byte_string({'h','i','s','s'}));
        encoder.end_object();
        encoder.flush();

        std::vector<uint8_t> bson = {0x4d,0x00,0x00,0x00,
                                     0x12, // int64
                                     0x30, // '0'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x7f,
                                     0x12, // int64
                                     0x31, // '1'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x7f,
                                     0x01, // double
                                     0x32, // '2'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xef,0x7f,
                                     0x08, // bool
                                     0x33, // '3'
                                     0x00, // terminator
                                     0x01,
                                     0x08, // bool
                                     0x34, // '4'
                                     0x00, // terminator
                                     0x00,
                                     0x0a, // null
                                     0x35, // '5'
                                     0x00, // terminator
                                     0x02, // string
                                     0x36, // '6'
                                     0x00, // terminator
                                     0x0a,0x00,0x00,0x00, // string length
                                     'P','u','s','s','y',' ','c','a','t',
                                     0x00, // terminator
                                     0x05, // binary
                                     0x37, // '7'
                                     0x00, // terminator
                                     0x04,0x00,0x00,0x00, // byte string length
                                     'h','i','s','s',
                                     0x00 // terminator
                                     };
        jsoncons::bson::check_equal(v,bson);
    }

    SECTION("outer object")
    {
        std::vector<uint8_t> v;
        bson::bson_bytes_encoder encoder(v);

        encoder.begin_object();
        encoder.name("a");
        encoder.begin_object();
        encoder.name("0");
        encoder.int64_value((std::numeric_limits<int64_t>::max)());
        encoder.end_object();
        encoder.end_object();
        encoder.flush();

        std::vector<uint8_t> bson = {0x18,0x00,0x00,0x00,
                                     0x03, // object
                                     'a',
                                     0x00,
                                     0x10,0x00,0x00,0x00,
                                     0x12, // int64
                                     0x30, // '0'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x7f,
                                     0x00, // terminator
                                     0x00 // terminator
                                     };
        jsoncons::bson::check_equal(v,bson);
    }

    SECTION("outer array")
    {
        std::vector<uint8_t> v;
        bson::bson_bytes_encoder encoder(v);

        encoder.begin_array();
        encoder.begin_object();
        encoder.name("0");
        encoder.int64_value((std::numeric_limits<int64_t>::max)());
        encoder.end_object();
        encoder.end_array();
        encoder.flush();

        std::vector<uint8_t> bson = {0x18,0x00,0x00,0x00,
                                     0x03, // object
                                     '0',
                                     0x00,
                                     0x10,0x00,0x00,0x00,
                                     0x12, // int64
                                     0x30, // '0'
                                     0x00, // terminator
                                     0xff,0xff,0xff,0xff,0xff,0xff,0xff,0x7f,
                                     0x00, // terminator
                                     0x00 // terminator
                                     };
        jsoncons::bson::check_equal(v,bson);
    }
}

