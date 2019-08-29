// Copyright 2016 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h"
#endif
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/cbor/cbor_encoder.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <limits>
#include <fstream>
#include <iomanip>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("test_serialize_to_stream")
{
json j = json::parse(R"(
{
   "application": "hiking",
   "reputons": [
   {
       "rater": "HikingAsylum.example.com",
       "assertion": "strong-hiker",
       "rated": "Marilyn C",
       "rating": 0.90
     }
   ]
}
)");

    std::ofstream os;
    os.open("./output/store.cbor", std::ios::binary | std::ios::out);
    cbor::encode_cbor(j,os);

    std::vector<uint8_t> v;
    std::ifstream is;
    is.open("./output/store.cbor", std::ios::binary | std::ios::in);

    json j2 = cbor::decode_cbor<json>(is);

    //std::cout << pretty_print(j2) << std::endl; 

    CHECK(j == j2);
}

TEST_CASE("serialize array to cbor")
{
    std::vector<uint8_t> v;
    cbor::cbor_bytes_encoder encoder(v);
    //encoder.begin_object(1);
    encoder.begin_array(3);
    encoder.bool_value(true);
    encoder.bool_value(false);
    encoder.null_value();
    encoder.end_array();
    //encoder.end_object();
    encoder.flush();

    try
    {
        json result = cbor::decode_cbor<json>(v);
        //std::cout << result << std::endl;
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << std::endl;
    }
}

TEST_CASE("test_serialize_indefinite_length_array")
{
    std::vector<uint8_t> v;
    cbor::cbor_bytes_encoder encoder(v);
    encoder.begin_array();
    encoder.begin_array(4);
    encoder.bool_value(true);
    encoder.bool_value(false);
    encoder.null_value();
    encoder.string_value("Hello");
    encoder.end_array();
    encoder.end_array();
    encoder.flush();

    try
    {
        json result = cbor::decode_cbor<json>(v);
        //std::cout << result << std::endl;
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << std::endl;
    }
} 
TEST_CASE("test_serialize_bignum")
{
    std::vector<uint8_t> v;
    cbor::cbor_bytes_encoder encoder(v);
    encoder.begin_array();

    std::vector<uint8_t> bytes = {0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
    bignum n(1, bytes.data(), bytes.size());
    std::string s;
    n.dump(s);
    encoder.string_value(s, semantic_tag::bigint);
    encoder.end_array();
    encoder.flush();

    try
    {
        json result = cbor::decode_cbor<json>(v);
        CHECK(result[0].as<std::string>() == std::string("18446744073709551616"));
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << std::endl;
    }
} 

TEST_CASE("test_serialize_negative_bignum1")
{
    std::vector<uint8_t> v;
    cbor::cbor_bytes_encoder encoder(v);
    encoder.begin_array();

    std::vector<uint8_t> bytes = {0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
    bignum n(-1, bytes.data(), bytes.size());
    std::string s;
    n.dump(s);
    encoder.string_value(s, semantic_tag::bigint);
    encoder.end_array();
    encoder.flush();

    try
    {
        json result = cbor::decode_cbor<json>(v);
        CHECK(result[0].as<std::string>() == std::string("-18446744073709551617"));
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << std::endl;
    }
} 

TEST_CASE("test_serialize_negative_bignum2")
{
    std::vector<uint8_t> v;
    cbor::cbor_bytes_encoder encoder(v);
    encoder.begin_array();

    std::vector<uint8_t> bytes = {0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
    bignum n(-1, bytes.data(), bytes.size());
    std::string s;
    n.dump(s);
    encoder.string_value(s, semantic_tag::bigint);
    encoder.end_array();
    encoder.flush();

    try
    {
        json result = cbor::decode_cbor<json>(v);
        json_options options;
        options.bigint_format(bigint_chars_format::number);
        std::string text;
        result.dump(text,options);
        CHECK(text == std::string("[-18446744073709551617]"));
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << std::endl;
    }
} 

TEST_CASE("test_serialize_negative_bignum3")
{
    std::vector<uint8_t> v;
    cbor::cbor_bytes_encoder encoder(v);
    encoder.begin_array();

    std::vector<uint8_t> bytes = {0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};

    bignum n(-1, bytes.data(), bytes.size());
    std::string s;
    n.dump(s);
    encoder.string_value(s, semantic_tag::bigint);
    encoder.end_array();
    encoder.flush();

    try
    {
        json result = cbor::decode_cbor<json>(v);
        json_options options;
        options.bigint_format(bigint_chars_format::base64url);
        std::string text;
        result.dump(text,options);
        CHECK(text == std::string("[\"~AQAAAAAAAAAA\"]"));
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << std::endl;
    }
} 

TEST_CASE("serialize bigdec to cbor")
{
    SECTION("-1 184467440737095516160")
    {
        std::vector<uint8_t> v;
        cbor::cbor_bytes_encoder encoder(v);
        encoder.string_value("18446744073709551616.0", semantic_tag::bigdec);
        encoder.flush();
        try
        {
            json result = cbor::decode_cbor<json>(v);
            CHECK(result.as<std::string>() == std::string("1.84467440737095516160e+19"));
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << std::endl;
        }
    }
    SECTION("18446744073709551616e-5")
    {
        std::vector<uint8_t> v;
        cbor::cbor_bytes_encoder encoder(v);
        encoder.string_value("18446744073709551616e-5", semantic_tag::bigdec);
        encoder.flush();
        try
        {
            json result = cbor::decode_cbor<json>(v);
            CHECK(result.as<std::string>() == std::string("184467440737095.51616"));
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << std::endl;
        }
    }
    SECTION("-18446744073709551616e-5")
    {
        std::vector<uint8_t> v;
        cbor::cbor_bytes_encoder encoder(v);
        encoder.string_value("-18446744073709551616e-5", semantic_tag::bigdec);
        encoder.flush();
        try
        {
            json result = cbor::decode_cbor<json>(v);
            CHECK(result.as<std::string>() == std::string("-184467440737095.51616"));
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << std::endl;
        }
    }
    SECTION("-18446744073709551616e5")
    {
        std::vector<uint8_t> v;
        cbor::cbor_bytes_encoder encoder(v);
        encoder.string_value("-18446744073709551616e5", semantic_tag::bigdec);
        encoder.flush();
        try
        {
            json result = cbor::decode_cbor<json>(v);
            CHECK(result.as<std::string>() == std::string("-1.8446744073709551616e+24"));
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << std::endl;
        }
    }
} 

TEST_CASE("Too many and too few items in CBOR map or array")
{
    std::error_code ec{};
    std::vector<uint8_t> v;
    cbor::cbor_bytes_encoder encoder(v);

    SECTION("Too many items in array")
    {
        CHECK(encoder.begin_array(3));
        CHECK(encoder.bool_value(true));
        CHECK(encoder.bool_value(false));
        CHECK(encoder.null_value());
        CHECK(encoder.begin_array(2));
        CHECK(encoder.string_value("cat"));
        CHECK(encoder.string_value("feline"));
        CHECK(encoder.end_array());
        REQUIRE_THROWS_WITH(encoder.end_array(), cbor::cbor_error_category_impl().message((int)cbor::cbor_errc::too_many_items).c_str());
        encoder.flush();
    }
    SECTION("Too few items in array")
    {
        CHECK(encoder.begin_array(5));
        CHECK(encoder.bool_value(true));
        CHECK(encoder.bool_value(false));
        CHECK(encoder.null_value());
        CHECK(encoder.begin_array(2));
        CHECK(encoder.string_value("cat"));
        CHECK(encoder.string_value("feline"));
        CHECK(encoder.end_array());
        REQUIRE_THROWS_WITH(encoder.end_array(), cbor::cbor_error_category_impl().message((int)cbor::cbor_errc::too_few_items).c_str());
        encoder.flush();
    }
    SECTION("Too many items in map")
    {
        CHECK(encoder.begin_object(3));
        CHECK(encoder.name("a"));
        CHECK(encoder.bool_value(true));
        CHECK(encoder.name("b"));
        CHECK(encoder.bool_value(false));
        CHECK(encoder.name("c"));
        CHECK(encoder.null_value());
        CHECK(encoder.name("d"));
        CHECK(encoder.begin_array(2));
        CHECK(encoder.string_value("cat"));
        CHECK(encoder.string_value("feline"));
        CHECK(encoder.end_array());
        REQUIRE_THROWS_WITH(encoder.end_object(), cbor::cbor_error_category_impl().message((int)cbor::cbor_errc::too_many_items).c_str());
        encoder.flush();
    }
    SECTION("Too few items in map")
    {
        CHECK(encoder.begin_object(5));
        CHECK(encoder.name("a"));
        CHECK(encoder.bool_value(true));
        CHECK(encoder.name("b"));
        CHECK(encoder.bool_value(false));
        CHECK(encoder.name("c"));
        CHECK(encoder.null_value());
        CHECK(encoder.name("d"));
        CHECK(encoder.begin_array(2));
        CHECK(encoder.string_value("cat"));
        CHECK(encoder.string_value("feline"));
        CHECK(encoder.end_array());
        REQUIRE_THROWS_WITH(encoder.end_object(), cbor::cbor_error_category_impl().message((int)cbor::cbor_errc::too_few_items).c_str());
        encoder.flush();
    }
    SECTION("Just enough items")
    {
        CHECK(encoder.begin_array(4)); // a fixed length array
        CHECK(encoder.string_value("foo"));
        CHECK(encoder.byte_string_value(byte_string{'P','u','s','s'})); // no suggested conversion
        CHECK(encoder.string_value("-18446744073709551617", semantic_tag::bigint));
        CHECK(encoder.string_value("273.15", semantic_tag::bigdec));
        CHECK(encoder.end_array());
        CHECK_FALSE(ec);
        encoder.flush();
    }
}

TEST_CASE("encode stringref")
{
    ojson j = ojson::parse(R"(
[
     {
       "name" : "Cocktail",
       "count" : 417,
       "rank" : 4
     },
     {
       "rank" : 4,
       "count" : 312,
       "name" : "Bath"
     },
     {
       "count" : 691,
       "name" : "Food",
       "rank" : 4
     }
  ]
)");

    cbor::cbor_options options;
    options.pack_strings(true);
    std::vector<uint8_t> buf;

    cbor::encode_cbor(j, buf, options);

    //for (auto c : buf)
    //{
    //    std::cout << std::hex << std::setprecision(2) << std::setw(2) 
    //              << std::noshowbase << std::setfill('0') << static_cast<int>(c);
    //}
    //std::cout << "\n";

    ojson j2 = cbor::decode_cbor<ojson>(buf);
    CHECK(j2 == j);
}
