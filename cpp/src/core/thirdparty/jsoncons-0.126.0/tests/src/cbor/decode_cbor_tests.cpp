// Copyright 2016 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h"
#endif
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <limits>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("cbor_view_test")
{
    ojson j1 = ojson::parse(R"(
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
 
    std::vector<uint8_t> c;
    cbor::encode_cbor(j1, c);

    json v = cbor::decode_cbor<json>(c); 
    CHECK(v.is_object());
    CHECK_FALSE(v.is_array());

    const json& reputons = v.at("reputons");
    CHECK(reputons.is_array());

    const json& reputons_0 = reputons.at(0);

    const json& reputons_0_rated = reputons_0.at("rated");
    (void)reputons_0_rated;

    const json& rating = reputons_0.at("rating");
    CHECK(rating.as_double() == 0.90);

    for (const auto& member : v.object_range())
    {
        const auto& key = member.key();
        const json& jval = member.value();

        (void)key;
        (void)jval;

        //std::cout << key << ": " << jval << std::endl;
    }
    //std::cout << std::endl;

    for (auto element : reputons.array_range())
    {
        json j = element;
        //std::cout << j << std::endl;
    }
    //std::cout << std::endl;
}

TEST_CASE("jsonpointer_test")
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

    std::vector<uint8_t> v;
    cbor::encode_cbor(j, v);

    json jdoc = cbor::decode_cbor<json>(v);
    std::string s;
    jdoc.dump(s);
    json j1 = json::parse(s);
    CHECK(j1 == j);

    std::error_code ec;
    const json& application = jsonpointer::get(jdoc, "/application", ec);
    CHECK_FALSE(ec);

    CHECK(application == j["application"]);

    const json& reputons_0_rated = jsonpointer::get(jdoc, "/reputons", ec);
    CHECK_FALSE(ec);

    json j4 = j["reputons"];
    CHECK(reputons_0_rated == j4);

    //std::cout << pretty_print(j3) << std::endl;
}

TEST_CASE("as_string_test")
{
    std::vector<uint8_t> v;
    cbor::cbor_bytes_encoder encoder(v);
    encoder.begin_array(10);
    encoder.bool_value(true);
    encoder.bool_value(false);
    encoder.null_value();
    encoder.string_value("Toronto");
    encoder.byte_string_value(byte_string{'H','e','l','l','o'});
    encoder.int64_value(-100);
    encoder.uint64_value(100);
    encoder.string_value("18446744073709551616", semantic_tag::bigint);
    encoder.double_value(10.5);
    encoder.string_value("-18446744073709551617", semantic_tag::bigint);
    encoder.end_array();
    encoder.flush();

    json j = cbor::decode_cbor<json>(v);

    std::string s0;
    j[0].dump(s0);
    CHECK(std::string("true") == s0);
    CHECK(std::string("true") == j[0].as_string());
    CHECK(true == j[0].as<bool>());
    CHECK(j[0].is<bool>());

    std::string s1;
    j[1].dump(s1);
    CHECK(std::string("false") == s1);
    CHECK(std::string("false") == j[1].as_string());
    CHECK(false == j[1].as<bool>());
    CHECK(j[1].is<bool>());

    std::string s2;
    j[2].dump(s2);
    CHECK(std::string("null") == s2);
    CHECK(std::string("null") == j[2].as_string());

    std::string s3;
    j[3].dump(s3);
    CHECK(std::string("\"Toronto\"") == s3);
    CHECK(std::string("Toronto") == j[3].as_string());
    CHECK(std::string("Toronto") ==j[3].as<std::string>());

    std::string s4;
    j[4].dump(s4);
    CHECK(std::string("\"SGVsbG8\"") == s4);
    CHECK(std::string("SGVsbG8") == j[4].as_string());
    CHECK(byte_string({'H','e','l','l','o'}) == j[4].as<byte_string>());

    std::string s5;
    j[5].dump(s5);
    CHECK(std::string("-100") ==s5);
    CHECK(std::string("-100") == j[5].as_string());
    CHECK(-100 ==j[5].as<int>());

    std::string s6;
    j[6].dump(s6);
    CHECK(std::string("100") == s6);
    CHECK(std::string("100") == j[6].as_string());

    std::string s7;
    j[7].dump(s7);
    CHECK(std::string("\"18446744073709551616\"") == s7);
    CHECK(std::string("18446744073709551616") == j[7].as_string());

    std::string s8;
    j[8].dump(s8);
    CHECK(std::string("10.5") == s8);
    CHECK(std::string("10.5") == j[8].as_string());

    std::string s9;
    j[9].dump(s9);
    CHECK(std::string("\"-18446744073709551617\"") == s9);
    CHECK(std::string("-18446744073709551617") == j[9].as_string());

}

TEST_CASE("dump cbor to string test")
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

    json j = cbor::decode_cbor<json>(v);

    std::string s0;
    j.dump(s0);
    CHECK("[\"-18446744073709551617\"]" == s0);
    //std::cout << s0 << std::endl;

    std::string s1;
    json_options options1;
    options1.bigint_format(bigint_chars_format::number);
    j.dump(s1,options1);
    CHECK("[-18446744073709551617]" == s1);
    //std::cout << s1 << std::endl;

    std::string s2;
    json_options options2;
    options2.bigint_format(bigint_chars_format::base10);
    j.dump(s2,options2);
    CHECK("[\"-18446744073709551617\"]" == s2);
    //std::cout << s2 << std::endl;

    std::string s3;
    json_options options3;
    options3.bigint_format(bigint_chars_format::base64url);
    j.dump(s3,options3);
    CHECK("[\"~AQAAAAAAAAAA\"]" == s3);
    //std::cout << s3 << std::endl;
} 

TEST_CASE("test_dump_to_stream")
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

    json j = cbor::decode_cbor<json>(v);

    std::ostringstream os0;
    j.dump(os0);
    CHECK("[\"-18446744073709551617\"]" == os0.str());
    //std::cout << os0.str() << std::endl;

    std::ostringstream os1;
    json_options options1;
    options1.bigint_format(bigint_chars_format::number);
    j.dump(os1,options1);
    CHECK("[-18446744073709551617]" == os1.str());
    //std::cout << os1.str() << std::endl;

    std::ostringstream os2;
    json_options options2;
    options2.bigint_format(bigint_chars_format::base10);
    j.dump(os2,options2);
    CHECK("[\"-18446744073709551617\"]" == os2.str());
    //std::cout << os2.str() << std::endl;

    std::ostringstream os3;
    json_options options3;
    options3.bigint_format(bigint_chars_format::base64url);
    j.dump(os3,options3);
    CHECK("[\"~AQAAAAAAAAAA\"]" == os3.str());
    //std::cout << os3.str() << std::endl;
} 

TEST_CASE("test_indefinite_length_object_iterator")
{
    std::vector<uint8_t> v;
    cbor::cbor_bytes_encoder encoder(v);
    encoder.begin_object(); // indefinite length object
    encoder.name("City");
    encoder.string_value("Toronto");
    encoder.name("Province");
    encoder.string_value("Ontario");
    encoder.end_object(); 
    encoder.flush();
    json bv2 = cbor::decode_cbor<json>(v);

    auto it2 = bv2.object_range().begin();
    CHECK_FALSE((it2 == bv2.object_range().end()));
    CHECK_FALSE((++it2 == bv2.object_range().end()));
    CHECK((++it2 == bv2.object_range().end()));
}

TEST_CASE("test_indefinite_length_array_iterator")
{
    std::vector<uint8_t> v;
    cbor::cbor_bytes_encoder encoder(v);
    encoder.begin_array(); // indefinite length array
    encoder.string_value("Toronto");
    encoder.string_value("Ontario");
    encoder.end_array(); 
    encoder.flush();
    json j = cbor::decode_cbor<json>(v);

    CHECK(j.size() == 2);

    auto it2 = j.array_range().begin();
    CHECK_FALSE((it2 == j.array_range().end()));
    CHECK_FALSE((++it2 == j.array_range().end()));
    CHECK((++it2 == j.array_range().end()));

}

TEST_CASE("cbor array comparison test")
{
    std::vector<uint8_t> v1;
    cbor::cbor_bytes_encoder encoder1(v1);
    encoder1.begin_array(); // indefinite length array
    encoder1.string_value("Toronto");
    encoder1.string_value("Vancouver");
    encoder1.end_array(); 
    encoder1.flush();
    json j1 = cbor::decode_cbor<json>(v1);

    std::vector<uint8_t> v2;
    cbor::cbor_bytes_encoder serializer2(v2);
    serializer2.begin_array(); // indefinite length array
    serializer2.string_value("Toronto");
    serializer2.string_value("Vancouver");
    serializer2.end_array(); 
    serializer2.flush();
    json j2 = cbor::decode_cbor<json>(v2);

    std::vector<uint8_t> v3;
    cbor::cbor_bytes_encoder serializer3(v3);
    serializer3.begin_array(); // indefinite length array
    serializer3.string_value("Toronto");
    serializer3.string_value("Montreal");
    serializer3.end_array(); 
    serializer3.flush();
    json j3 = cbor::decode_cbor<json>(v3);

    SECTION("operator== test")
    {
        CHECK(j1 == j2);
        REQUIRE(j1.size() == 2);
        REQUIRE(j2.size() == 2);
        CHECK(j1[0] == j2[0]);
        CHECK(j1[1] == j2[1]);
    }

    SECTION("element operator== test")
    {
        CHECK_FALSE(j1 == j3);
        REQUIRE(j1.size() == 2);
        REQUIRE(j1.size() == j3.size());
        CHECK(j1[0] == j3[0]);
        CHECK_FALSE(j1[1] == j3[1]);
    }
}

TEST_CASE("cbor object comparison")
{
    std::vector<uint8_t> v;
    cbor::cbor_bytes_encoder encoder1(v);
    encoder1.begin_object(); // indefinite length array
    encoder1.name("City");
    encoder1.string_value("Montreal");
    encoder1.name("Amount");
    encoder1.string_value("273.15", semantic_tag::bigdec);
    encoder1.name("Date");
    encoder1.string_value("2018-05-07 12:41:07-07:00", semantic_tag::datetime) ;
    encoder1.end_object(); 
    encoder1.flush();
    json j1 = cbor::decode_cbor<json>(v);

    //std::cout << pretty_print(j1) << "\n";
 
    REQUIRE(j1.size() == 3);

    std::vector<uint8_t> buf2;
    cbor::cbor_bytes_encoder serializer2(buf2);
    serializer2.begin_object(); // indefinite length array
    serializer2.name("City");
    serializer2.string_value("Toronto");
    serializer2.name("Amount");
    serializer2.string_value("273.15", semantic_tag::bigdec);
    serializer2.name("Date");
    serializer2.string_value("2018-10-18 12:41:07-07:00", semantic_tag::datetime) ;
    serializer2.end_object(); 
    serializer2.flush();
    json j2 = cbor::decode_cbor<json>(buf2);
    REQUIRE(j2.size() == j1.size());

    std::vector<uint8_t> buf3;
    cbor::cbor_bytes_encoder serializer3(buf3);
    serializer3.begin_object(); // indefinite length array
    serializer3.name("empty-object");
    serializer3.begin_object(0);
    serializer3.end_object();
    serializer3.name("empty-array");
    serializer3.begin_array(0);
    serializer3.end_array();
    serializer3.name("empty-string");
    serializer3.string_value("");
    serializer3.name("empty-byte_string");
    serializer3.byte_string_value(jsoncons::byte_string{});
    serializer3.end_object(); 
    serializer3.flush();
    json j3 = cbor::decode_cbor<json>(buf3);

    SECTION("contains")
    {
        CHECK(j1.contains("City"));
        CHECK(j1.contains("Amount"));
        CHECK(j1.contains("Date"));
        CHECK_FALSE(j1.contains("Country"));
    }

    SECTION("empty")
    {
        CHECK_FALSE(j3.empty());
        CHECK(j3["empty-object"].empty());
        CHECK(j3["empty-array"].empty());
        CHECK(j3["empty-string"].empty());
        CHECK(j3["empty-byte_string"].empty());
    }

    SECTION("size")
    {
        CHECK(j1.size() == 3);
    }

    SECTION("operator==")
    {
        CHECK_FALSE(j1 == j2);
        CHECK_FALSE(j1["City"] == j2["City"]);
        CHECK(j1["Amount"] == j2["Amount"]);
        CHECK_FALSE(j1["Date"] == j2["Date"]);
    }
}

TEST_CASE("cbor member tests")
{
    std::vector<uint8_t> v;
    cbor::cbor_bytes_encoder encoder(v);
    encoder.begin_object(); // indefinite length object
    encoder.name("empty-object");
    encoder.begin_object(0);
    encoder.end_object();
    encoder.name("empty-array");
    encoder.begin_array(0);
    encoder.end_array();
    encoder.name("empty-string");
    encoder.string_value("");
    encoder.name("empty-byte_string");
    encoder.byte_string_value(jsoncons::byte_string{});

    encoder.name("City");
    encoder.string_value("Montreal");
    encoder.name("Amount");
    encoder.string_value("273.15", semantic_tag::bigdec);
    encoder.name("Date");
    encoder.string_value("2018-05-07 12:41:07-07:00", semantic_tag::datetime) ;

    encoder.end_object(); 
    encoder.flush();
    json j = cbor::decode_cbor<json>(v);

    SECTION("contains")
    {
        CHECK(j.contains("City"));
        CHECK(j.contains("Amount"));
        CHECK(j.contains("Date"));
        CHECK_FALSE(j.contains("Country"));
    }

    SECTION("empty")
    {
        CHECK_FALSE(j.empty());
        CHECK(j["empty-object"].empty());
        CHECK(j["empty-array"].empty());
        CHECK(j["empty-string"].empty());
        CHECK(j["empty-byte_string"].empty());
    }

    SECTION("size")
    {
        CHECK(j.size() == 7);
    }
}

TEST_CASE("cbor conversion tests")
{
    std::vector<uint8_t> v;
    cbor::cbor_bytes_encoder encoder(v);
    encoder.begin_array(); // indefinite length outer array
    encoder.begin_array(4); // a fixed length array
    encoder.string_value("foo");
    encoder.byte_string_value(byte_string{'P','u','s','s'}); // no suggested conversion
    encoder.string_value("-18446744073709551617", semantic_tag::bigint);
    encoder.string_value("273.15", semantic_tag::bigdec);
    encoder.end_array();
    encoder.end_array();
    encoder.flush();

    json j = cbor::decode_cbor<json>(v);
    REQUIRE(j.size() == 1);

    auto range1 = j.array_range();
    auto it = range1.begin();
    const json& inner_array = *it++;
    REQUIRE(inner_array.size() == 4);
    REQUIRE((it == range1.end()));

    auto range2 = inner_array.array_range();
    auto it2 = range2.begin();
    CHECK(it2->as_string() == "foo");
    it2++;
    CHECK(it2->as_byte_string() == byte_string{'P','u','s','s'});
    it2++;
    CHECK(bool(it2->as_bignum() == bignum{"-18446744073709551617"}));
    it2++;
    CHECK(bool(it2->as_string() == std::string{"273.15"}));
    it2++;
    CHECK((it2 == range2.end()));
}

TEST_CASE("cbor array as<> test")
{
    std::vector<uint8_t> v;
    cbor::cbor_bytes_encoder encoder(v);
    encoder.begin_array(); // indefinite length outer array
    encoder.string_value("foo");
    encoder.byte_string_value(byte_string({'b','a','r'}));
    encoder.string_value("-18446744073709551617", semantic_tag::bigint);
    encoder.string_value("273.15", semantic_tag::bigdec);
    encoder.string_value("2015-05-07 12:41:07-07:00", semantic_tag::datetime) ;
    encoder.int64_value(1431027667, semantic_tag::timestamp);
    encoder.int64_value(-1431027667, semantic_tag::timestamp);
    encoder.double_value(1431027667.5, semantic_tag::timestamp);
    encoder.end_array();
    encoder.flush();

/*
9f -- Start indefinite length array 
  63 -- String value of length 3 
    666f6f -- "foo"
  43 -- Byte string value of length 3
    626172 -- 'b''a''r'
  c3 -- Tag 3 (negative bignum)
    49 Byte string value of length 9
      010000000000000000 -- Bytes content
  c4  - Tag 4 (decimal fraction)
    82 -- Array of length 2
      21 -- -2
      19 6ab3 -- 27315
  c0 -- Tag 0 (date-time)
    78 19 -- Length (25)
      323031352d30352d30372031323a34313a30372d30373a3030 -- "2015-05-07 12:41:07-07:00"
  c1 -- Tag 1 (epoch time)
    1a -- uint32_t
      554bbfd3 -- 1431027667 
  c1
    3a
      554bbfd2
  c1
    fb
      41d552eff4e00000
  ff -- "break" 
*/

    //std::cout << "v: \n";
    //for (auto c : v)
    //{
    //    std::cout << std::hex << std::setprecision(2) << std::setw(2)
    //              << std::setfill('0') << static_cast<int>(c);
    //}
    //std::cout << "\n\n";

    json j = cbor::decode_cbor<json>(v); // a non-owning view of the CBOR v

    CHECK(j.size() == 8);

    SECTION("j[0].is<T>()")
    {
        CHECK(j[0].is<std::string>());
        CHECK(j[1].is<byte_string>());
        CHECK(j[1].is<byte_string_view>());
        CHECK(j[2].is<bignum>());
        CHECK(j[3].is_string());
        CHECK(j[3].get_semantic_tag() == semantic_tag::bigdec);
        CHECK(j[4].is<std::string>());
        CHECK(j[5].is<int>());
        CHECK(j[5].is<unsigned int>());
        CHECK(j[6].is<int>());
        CHECK_FALSE(j[6].is<unsigned int>());
        CHECK(j[7].is<double>());
    }

    SECTION("j[0].as<T>()")
    {
        CHECK(j[0].as<std::string>() == std::string("foo"));
        CHECK(j[1].as<jsoncons::byte_string>() == jsoncons::byte_string({'b','a','r'}));
        CHECK(j[2].as<std::string>() == std::string("-18446744073709551617"));
        CHECK(bool(j[2].as<jsoncons::bignum>() == jsoncons::bignum("-18446744073709551617")));
        CHECK(j[3].as<std::string>() == std::string("273.15"));
        CHECK(j[4].as<std::string>() == std::string("2015-05-07 12:41:07-07:00"));
        CHECK(j[5].as<int64_t>() == 1431027667);
        CHECK(j[5].as<uint64_t>() == 1431027667U);
        CHECK(j[6].as<int64_t>() == -1431027667);
        CHECK(j[7].as<double>() == 1431027667.5);
    }

    SECTION("array_iterator is<T> test")
    {
        auto it = j.array_range().begin();
        CHECK(it++->is<std::string>());
        CHECK(it++->is<byte_string>());
        CHECK(it++->is<bignum>());
        CHECK(it++->is<std::string>());
        CHECK(it++->is<std::string>());
        CHECK(it++->is<int>());
        CHECK(it++->is<int>());
        CHECK(it++->is<double>());
    }
}

TEST_CASE("cbor bigfloat tests")
{
    SECTION("1.5")
    {
        std::vector<uint8_t> v = {0xc5, // Tag 5 
                                  0x82, // Array of length 2
                                  0x20, // -1 
                                  0x03 // 3 
                                 };

        json j = cbor::decode_cbor<json>(v);

        //std::string s = j.as<std::string>();

        double val = j.as<double>();
        CHECK(val == Approx(1.5).epsilon(0.0000000001));
    }
    SECTION("-1.5")
    {
        std::vector<uint8_t> v = {0xc5, // Tag 5 
                                  0x82, // Array of length 2
                                  0x20, // -1 
                                  0x22 // -3 
                                 };

        json j = cbor::decode_cbor<json>(v);
        //std::string s = j.as<std::string>();
        //CHECK(s == std::string("-1.5"));

        double val = j.as<double>();
        CHECK(val == Approx(-1.5).epsilon(0.0000000001));
    }
} 

