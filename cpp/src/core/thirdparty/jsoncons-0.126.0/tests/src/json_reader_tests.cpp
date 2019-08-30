// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <jsoncons/json_reader.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>

using namespace jsoncons;

void test_json_reader_error(const std::string& text, std::error_code ec)
{
    REQUIRE_THROWS(json::parse(text));
    try
    {
        json::parse(text);
    }
    catch (const ser_error& e)
    {
        if (e.code() != ec)
        {
            std::cout << text << std::endl;
            std::cout << e.code().value() << " " << e.what() << std::endl; 
        }
        CHECK(ec == e.code());
    }
}

void test_json_reader_ec(const std::string& text, std::error_code expected)
{
    std::error_code ec;

    std::istringstream is(text);
    json_decoder<json> decoder;
    json_reader reader(is,decoder);

    reader.read(ec);
    //std::cerr << text << std::endl;
    //std::cerr << ec.message() 
    //          << " at line " << reader.line() 
    //          << " and column " << reader.column() << std::endl;

    CHECK(ec);
    CHECK(ec == expected);
}

TEST_CASE("test_missing_separator")
{
    std::string jtext = R"({"field1"{}})";    

    test_json_reader_error(jtext, jsoncons::json_errc::expected_colon);
    test_json_reader_ec(jtext, jsoncons::json_errc::expected_colon);
}

TEST_CASE("test_read_invalid_value")
{
    std::string jtext = R"({"field1":ru})";    

    test_json_reader_error(jtext,jsoncons::json_errc::expected_value);
    test_json_reader_ec(jtext, jsoncons::json_errc::expected_value);
}

TEST_CASE("test_read_unexpected_end_of_file")
{
    std::string jtext = R"({"field1":{})";    

    test_json_reader_error(jtext, jsoncons::json_errc::unexpected_eof);
    test_json_reader_ec(jtext, jsoncons::json_errc::unexpected_eof);
}


TEST_CASE("test_read_value_not_found")
{
    std::string jtext = R"({"name":})";    

    test_json_reader_error(jtext, jsoncons::json_errc::expected_value);
    test_json_reader_ec(jtext, jsoncons::json_errc::expected_value);
}

TEST_CASE("test_read_escaped_characters")
{
    std::string input("[\"\\n\\b\\f\\r\\t\"]");
    std::string expected("\n\b\f\r\t");

    json o = json::parse(input);
    CHECK(expected == o[0].as<std::string>());
}


TEST_CASE("test_read_expected_colon")
{
    test_json_reader_error("{\"name\" 10}", jsoncons::json_errc::expected_colon);
    test_json_reader_error("{\"name\" true}", jsoncons::json_errc::expected_colon);
    test_json_reader_error("{\"name\" false}", jsoncons::json_errc::expected_colon);
    test_json_reader_error("{\"name\" null}", jsoncons::json_errc::expected_colon);
    test_json_reader_error("{\"name\" \"value\"}", jsoncons::json_errc::expected_colon);
    test_json_reader_error("{\"name\" {}}", jsoncons::json_errc::expected_colon);
    test_json_reader_error("{\"name\" []}", jsoncons::json_errc::expected_colon);
}

TEST_CASE("test_read_expected_name")
{
    test_json_reader_error("{10}", jsoncons::json_errc::expected_name);
    test_json_reader_error("{true}", jsoncons::json_errc::expected_name);
    test_json_reader_error("{false}", jsoncons::json_errc::expected_name);
    test_json_reader_error("{null}", jsoncons::json_errc::expected_name);
    test_json_reader_error("{{}}", jsoncons::json_errc::expected_name);
    test_json_reader_error("{[]}", jsoncons::json_errc::expected_name);
}

TEST_CASE("test_read_expected_value")
{
    test_json_reader_error("[tru]", jsoncons::json_errc::invalid_value);
    test_json_reader_error("[fa]", jsoncons::json_errc::invalid_value);
    test_json_reader_error("[n]", jsoncons::json_errc::invalid_value);
}

TEST_CASE("test_read_primitive_pass")
{
    json val;
    CHECK_NOTHROW((val=json::parse("null")));
    CHECK(val == json::null());
    CHECK_NOTHROW((val=json::parse("false")));
    CHECK(val == json(false));
    CHECK_NOTHROW((val=json::parse("true")));
    CHECK(val == json(true));
    CHECK_NOTHROW((val=json::parse("10")));
    CHECK(val == json(10));
    CHECK_NOTHROW((val=json::parse("1.999")));
    CHECK(val == json(1.999));
    CHECK_NOTHROW((val=json::parse("\"string\"")));
    CHECK(val == json("string"));
}

TEST_CASE("test_read_empty_structures")
{
    json val;
    CHECK_NOTHROW((val=json::parse("{}")));
    CHECK_NOTHROW((val=json::parse("[]")));
    CHECK_NOTHROW((val=json::parse("{\"object\":{},\"array\":[]}")));
    CHECK_NOTHROW((val=json::parse("[[],{}]")));
}

TEST_CASE("test_read_primitive_fail")
{
    test_json_reader_error("null {}", jsoncons::json_errc::extra_character);
    test_json_reader_error("n ", jsoncons::json_errc::invalid_value);
    test_json_reader_error("nu ", jsoncons::json_errc::invalid_value);
    test_json_reader_error("nul ", jsoncons::json_errc::invalid_value);
    test_json_reader_error("false {}", jsoncons::json_errc::extra_character);
    test_json_reader_error("fals ", jsoncons::json_errc::invalid_value);
    test_json_reader_error("true []", jsoncons::json_errc::extra_character);
    test_json_reader_error("tru ", jsoncons::json_errc::invalid_value);
    test_json_reader_error("10 {}", jsoncons::json_errc::extra_character);
    test_json_reader_error("1a ", jsoncons::json_errc::invalid_number);
    test_json_reader_error("1.999 []", jsoncons::json_errc::extra_character);
    test_json_reader_error("1e0-1", jsoncons::json_errc::invalid_number);
    test_json_reader_error("\"string\"{}", jsoncons::json_errc::extra_character);
    test_json_reader_error("\"string\"[]", jsoncons::json_errc::extra_character);
}

TEST_CASE("test_read_multiple")
{
    std::string in="{\"a\":1,\"b\":2,\"c\":3}{\"a\":4,\"b\":5,\"c\":6}";
    //std::cout << in << std::endl;

    std::istringstream is(in);

    jsoncons::json_decoder<json> decoder;
    json_reader reader(is,decoder);

    REQUIRE_FALSE(reader.eof());
    reader.read_next();
    json val = decoder.get_result();
    CHECK(1 == val["a"].as<int>());
    REQUIRE_FALSE(reader.eof());
    reader.read_next();
    json val2 = decoder.get_result();
    CHECK(4 == val2["a"].as<int>());
    CHECK(reader.eof());
}

TEST_CASE("json_reader read from string test")
{
    std::string s = R"(
{
  "store": {
    "book": [
      {
        "category": "reference",
        "author": "Margaret Weis",
        "title": "Dragonlance Series",
        "price": 31.96
      },
      {
        "category": "reference",
        "author": "Brent Weeks",
        "title": "Night Angel Trilogy",
        "price": 14.70
      }
    ]
  }
}
)";

    json_decoder<json> decoder;
    json_reader reader(s, decoder);
    reader.read();
    json j = decoder.get_result();

    REQUIRE(j.is_object());
    REQUIRE(j.size() == 1);
    REQUIRE(j[0].is_object());
    REQUIRE(j[0].size() == 1);
    REQUIRE(j[0][0].is_array());
    REQUIRE(j[0][0].size() == 2);
    CHECK(j[0][0][0]["category"].as<std::string>() == std::string("reference"));
    CHECK(j[0][0][1]["author"].as<std::string>() == std::string("Brent Weeks"));
}



