// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <jsoncons/json_parser.hpp>
#include <jsoncons/json_decoder.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <fstream>

using namespace jsoncons;

TEST_CASE("Test cyrillic.json")
{
    std::string path = "./input/cyrillic.json";
    std::fstream is(path);
    if (!is)
    {
        std::cout << "Cannot open " << path << std::endl;
    }
    REQUIRE(is);
    json j = json::parse(is);
}

TEST_CASE("test_object2")
{
json source = json::parse(R"(
{
    "a" : "2",
    "c" : [4,5,6]
}
)");

    std::cout << source << std::endl;
}

TEST_CASE("test_object_with_three_members")
{
    std::string input = "{\"A\":\"Jane\", \"B\":\"Roe\",\"C\":10}";
    json val = json::parse(input);

    CHECK(true == val.is_object());
    CHECK(3 == val.size());
}

TEST_CASE("test_double")
{
    json val = json::parse("42.229999999999997");
}

TEST_CASE("test_array_of_integer")
{
    std::string s = "[1,2,3]";
    json j1 = json::parse(s);
    CHECK(true == j1.is_array());
    CHECK(3 == j1.size());

    std::istringstream is(s);
    json j2 = json::parse(is);
    CHECK(true == j2.is_array());
    CHECK(3 == j2.size());
}

TEST_CASE("test_skip_bom")
{
    std::string s = "\xEF\xBB\xBF[1,2,3]";
    json j1 = json::parse(s);
    CHECK(true == j1.is_array());
    CHECK(3 == j1.size());

    std::istringstream is(s);
    json j2 = json::parse(is);
    CHECK(true == j2.is_array());
    CHECK(3 == j2.size());
}

TEST_CASE("test_parse_empty_object")
{
    jsoncons::json_decoder<json> decoder;
    json_parser parser;

    parser.reset();

    static std::string s("{}");

    parser.update(s.data(),s.length());
    parser.parse_some(decoder);
    parser.finish_parse(decoder);
    CHECK(parser.done());

    json j = decoder.get_result();
}

TEST_CASE("test_parse_array")
{
    jsoncons::json_decoder<json> decoder;
    json_parser parser;

    parser.reset();

    static std::string s("[]");

    parser.update(s.data(),s.length());
    parser.parse_some(decoder);
    parser.finish_parse(decoder);
    CHECK(parser.done());

    json j = decoder.get_result();
}

TEST_CASE("test_parse_string")
{
    jsoncons::json_decoder<json> decoder;
    json_parser parser;

    parser.reset();

    static std::string s("\"\"");

    parser.update(s.data(),s.length());
    parser.parse_some(decoder);
    parser.finish_parse(decoder);
    CHECK(parser.done());

    json j = decoder.get_result();
}

TEST_CASE("test_parse_integer")
{
    jsoncons::json_decoder<json> decoder;
    json_parser parser;

    parser.reset();

    static std::string s("10");

    parser.update(s.data(),s.length());
    parser.parse_some(decoder);
    parser.finish_parse(decoder);
    CHECK(parser.done());

    json j = decoder.get_result();
}

TEST_CASE("test_parse_integer_space")
{
    jsoncons::json_decoder<json> decoder;
    json_parser parser;

    parser.reset();

    static std::string s("10 ");

    parser.update(s.data(),s.length());
    parser.parse_some(decoder);
    parser.finish_parse(decoder);
    CHECK(parser.done());

    json j = decoder.get_result();
}

TEST_CASE("test_parse_double_space")
{
    jsoncons::json_decoder<json> decoder;
    json_parser parser;

    parser.reset();

    static std::string s("10.0 ");

    parser.update(s.data(),s.length());
    parser.parse_some(decoder);
    parser.finish_parse(decoder);
    CHECK(parser.done());

    json j = decoder.get_result();
}

TEST_CASE("test_parse_false")
{
    jsoncons::json_decoder<json> decoder;
    json_parser parser;

    parser.reset();

    static std::string s("false");

    parser.update(s.data(),s.length());
    parser.parse_some(decoder);
    parser.finish_parse(decoder);
    CHECK(parser.done());

    json j = decoder.get_result();
}

TEST_CASE("test_parse_true")
{
    jsoncons::json_decoder<json> decoder;
    json_parser parser;

    parser.reset();

    static std::string s("true");

    parser.update(s.data(),s.length());
    parser.parse_some(decoder);
    parser.finish_parse(decoder);
    CHECK(parser.done());

    json j = decoder.get_result();
}

TEST_CASE("test_parse_null")
{
    jsoncons::json_decoder<json> decoder;
    json_parser parser;

    parser.reset();

    static std::string s("null");

    parser.update(s.data(),s.length());
    parser.parse_some(decoder);
    parser.finish_parse(decoder);
    CHECK(parser.done());

    json j = decoder.get_result();
}

TEST_CASE("test_parse_array_string")
{
    jsoncons::json_decoder<json> decoder;
    json_parser parser;

    parser.reset();

    static std::string s1("[\"\"");

    parser.update(s1.data(),s1.length());
    parser.parse_some(decoder);
    CHECK_FALSE(parser.done());
    static std::string s2("]");
    parser.update(s2.data(), s2.length());
    parser.parse_some(decoder);
    parser.finish_parse(decoder);
    CHECK(parser.done());

    json j = decoder.get_result();
}

TEST_CASE("test_incremental_parsing")
{
    jsoncons::json_decoder<json> decoder;
    json_parser parser;

    parser.reset();

    parser.update("[fal",4);
    parser.parse_some(decoder);
    CHECK_FALSE(parser.done());
    CHECK(parser.source_exhausted());
    parser.update("se]",3);
    parser.parse_some(decoder);

    parser.finish_parse(decoder);
    CHECK(parser.done());

    json j = decoder.get_result();
    REQUIRE(j.is_array());
    CHECK_FALSE(j[0].as<bool>());
}




