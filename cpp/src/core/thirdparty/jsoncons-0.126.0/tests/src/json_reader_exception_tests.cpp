// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json_reader.hpp>
#include <jsoncons/json.hpp>
#include <jsoncons/json_decoder.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <fstream>

using namespace jsoncons;

TEST_CASE("test_filename_invalid")
{
    std::string in_file = "./input/json-exception--1.json";
    std::ifstream is(in_file);

    json_decoder<json> decoder;

    try
    {
        json_reader reader(is,decoder);
        reader.read_next();
    }
    catch (const std::exception&)
    {
    }
    //CHECK(false == decoder.is_valid());
}

TEST_CASE("test_exception_left_brace")
{
    std::string in_file = "./input/json-exception-1.json";
    std::ifstream is(in_file);

    json_decoder<json> decoder;
    try
    {
        json_reader reader(is,decoder);
        reader.read_next();
    }
    catch (const ser_error& e)
    {
        CHECK(e.code() == json_errc::expected_comma_or_right_bracket);
        CHECK(14 == e.line());
        CHECK(30 == e.column());
    }
    CHECK(false == decoder.is_valid());
}
TEST_CASE("test_exception_right_brace")
{
    std::string in_file = "./input/json-exception-2.json";
    std::ifstream is(in_file);

    json_decoder<json> decoder;
    try
    {
        json_reader reader(is,decoder);
        reader.read_next();  // must throw
        CHECK(0 != 0);
    }
    catch (const ser_error& e)
    {
        //std::cout << e.what() << std::endl;
        CHECK(e.code() == json_errc::expected_comma_or_right_brace);
        CHECK(17 == e.line());
        CHECK(9 == e.column());
    }
    CHECK(false == decoder.is_valid());
}

TEST_CASE("test_exception_array_eof")
{
    std::istringstream is("[100");

    json_decoder<json> decoder;
    try
    {
        json_reader reader(is,decoder);
        reader.read_next();  // must throw
        CHECK(0 != 0);
    }
    catch (const ser_error& e)
    {
        CHECK(e.code() == json_errc::unexpected_eof);
        CHECK(1 == e.line());
        CHECK(5 == e.column());
    }
    CHECK(false == decoder.is_valid());
}

TEST_CASE("test_exception_unicode_eof")
{
    std::istringstream is("[\"\\u");

    json_decoder<json> decoder;
    try
    {
        json_reader reader(is,decoder);
        reader.read_next();  // must throw
        CHECK(0 != 0);
    }
    catch (const ser_error& e)
    {
        //std::cout << e.what() << std::endl;
        CHECK(e.code() == json_errc::unexpected_eof);
        CHECK(1 == e.line());
        CHECK(5 == e.column());
    }
    CHECK(false == decoder.is_valid());
}

TEST_CASE("test_exception_tru_eof")
{
    std::istringstream is("[tru");

    json_decoder<json> decoder;
    try
    {
        json_reader reader(is,decoder);
        reader.read_next();  // must throw
        CHECK(0 != 0);
    }
    catch (const ser_error& e)
    {
        //std::cout << e.what() << std::endl;
        CHECK(e.code() == json_errc::unexpected_eof);
        CHECK(1 == e.line());
        CHECK(5 == e.column());
    }
    CHECK(false == decoder.is_valid());
}

TEST_CASE("test_exception_fals_eof")
{
    std::istringstream is("[fals");

    json_decoder<json> decoder;
    try
    {
        json_reader reader(is,decoder);
        reader.read_next();  // must throw
        CHECK(0 != 0);
    }
    catch (const ser_error& e)
    {
        //std::cout << e.what() << std::endl;
        CHECK(e.code() == json_errc::unexpected_eof);
        CHECK(1 == e.line());
        CHECK(6 == e.column());
    }
    CHECK(false == decoder.is_valid());
}

TEST_CASE("test_exception_nul_eof")
{
    std::istringstream is("[nul");

    json_decoder<json> decoder;
    try
    {
        json_reader reader(is,decoder);
        reader.read_next();  // must throw
        CHECK(0 != 0);
    }
    catch (const ser_error& e)
    {
        //std::cout << e.what() << std::endl;
        CHECK(e.code() == json_errc::unexpected_eof);
        CHECK(1 == e.line());
        CHECK(5 == e.column());
    }
    CHECK(false == decoder.is_valid());
}

TEST_CASE("test_exception_true_eof")
{
    std::istringstream is("[true");

    json_decoder<json> decoder;
    try
    {
        json_reader reader(is,decoder);
        reader.read_next();  // must throw
        CHECK(0 != 0);
    }
    catch (const ser_error& e)
    {
        CHECK(e.code() == json_errc::unexpected_eof);
        CHECK(1 == e.line());
        CHECK(6 == e.column());
    }
    CHECK(false == decoder.is_valid());
}

TEST_CASE("test_exception_false_eof")
{
    std::istringstream is("[false");

    json_decoder<json> decoder;
    try
    {
        json_reader reader(is,decoder);
        reader.read_next();  // must throw
        CHECK(0 != 0);
    }
    catch (const ser_error& e)
    {
        CHECK(e.code() == json_errc::unexpected_eof);
        CHECK(1 == e.line());
        CHECK(7 == e.column());
    }
    CHECK(false == decoder.is_valid());
}

TEST_CASE("test_exception_null_eof")
{
    std::istringstream is("[null");

    json_decoder<json> decoder;
    try
    {
        json_reader reader(is,decoder);
        reader.read_next();  // must throw
        CHECK(0 != 0);
    }
    catch (const ser_error& e)
    {
        CHECK(e.code() == json_errc::unexpected_eof);
        CHECK(1 == e.line());
        CHECK(6 == e.column());
    }
    CHECK(false == decoder.is_valid());
}

TEST_CASE("test_exception")
{
    std::string input("{\"field1\":\n\"value}");
    REQUIRE_THROWS_AS(json::parse(input),ser_error);
    try
    {
        json::parse(input);
    }
    catch (const ser_error& e)
    {
        CHECK((e.code() == json_errc::unexpected_eof && e.line() == 2 && e.column() == 9));
    }
}

